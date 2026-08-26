"""Load test: measure message throughput and delivery latency
under N concurrent WebSocket connections split across both FastAPI
instances.

This exists to turn "scalable" into a number that can be defended in an
interview, not just asserted. It deliberately targets the cross-instance
delivery path proven functionally by tests/websocket/test_multi_instance.py
(A -> FastAPI #1 -> Redis Pub/Sub -> FastAPI #2 -> B): every pair below has
its sender connected to instance A and its receiver connected to instance B,
so every single measured message crosses the Redis Pub/Sub fanout described
in README's "Horizontal scaling & Redis Pub/Sub" section - the harder path,
not the easy same-instance case.

Usage (against the docker-compose stack, which exposes fastapi-1 on 8001
and fastapi-2 on 8002 directly - see docker-compose.yml):

    docker compose up --build -d
    .venv/Scripts/python scripts/load_test.py --pairs 100 --messages 20

Two phases, with a hard barrier between them:

1. Setup - register both users, create the conversation, open both
   WebSockets, join. Done concurrently for every pair, and *not* timed.
2. Measure - only after every pair has finished setup does any pair start
   sending. This barrier matters: registration hashes each password with
   Argon2id (app.auth.passwords, deliberately expensive - time_cost=3,
   memory_cost=64MB), synchronously, on the same single-threaded event loop
   that also drives every open WebSocket. Without the barrier, pairs still
   registering would starve pairs already in their measured send/receive
   loop of event-loop time, inflating "message delivery latency" with
   registration CPU cost that has nothing to do with the messaging pipeline
   this script is actually trying to measure.

Each sender paces its own sends (see --send-interval) to stay under the
default `rate_limit_message_send_max_events` / `_window_seconds` (20 per 10s
- see app.core.config.Settings and README's Rate limiting section) rather
than tune that limit away for this run: the point of this script is to
measure aggregate throughput across many concurrent *users*, which is
exactly what horizontal scaling is for, not the ceiling of one connection
ignoring the abuse protection every real client would also be subject to.
Raise --send-interval if a different rate_limit_message_send_* configuration
is deployed. Sends are also jittered within the interval (see `sender()`
below) so 100+ pairs launched together don't fire in synchronized lockstep
bursts every tick - a self-inflicted thundering-herd pattern that would
distort latency compared to organically-arriving concurrent traffic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass, field

import httpx
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

PASSWORD = "correct-horse-battery-staple"


@dataclass
class PairResult:
    latencies_seconds: list[float] = field(default_factory=list)
    error: str | None = None


@dataclass
class ReadyPair:
    sender_ws: ClientConnection
    receiver_ws: ClientConnection
    conversation_id: str


async def _wait_until_ready(client: httpx.AsyncClient, base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = await client.get(f"{base_url}/health")
            if response.status_code == 200:
                return
        except httpx.HTTPError as error:
            last_error = error
        await asyncio.sleep(0.5)
    raise RuntimeError(f"{base_url} never became healthy") from last_error


def _synthetic_client_ip(pair_index: int) -> str:
    """A distinct fake source IP per pair, sent as X-Forwarded-For.

    app.core.rate_limit._client_ip trusts X-Forwarded-For unconditionally
    when a request reaches FastAPI directly (its own documented trust
    boundary: in docker-compose the only real path in is through Nginx,
    which overwrites this header - see that function's docstring). This
    script talks to fastapi-1/fastapi-2 directly, bypassing Nginx, so it can
    lean on that same boundary to simulate one distinct client IP per pair
    rather than every registration in this run competing for the single
    per-IP registration-rate-limit budget a real single-IP client would
    have. This is arguably more realistic too: real load comes from many
    distinct client IPs, not one script hammering the API from one address.
    """
    return f"10.{(pair_index >> 16) & 0xFF}.{(pair_index >> 8) & 0xFF}.{pair_index & 0xFF}"


async def _register(
    client: httpx.AsyncClient, base_url: str, email: str, client_ip: str
) -> tuple[str, str]:
    response = await client.post(
        f"{base_url}/auth/register",
        json={"email": email, "password": PASSWORD, "device_label": "load-test"},
        headers={"X-Forwarded-For": client_ip},
    )
    response.raise_for_status()
    access_token = response.json()["access_token"]
    profile = await client.get(
        f"{base_url}/auth/me", headers={"Authorization": f"Bearer {access_token}"}
    )
    profile.raise_for_status()
    return access_token, profile.json()["id"]


async def _create_direct_conversation(
    client: httpx.AsyncClient, base_url: str, sender_token: str, receiver_id: str
) -> str:
    response = await client.post(
        f"{base_url}/conversations",
        headers={"Authorization": f"Bearer {sender_token}"},
        json={"kind": "direct", "member_ids": [receiver_id]},
    )
    response.raise_for_status()
    result: str = response.json()["id"]
    return result


async def _setup_pair(
    pair_index: int,
    *,
    http_client: httpx.AsyncClient,
    instance_a_http: str,
    instance_a_ws: str,
    instance_b_ws: str,
    stack: AsyncExitStack,
) -> ReadyPair:
    """Register both users, create the conversation, open and join both
    WebSockets. Raises on any failure - the caller (main) reports which
    pairs failed setup and proceeds with only the pairs that succeeded.
    """
    client_ip = _synthetic_client_ip(pair_index)
    sender_token, _sender_id = await _register(
        http_client,
        instance_a_http,
        f"load-sender-{pair_index}-{uuid.uuid4()}@example.com",
        client_ip,
    )
    receiver_token, receiver_id = await _register(
        http_client,
        instance_a_http,
        f"load-receiver-{pair_index}-{uuid.uuid4()}@example.com",
        client_ip,
    )
    conversation_id = await _create_direct_conversation(
        http_client, instance_a_http, sender_token, receiver_id
    )

    sender_ws = await stack.enter_async_context(
        ws_connect(
            f"{instance_a_ws}/ws", additional_headers={"Authorization": f"Bearer {sender_token}"}
        )
    )
    receiver_ws = await stack.enter_async_context(
        ws_connect(
            f"{instance_b_ws}/ws",
            additional_headers={"Authorization": f"Bearer {receiver_token}"},
        )
    )

    assert json.loads(await sender_ws.recv())["type"] == "auth:authenticated"
    assert json.loads(await receiver_ws.recv())["type"] == "auth:authenticated"

    await sender_ws.send(
        json.dumps({"type": "conversation:join", "conversation_id": conversation_id})
    )
    assert json.loads(await sender_ws.recv())["type"] == "conversation:joined"
    await receiver_ws.send(
        json.dumps({"type": "conversation:join", "conversation_id": conversation_id})
    )
    assert json.loads(await receiver_ws.recv())["type"] == "conversation:joined"

    return ReadyPair(sender_ws=sender_ws, receiver_ws=receiver_ws, conversation_id=conversation_id)


async def _measure_pair(pair: ReadyPair, *, message_count: int, send_interval: float) -> PairResult:
    result = PairResult()
    sender_ws, receiver_ws, conversation_id = pair.sender_ws, pair.receiver_ws, pair.conversation_id

    # One sender, one fresh conversation, strictly sequential sends:
    # sequence numbers are guaranteed to land as 1..message_count in send
    # order (see app.messages.service.persist_message's per-conversation
    # counter), so send time can be tracked by position alone - no need to
    # correlate against the server's response.
    send_times: dict[int, float] = {}

    async def sender() -> None:
        # See module docstring: jittered so pairs don't all fire on the same
        # --send-interval tick in lockstep.
        await asyncio.sleep(random.uniform(0, send_interval))
        for i in range(1, message_count + 1):
            send_times[i] = time.perf_counter()
            await sender_ws.send(
                json.dumps(
                    {
                        "type": "message:send",
                        "conversation_id": conversation_id,
                        "client_message_id": str(uuid.uuid4()),
                        "body": f"load message {i}",
                    }
                )
            )
            new_event = json.loads(await sender_ws.recv())
            assert new_event["type"] == "message:new", new_event
            ack = json.loads(await sender_ws.recv())
            assert ack["type"] == "message:ack", ack
            if i < message_count:
                await asyncio.sleep(send_interval)

    async def receiver() -> None:
        received = 0
        while received < message_count:
            event = json.loads(await receiver_ws.recv())
            if event["type"] != "message:new":
                continue
            t1 = time.perf_counter()
            t0 = send_times.get(event["sequence_number"])
            if t0 is not None:
                result.latencies_seconds.append(t1 - t0)
            received += 1

    try:
        await asyncio.wait_for(
            asyncio.gather(sender(), receiver()),
            timeout=max(30.0, message_count * send_interval * 3 + 10),
        )
    except Exception as error:  # noqa: BLE001 - report and keep other pairs running
        result.error = f"{type(error).__name__}: {error}"
    return result


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, max(0, round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[index]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pairs", type=int, default=100, help="sender/receiver pairs (2x connections)"
    )
    parser.add_argument("--messages", type=int, default=20, help="messages sent per sender")
    parser.add_argument(
        "--send-interval",
        type=float,
        default=0.6,
        help="seconds between one sender's messages (default stays under the "
        "default rate_limit_message_send_max_events=20 per 10s window)",
    )
    parser.add_argument("--instance-a", default="http://localhost:8001", help="fastapi-1 base URL")
    parser.add_argument("--instance-b", default="http://localhost:8002", help="fastapi-2 base URL")
    parser.add_argument("--ready-timeout", type=float, default=30.0)
    parser.add_argument("--output", default=None, help="optional path to write JSON results to")
    args = parser.parse_args()

    instance_a_ws = args.instance_a.replace("http://", "ws://").replace("https://", "wss://")
    instance_b_ws = args.instance_b.replace("http://", "ws://").replace("https://", "wss://")

    async with httpx.AsyncClient(timeout=30.0) as http_client, AsyncExitStack() as stack:
        await _wait_until_ready(http_client, args.instance_a, args.ready_timeout)
        await _wait_until_ready(http_client, args.instance_b, args.ready_timeout)

        print(
            f"Setting up {args.pairs} pairs ({args.pairs * 2} connections, sender on "
            f"{args.instance_a} -> receiver on {args.instance_b})..."
        )
        setup_start = time.perf_counter()
        setup_results = await asyncio.gather(
            *(
                _setup_pair(
                    i,
                    http_client=http_client,
                    instance_a_http=args.instance_a,
                    instance_a_ws=instance_a_ws,
                    instance_b_ws=instance_b_ws,
                    stack=stack,
                )
                for i in range(args.pairs)
            ),
            return_exceptions=True,
        )
        setup_seconds = time.perf_counter() - setup_start

        ready_pairs = [p for p in setup_results if isinstance(p, ReadyPair)]
        setup_errors = [
            f"{type(p).__name__}: {p}" for p in setup_results if not isinstance(p, ReadyPair)
        ]
        print(
            f"Setup done in {setup_seconds:.2f}s: {len(ready_pairs)}/{args.pairs} pairs ready. "
            f"Sending {args.messages} messages/sender (every message crossing the Redis Pub/Sub "
            f"fanout between instances)..."
        )

        wall_start = time.perf_counter()
        results = await asyncio.gather(
            *(
                _measure_pair(pair, message_count=args.messages, send_interval=args.send_interval)
                for pair in ready_pairs
            )
        )
        wall_seconds = time.perf_counter() - wall_start

    measure_errors = [r.error for r in results if r.error is not None]
    all_latencies = sorted(latency for r in results for latency in r.latencies_seconds)
    delivered = len(all_latencies)
    expected = len(ready_pairs) * args.messages

    report = {
        "pairs_requested": args.pairs,
        "pairs_ready": len(ready_pairs),
        "setup_failures": len(setup_errors),
        "messages_per_sender": args.messages,
        "connections": len(ready_pairs) * 2,
        "expected_messages": expected,
        "delivered_messages": delivered,
        "failed_pairs_during_measurement": len(measure_errors),
        "setup_seconds": round(setup_seconds, 3),
        "measured_wall_seconds": round(wall_seconds, 3),
        "throughput_messages_per_second": round(delivered / wall_seconds, 2) if wall_seconds else 0,
        "latency_ms": {
            "min": round(all_latencies[0] * 1000, 2) if all_latencies else None,
            "p50": round(_percentile(all_latencies, 50) * 1000, 2) if all_latencies else None,
            "p95": round(_percentile(all_latencies, 95) * 1000, 2) if all_latencies else None,
            "p99": round(_percentile(all_latencies, 99) * 1000, 2) if all_latencies else None,
            "max": round(all_latencies[-1] * 1000, 2) if all_latencies else None,
            "mean": round(statistics.mean(all_latencies) * 1000, 2) if all_latencies else None,
        },
        "sample_setup_errors": setup_errors[:5],
        "sample_measurement_errors": measure_errors[:5],
    }

    print(json.dumps(report, indent=2))
    if setup_errors or measure_errors:
        print(
            f"\n{len(setup_errors)} setup failures, {len(measure_errors)} measurement "
            "failures - see sample errors above.",
            file=sys.stderr,
        )

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
