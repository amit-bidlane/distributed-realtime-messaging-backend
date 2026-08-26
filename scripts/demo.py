"""Interactive two-terminal WebSocket demo client.

This is a manual-testing utility, not part of the application or its test
suite: this project has no frontend, so there is otherwise no way to
manually exercise a real two-user conversation - join, send, edit, delete,
typing, read receipts, offline replay - by hand. Open two terminals, one per
demo user:

    .venv/Scripts/python scripts/demo.py --as alice
    .venv/Scripts/python scripts/demo.py --as bob

Both default to talking to Nginx on localhost:8080. To prove cross-instance
delivery the same way tests/websocket/test_multi_instance.py does, point one
terminal at Nginx and the other directly at the other FastAPI instance
(docker-compose exposes fastapi-1 on 8001 and fastapi-2 on 8002 directly):

    .venv/Scripts/python scripts/demo.py --as alice --port 8080
    .venv/Scripts/python scripts/demo.py --as bob --port 8002

On startup each side registers (or, if the demo user already exists, logs
in), resolves the other demo user's id via POST /contacts/sync, creates or
reuses their shared direct conversation, connects to /ws with the JWT in the
Authorization header (never the query string, per project design rule), and joins
that conversation. A background task then prints every inbound event as it
arrives, while the foreground reads commands from stdin.

Commands:

    <text>                  -> message:send
    /edit <id> <new text>   -> message:edit
    /delete <id>            -> message:delete
    /read <id>              -> message:read
    /typing                 -> typing:start, then typing:stop after 3s
    /history                -> GET /conversations/{id}/messages, pretty-printed
    /quit                   -> close cleanly
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

DEMO_PASSWORD = "correct-horse-battery-staple"
DEMO_USERS = {
    "alice": "alice@demo.local",
    "bob": "bob@demo.local",
}


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


@dataclass
class DemoSession:
    """An httpx client plus the refresh token needed to renew its access
    token. The WebSocket connection is authenticated once at connect time and
    never needs this (see app.websocket.auth), but access_token_ttl_minutes
    (app.core.config.Settings) is short enough that a long interactive
    session's REST calls (/history) can otherwise hit a stale token.
    """

    client: httpx.AsyncClient
    refresh_token: str

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Issue a request, transparently refreshing and retrying once on 401."""
        response = await self.client.request(method, url, **kwargs)
        if response.status_code != 401:
            return response
        await self._refresh()
        return await self.client.request(method, url, **kwargs)

    async def _refresh(self) -> None:
        response = await self.client.post(
            "/auth/refresh", json={"refresh_token": self.refresh_token}
        )
        response.raise_for_status()
        tokens = response.json()
        self.refresh_token = tokens["refresh_token"]
        self.client.headers["Authorization"] = f"Bearer {tokens['access_token']}"


async def _register_or_login(client: httpx.AsyncClient, email: str) -> tuple[str, str]:
    """Register the demo user, or log in if already registered (409)."""
    response = await client.post(
        "/auth/register",
        json={"email": email, "password": DEMO_PASSWORD, "device_label": "demo-cli"},
    )
    if response.status_code == 409:
        response = await client.post(
            "/auth/login",
            json={"email": email, "password": DEMO_PASSWORD, "device_label": "demo-cli"},
        )
    response.raise_for_status()
    tokens = response.json()
    return tokens["access_token"], tokens["refresh_token"]


async def _find_peer_id(session: DemoSession, peer_email: str) -> uuid.UUID:
    response = await session.request(
        "POST", "/contacts/sync", json={"identifiers": [peer_email]}
    )
    response.raise_for_status()
    matches = response.json()["matches"]
    if not matches:
        raise RuntimeError(
            f"{peer_email} not found - start the other terminal (scripts/demo.py --as ...) first"
        )
    return uuid.UUID(matches[0]["user_id"])


async def _get_or_create_direct_conversation(
    session: DemoSession, peer_id: uuid.UUID
) -> uuid.UUID:
    response = await session.request(
        "POST", "/conversations", json={"kind": "direct", "member_ids": [str(peer_id)]}
    )
    response.raise_for_status()
    return uuid.UUID(response.json()["id"])


def _print_event(event: dict[str, object]) -> None:
    print(f"\n[{_timestamp()}] <- {json.dumps(event)}")
    print("> ", end="", flush=True)


async def _listen(websocket: ClientConnection) -> None:
    """Background task: print every inbound event as it arrives."""
    async for raw in websocket:
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            print(f"\n[{_timestamp()}] <- (unparseable frame) {raw!r}")
            continue
        _print_event(event)


async def _print_history(session: DemoSession, conversation_id: uuid.UUID) -> None:
    response = await session.request("GET", f"/conversations/{conversation_id}/messages")
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))


async def _typing_burst(websocket: ClientConnection, conversation_id: uuid.UUID) -> None:
    await websocket.send(
        json.dumps({"type": "typing:start", "conversation_id": str(conversation_id)})
    )
    await asyncio.sleep(3)
    await websocket.send(
        json.dumps({"type": "typing:stop", "conversation_id": str(conversation_id)})
    )


async def _read_commands(
    websocket: ClientConnection, session: DemoSession, conversation_id: uuid.UUID
) -> None:
    """Foreground loop: read stdin commands without blocking the event loop.

    input() is blocking, so it runs in a worker thread via
    asyncio.to_thread - the background listener task above keeps printing
    inbound events while this loop waits on the next line.
    """
    loop = asyncio.get_running_loop()
    while True:
        print("> ", end="", flush=True)
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        text = line.strip()
        if not text:
            continue

        if text == "/quit":
            return
        if text == "/history":
            await _print_history(session, conversation_id)
            continue
        if text == "/typing":
            asyncio.create_task(_typing_burst(websocket, conversation_id))
            continue
        if text.startswith("/read "):
            message_id = text.removeprefix("/read ").strip()
            await websocket.send(
                json.dumps(
                    {
                        "type": "message:read",
                        "conversation_id": str(conversation_id),
                        "message_id": message_id,
                    }
                )
            )
            continue
        if text.startswith("/delete "):
            message_id = text.removeprefix("/delete ").strip()
            await websocket.send(
                json.dumps(
                    {
                        "type": "message:delete",
                        "conversation_id": str(conversation_id),
                        "message_id": message_id,
                    }
                )
            )
            continue
        if text.startswith("/edit "):
            remainder = text.removeprefix("/edit ").strip()
            message_id, _, new_body = remainder.partition(" ")
            if not new_body:
                print("usage: /edit <message_id> <new text>")
                continue
            await websocket.send(
                json.dumps(
                    {
                        "type": "message:edit",
                        "conversation_id": str(conversation_id),
                        "message_id": message_id,
                        "body": new_body,
                    }
                )
            )
            continue
        if text.startswith("/"):
            print(f"Unknown command: {text}")
            continue

        await websocket.send(
            json.dumps(
                {
                    "type": "message:send",
                    "conversation_id": str(conversation_id),
                    "client_message_id": str(uuid.uuid4()),
                    "body": text,
                }
            )
        )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--as", dest="as_user", choices=sorted(DEMO_USERS), required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    self_email = DEMO_USERS[args.as_user]
    peer_email = next(email for name, email in DEMO_USERS.items() if name != args.as_user)

    base_url = f"http://{args.host}:{args.port}"
    ws_url = f"ws://{args.host}:{args.port}/ws"

    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        access_token, refresh_token = await _register_or_login(client, self_email)
        client.headers["Authorization"] = f"Bearer {access_token}"
        session = DemoSession(client=client, refresh_token=refresh_token)

        peer_id = await _find_peer_id(session, peer_email)
        conversation_id = await _get_or_create_direct_conversation(session, peer_id)
        print(f"Connected as {self_email}, conversation with {peer_email}: {conversation_id}")

        async with ws_connect(
            ws_url, additional_headers={"Authorization": f"Bearer {access_token}"}
        ) as websocket:
            authenticated = json.loads(await websocket.recv())
            assert authenticated["type"] == "auth:authenticated", authenticated

            await websocket.send(
                json.dumps({"type": "conversation:join", "conversation_id": str(conversation_id)})
            )
            joined = json.loads(await websocket.recv())
            assert joined["type"] == "conversation:joined", joined
            print(f"Joined conversation {conversation_id}. Type a message, or /quit to exit.")

            listener = asyncio.create_task(_listen(websocket))
            try:
                await _read_commands(websocket, session, conversation_id)
            finally:
                listener.cancel()
                try:
                    await listener
                except (asyncio.CancelledError, Exception):
                    pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\ninterrupted, exiting")
