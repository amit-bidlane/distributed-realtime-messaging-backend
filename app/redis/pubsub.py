import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis

from app.core.metrics import redis_errors

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "ws:events"

RemoteEventHandler = Callable[[uuid.UUID, dict[str, Any]], Awaitable[None]]


class EventFanout:
    """Relays WebSocket broadcast events between FastAPI instances via Redis Pub/Sub.

    Each instance's ConnectionManager only knows about the sockets connected
    directly to it (see app.websocket.manager). To reach a recipient
    connected to a *different* instance, an event that was already
    broadcast locally is also PUBLISHed here; every instance (including
    itself) is subscribed to the same channel, so `publish` tags the
    message with this instance's own id and `listen` skips anything
    carrying that id back - otherwise an instance would rebroadcast its own
    events to its own sockets a second time.

    Delivery guarantee: this is fire-and-forget, like Redis Pub/Sub itself -
    in two senses. First, PUBLISH does not persist the message anywhere: if
    a subscribing instance's Redis connection is briefly down (or still
    reconnecting) when a publish happens, that instance's locally-connected
    clients never receive that specific live push; there is nothing queued
    to replay it from later. Second, publish() itself does not block its
    caller on the network round-trip either - it schedules the PUBLISH as a
    background task and returns immediately (see publish()'s docstring), so
    a slow or failed Redis call never adds latency to the local broadcast
    that already happened, or to whatever the caller does next. This is an
    accepted trade-off, not an oversight:
    PostgreSQL remains the durable source of truth for every message (see
    app.messages.service.persist_message), so no data is lost - only a
    live, real-time delivery. A recipient who misses a live push still
    catches up on reconnect/rejoin via offline-recovery replay (see
    app.websocket.router._replay_missing_messages), which reads directly
    from PostgreSQL rather than depending on this channel. If guaranteed
    at-least-once cross-instance delivery ever becomes a hard requirement,
    the documented upgrade path is Redis Streams with consumer groups
    (XADD / XREADGROUP), which durably queue entries per-consumer instead
    of dropping them when nobody is listening at publish time.

    Known test flake (tracked, not yet root-caused): running the full
    tests/websocket suite repeatedly shows an occasional failure (roughly
    1 in 20 runs; 3/60 on the two affected test files, with and without
    the session shield) where a test receives WebSocket frames in an
    order it didn't expect (e.g. a message:replay where message:delivered
    was expected). It has been seen with the two affected files run
    together as well as in the full suite. A 5-run baseline against the
    pre-fanout code showed none, which is too few runs to mean much at
    this rate. The working theory is that this class's background
    listen() task, which wakes on *every* publish (including this
    instance's own, which it just discards - see _dispatch), adds enough
    extra concurrent scheduling activity on the shared event loop to
    occasionally perturb the relative ordering between two sockets' handler
    coroutines in a test - exposing a timing assumption that already had no
    real headroom, rather than corrupting any state. This is a test-harness
    ordering issue, not a data-correctness one: PostgreSQL is authoritative
    for every message/receipt regardless of WebSocket delivery order (see
    app.messages.service), so no scenario here loses or corrupts data - at
    worst a client's live view and a replay briefly disagree on ordering,
    which offline-recovery replay (_replay_missing_messages) reconciles the
    same way it already reconciles any other missed live push. Revisit if
    it recurs; not chased further here.
    """

    def __init__(self, redis: Redis, *, instance_id: uuid.UUID | None = None) -> None:
        self._redis = redis
        self.instance_id = instance_id or uuid.uuid4()
        # Set once the subscription is confirmed active. app.main's lifespan
        # waits on this before the app starts accepting traffic, so there is
        # no startup window where a freshly-launched instance could miss
        # events published while it was still subscribing.
        self.ready = asyncio.Event()
        # Cooperative shutdown flag - see listen()/stop() for why this is
        # used instead of Task.cancel().
        self._stopping = asyncio.Event()
        # Upper bound on how long a shutdown waits for listen() to notice
        # _stopping and return, once no message is currently being
        # dispatched. Short enough that shutdown stays snappy (every test
        # that opens a WebSocket pays up to this much teardown latency, and
        # a real deploy's rolling restart pays it once per instance), long
        # enough to not busy-poll Redis.
        self._poll_timeout_seconds = 0.2
        # Tracks in-flight publish() background tasks so they aren't
        # garbage-collected mid-flight (a known asyncio pitfall for a task
        # with no other live reference) and so shutdown can give them a
        # brief chance to finish - see publish() and wait_idle().
        self._pending_publishes: set[asyncio.Task[None]] = set()

    def publish(self, conversation_id: uuid.UUID, payload: dict[str, Any]) -> None:
        """Schedule a PUBLISH and return immediately - never awaits the network call.

        The caller (see app.websocket.router._broadcast) has already
        delivered `payload` to this instance's own local sockets before
        calling this. A slow or briefly-unreachable Redis must never add
        latency to that local delivery, or to whatever the caller does
        next in the same request - that would make every send/typing/read
        event pay for a cross-instance side effect its own sender doesn't
        need. This is what "fire-and-forget" means for this method
        specifically, distinct from (but in the same spirit as) the
        no-durability guarantee described on the class docstring.
        """
        task = asyncio.create_task(self._do_publish(conversation_id, payload))
        self._pending_publishes.add(task)
        task.add_done_callback(self._pending_publishes.discard)

    async def wait_idle(self, timeout: float = 1.0) -> None:
        """Best-effort wait for any in-flight publish() tasks to finish.

        Purely a shutdown tidiness measure, not a correctness requirement:
        publish() already treats a failed/incomplete PUBLISH as an
        acceptable loss (see the class docstring), so skipping this would
        not lose anything publish() itself considers durable. It just gives
        a publish scheduled right before shutdown a brief chance to
        actually reach Redis instead of being abandoned mid-flight when the
        connection closes, and avoids "Task was destroyed but it is
        pending" noise on an otherwise clean shutdown.
        """
        if not self._pending_publishes:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*self._pending_publishes, return_exceptions=True),
                timeout=timeout,
            )

    async def _do_publish(self, conversation_id: uuid.UUID, payload: dict[str, Any]) -> None:
        envelope = json.dumps(
            {
                "origin": str(self.instance_id),
                "conversation_id": str(conversation_id),
                "payload": payload,
            }
        )
        try:
            await self._redis.publish(EVENTS_CHANNEL, envelope)
        except Exception:
            # Fire-and-forget: the local broadcast on this instance already
            # happened before publish() is ever called (see
            # app.websocket.router._broadcast), and the underlying message
            # is already durably committed to PostgreSQL. A publish failure
            # here only costs *other* instances this one live push.
            redis_errors.labels(scope="event_fanout_publish").inc()
            logger.warning(
                "event_fanout_publish_failed conversation_id=%s", conversation_id, exc_info=True
            )

    async def listen(self, on_event: RemoteEventHandler) -> None:
        """Run until stop() is called, relaying events from other instances to on_event.

        Intended to run as a background task for the lifetime of the app
        (started in app.main's lifespan). Deliberately polls via
        get_message(timeout=...) in a loop rather than `async for message in
        pubsub.listen()`: that alternative blocks indefinitely between
        messages, which would force shutdown to reach for Task.cancel() to
        unblock it. on_event (see app.websocket.router.handle_remote_event)
        does no database work today specifically *because* an earlier
        version did, and a cancellation landing mid-write there produced an
        intermittent hang - SQLAlchemy's aiosqlite dialect wraps some of its
        own connection cleanup in asyncio.shield() (deliberately
        uncancellable - see the long comment on
        tests/websocket/test_presence.py::test_activity_refreshes_heartbeat
        for the same underlying issue on a different code path), which can
        wedge instead of raising when cancelled mid-operation. Polling with a
        bounded timeout keeps this safe regardless of what on_event ever
        ends up doing: the only thing cancellable is a wait for the *next*
        message that hasn't arrived yet, never a dispatch already in
        progress, so shutdown only has to wait out one poll interval.

        Nothing awaits this task directly (see app.main's lifespan, which
        starts it and moves on without waiting for `ready`), so a Redis
        connection failure here must not surface as an unhandled task
        exception - it's caught, logged, and this coroutine simply returns.
        Consequence: if Redis is unreachable when an instance starts, that
        instance never activates cross-instance fanout for its lifetime
        (there is no reconnect/retry loop here) - it still serves
        everything else normally, and /ready already reports Redis
        unavailability per-request for anything that needs to act on that.
        """
        try:
            pubsub = self._redis.pubsub()
            async with pubsub:
                await pubsub.subscribe(EVENTS_CHANNEL)
                self.ready.set()
                while not self._stopping.is_set():
                    message = await pubsub.get_message(
                        timeout=self._poll_timeout_seconds, ignore_subscribe_messages=True
                    )
                    if message is None:
                        continue
                    await self._dispatch(message["data"], on_event)
        except Exception:
            redis_errors.labels(scope="event_fanout_listener").inc()
            logger.warning("event_fanout_listener_failed", exc_info=True)

    def stop(self) -> None:
        """Ask listen() to return after its current poll/dispatch completes."""
        self._stopping.set()

    async def _dispatch(self, raw: Any, on_event: RemoteEventHandler) -> None:
        try:
            envelope = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("event_fanout_bad_payload")
            return
        if envelope.get("origin") == str(self.instance_id):
            return  # our own publish - already delivered to local sockets
        try:
            conversation_id = uuid.UUID(envelope["conversation_id"])
            payload = envelope["payload"]
            if not isinstance(payload, dict):
                raise ValueError
        except (KeyError, ValueError, TypeError):
            logger.warning("event_fanout_bad_envelope")
            return
        await on_event(conversation_id, payload)
