import logging
import time
import uuid
from json import JSONDecodeError
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, WebSocketException, status
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.auth.tokens import TokenClaims
from app.conversations.models import ConversationMember
from app.conversations.service import ConversationAccessError, get_conversation_for_member
from app.core.config import Settings
from app.core.logging import bind_request_id
from app.core.metrics import (
    active_connections,
    database_errors,
    message_send_latency_seconds,
    messages_failed,
    messages_sent,
    redis_errors,
)
from app.messages.models import Message
from app.messages.schemas import MessageOut
from app.messages.service import (
    MessageMutationError,
    ReceiptError,
    advance_last_acknowledged_sequence,
    delete_message,
    edit_message,
    get_missing_messages,
    mark_delivered,
    mark_read,
    persist_message,
)
from app.presence.service import record_heartbeat, remove_connection
from app.presence.typing_indicators import start_typing, stop_typing
from app.redis.pubsub import EventFanout
from app.redis.rate_limit import check_rate_limit
from app.websocket.auth import authenticate_websocket
from app.websocket.manager import ConnectionManager
from app.websocket.schemas import (
    ConversationJoinEvent,
    ConversationLeaveEvent,
    MessageDeleteEvent,
    MessageEditEvent,
    MessageReadEvent,
    MessageSendEvent,
    PresenceHeartbeatEvent,
    TypingStartEvent,
    TypingStopEvent,
    inbound_event_adapter,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.websocket("/ws")
async def authenticated_websocket(websocket: WebSocket) -> None:
    manager: ConnectionManager = websocket.app.state.connection_manager
    session_factory: async_sessionmaker[AsyncSession] = websocket.app.state.session_factory
    settings: Settings = websocket.app.state.settings
    redis: Redis = websocket.app.state.redis
    fanout: EventFanout = websocket.app.state.fanout

    if not manager.accepting_connections:
        raise WebSocketException(code=status.WS_1013_TRY_AGAIN_LATER, reason="server shutting down")

    try:
        claims = await authenticate_websocket(websocket, session_factory, settings)
    except WebSocketException as exc:
        logger.info("websocket_auth_rejected reason=%s", exc.reason)
        raise
    await websocket.accept()
    await manager.connect(websocket, claims.user_id)
    # A fresh id per physical connection (not the JWT session_id): two tabs
    # on the same login session must be tracked as independent liveness
    # entries, so one tab closing can never clear the other's presence.
    # Reused below as the log correlation id for this connection's entire
    # lifetime (see bind_request_id) - every log line emitted while handling
    # this connection's events, including several call frames deep (e.g. a
    # presence or rate-limit failure), is automatically tagged with it.
    connection_id = uuid.uuid4()
    active_connections.inc()
    connected_at = time.monotonic()

    with bind_request_id(str(connection_id)):
        await _refresh_presence_heartbeat(redis, claims.user_id, connection_id, settings)
        logger.info(
            "websocket_connected user_id=%s connection_id=%s", claims.user_id, connection_id
        )
        await websocket.send_json({"type": "auth:authenticated"})

        try:
            while True:
                try:
                    raw = await websocket.receive_json()
                except (JSONDecodeError, UnicodeDecodeError, TypeError):
                    await websocket.send_json({"type": "error", "detail": "invalid json"})
                    continue
                # Any inbound traffic - even something that fails validation
                # below - proves the connection is alive right now.
                await _refresh_presence_heartbeat(redis, claims.user_id, connection_id, settings)
                await _handle_event(
                    websocket, manager, fanout, session_factory, redis, settings, claims, raw
                )
        except WebSocketDisconnect:
            pass
        finally:
            await manager.disconnect(websocket)
            await _clear_presence_connection(redis, claims.user_id, connection_id)
            active_connections.dec()
            logger.info(
                "websocket_disconnected user_id=%s connection_id=%s duration_s=%.1f",
                claims.user_id,
                connection_id,
                time.monotonic() - connected_at,
            )


async def _refresh_presence_heartbeat(
    redis: Redis, user_id: uuid.UUID, connection_id: uuid.UUID, settings: Settings
) -> None:
    """Best-effort: presence is ephemeral Redis state, never the source of
    truth (a project design rule), so a Redis hiccup here must never take down the
    connection or block the event that triggered this heartbeat - the same
    fire-and-forget trade-off already accepted for cross-instance fanout
    (see app.redis.pubsub.EventFanout). Worst case, presence goes briefly
    stale instead of the whole connection dying; discovered as a genuine gap
    by tests/websocket/test_failure_scenarios.py, which characterized the
    unwrapped call raising past this handler entirely on a Redis outage.
    """
    try:
        await record_heartbeat(
            redis,
            user_id,
            connection_id,
            heartbeat_ttl_seconds=settings.presence_heartbeat_ttl_seconds,
        )
    except Exception:
        redis_errors.labels(scope="presence_heartbeat").inc()
        logger.warning("presence_heartbeat_failed user_id=%s", user_id, exc_info=True)


async def _clear_presence_connection(
    redis: Redis, user_id: uuid.UUID, connection_id: uuid.UUID
) -> None:
    """Best-effort, same reasoning as _refresh_presence_heartbeat. This runs
    in the disconnect `finally` block, so letting it raise would replace
    whatever caused the disconnect (or a clean close) with an unhandled
    Redis error escaping the ASGI app - doubling one failure into two.
    """
    try:
        await remove_connection(redis, user_id, connection_id)
    except Exception:
        redis_errors.labels(scope="presence_disconnect_cleanup").inc()
        logger.warning("presence_disconnect_cleanup_failed user_id=%s", user_id, exc_info=True)


async def _handle_event(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    raw: Any,
) -> None:
    try:
        event = inbound_event_adapter.validate_python(raw)
    except ValidationError:
        await websocket.send_json({"type": "error", "detail": "invalid event"})
        return

    if isinstance(event, ConversationJoinEvent):
        await _handle_join(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, ConversationLeaveEvent):
        await _handle_leave(websocket, manager, event)
    elif isinstance(event, MessageSendEvent):
        await _handle_message_send(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, MessageReadEvent):
        await _handle_message_read(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, MessageEditEvent):
        await _handle_message_edit(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, MessageDeleteEvent):
        await _handle_message_delete(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, TypingStartEvent):
        await _handle_typing_start(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, TypingStopEvent):
        await _handle_typing_stop(
            websocket, manager, fanout, session_factory, redis, settings, claims, event
        )
    elif isinstance(event, PresenceHeartbeatEvent):
        pass  # already refreshed above; no reply needed


async def _handle_join(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: ConversationJoinEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="conversation_action",
        user_id=claims.user_id,
        max_events=settings.rate_limit_conversation_action_max_events,
        window_seconds=settings.rate_limit_conversation_action_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not await _is_authorized_member(session_factory, event.conversation_id, claims.user_id):
        await _send_error(websocket, "conversation not found", event.conversation_id)
        return
    await manager.join(websocket, event.conversation_id)
    await websocket.send_json(
        {"type": "conversation:joined", "conversation_id": str(event.conversation_id)}
    )
    await _replay_missing_messages(
        websocket, manager, fanout, session_factory, settings, claims, event.conversation_id
    )


async def _handle_leave(
    websocket: WebSocket,
    manager: ConnectionManager,
    event: ConversationLeaveEvent,
) -> None:
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "not joined", event.conversation_id)
        return
    await manager.leave(websocket, event.conversation_id)
    await websocket.send_json(
        {"type": "conversation:left", "conversation_id": str(event.conversation_id)}
    )


async def _handle_message_send(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: MessageSendEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="message_send",
        user_id=claims.user_id,
        max_events=settings.rate_limit_message_send_max_events,
        window_seconds=settings.rate_limit_message_send_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before sending", event.conversation_id)
        return

    # Measures receive-to-ack latency for a successful send only: an
    # authorization/rate-limit rejection above never reaches persistence, so
    # timing it would mix "how long the DB took" with "how long the client
    # took to get rejected" in the same histogram.
    started = time.monotonic()
    async with session_factory() as session:
        try:
            await get_conversation_for_member(session, event.conversation_id, claims.user_id)
        except ConversationAccessError:
            await _send_error(websocket, "conversation not found", event.conversation_id)
            return
        try:
            result = await persist_message(
                session,
                conversation_id=event.conversation_id,
                sender_id=claims.user_id,
                client_message_id=event.client_message_id,
                body=event.body,
            )
        except Exception:
            messages_failed.inc()
            database_errors.labels(scope="message_persist").inc()
            logger.exception("message_persist_failed conversation_id=%s", event.conversation_id)
            await _send_error(websocket, "failed to send message", event.conversation_id)
            return
        # The sender already has this message locally (they just sent it),
        # so their own replay cursor advances immediately rather than
        # waiting for a delivery receipt they can never receive for their
        # own send (see app.messages.service.mark_delivered).
        await advance_last_acknowledged_sequence(
            session,
            conversation_id=event.conversation_id,
            user_id=claims.user_id,
            sequence_number=result.message.sequence_number,
        )

    message = result.message
    # Persisted before broadcast: the row above is already committed, so a
    # dropped connection or a crash from here on never loses the message.
    if not result.is_duplicate:
        await _broadcast(
            manager,
            fanout,
            event.conversation_id,
            {
                "type": "message:new",
                "id": str(message.id),
                "conversation_id": str(message.conversation_id),
                "sender_id": str(message.sender_id),
                "sequence_number": message.sequence_number,
                "body": message.body,
                "created_at": message.created_at.isoformat(),
            },
        )
        # DELIVERED is automatic: computed from who is actually still in the
        # room right after the broadcast above, so a connection that failed
        # mid-send (and was pruned as stale by manager.broadcast) is
        # correctly not marked delivered. See app.messages.service.mark_delivered.
        #
        # This only accounts for recipients connected to *this* instance.
        # A recipient connected to a different instance still gets the live
        # message:new push (handle_remote_event relays it), but is not
        # marked DELIVERED at that moment - see handle_remote_event's
        # docstring for why that DB write deliberately does not happen on
        # the cross-instance path, and how it still converges correctly via
        # offline-recovery replay instead.
        await _mark_delivered_for_room(
            manager, fanout, session_factory, event.conversation_id, message
        )
    await websocket.send_json(
        {
            "type": "message:ack",
            "client_message_id": str(event.client_message_id),
            "id": str(message.id),
            "sequence_number": message.sequence_number,
            "duplicate": result.is_duplicate,
        }
    )
    messages_sent.inc()
    message_send_latency_seconds.observe(time.monotonic() - started)


async def _mark_delivered_for_room(
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    conversation_id: uuid.UUID,
    message: Message,
) -> None:
    """Mark DELIVERED for every recipient of `message` connected to *this instance*.

    Called both right after a local send (_handle_message_send) and when a
    message:new for this message arrives from another instance via Redis
    Pub/Sub (handle_remote_event), so a given recipient is marked delivered
    by whichever instance actually holds their live socket - each instance
    only ever looks at its own ConnectionManager (manager.room_user_ids).
    mark_delivered is idempotent, so if the same user is connected via more
    than one instance at once (multi-device), the second instance's attempt
    is simply a no-op rather than a duplicate receipt/broadcast.
    """
    recipient_ids = manager.room_user_ids(conversation_id, exclude_user_id=message.sender_id)
    if not recipient_ids:
        return
    events: list[dict[str, Any]] = []
    async with session_factory() as session:
        for recipient_id in recipient_ids:
            event = await _mark_delivered(
                session, conversation_id=conversation_id, message=message, recipient_id=recipient_id
            )
            if event is not None:
                events.append(event)
    # Session closed - every receipt/cursor write above is committed before
    # any socket write below, so there is nothing left "in flight" for a
    # disconnect racing this call to interrupt. See _replay_missing_messages
    # for why that ordering matters, not just for tidiness.
    for event in events:
        await _broadcast(manager, fanout, conversation_id, event)


async def _mark_delivered(
    session: AsyncSession,
    *,
    conversation_id: uuid.UUID,
    message: Message,
    recipient_id: uuid.UUID,
) -> dict[str, Any] | None:
    """Mark one recipient DELIVERED and advance their replay cursor to match.

    Pure DB work, no socket I/O - shared by live delivery (broadcast time,
    see _mark_delivered_for_room) and offline replay (reconnect time, see
    _replay_missing_messages) so both paths share one receipt + cursor
    bookkeeping implementation instead of two parallel ones. Returns the
    message:delivered event to broadcast, or None if this recipient was
    already delivered (nothing changed, nothing to announce).
    """
    result = await mark_delivered(session, message=message, user_id=recipient_id)
    if not result.changed:
        return None
    assert result.receipt.delivered_at is not None
    await advance_last_acknowledged_sequence(
        session,
        conversation_id=conversation_id,
        user_id=recipient_id,
        sequence_number=message.sequence_number,
    )
    return {
        "type": "message:delivered",
        "conversation_id": str(conversation_id),
        "message_id": str(message.id),
        "user_id": str(recipient_id),
        "delivered_at": result.receipt.delivered_at.isoformat(),
    }


async def _replay_missing_messages(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    claims: TokenClaims,
    conversation_id: uuid.UUID,
) -> None:
    """Send everything this member hasn't acknowledged yet, oldest first.

    Runs on every conversation:join, which is also the reconnect path: the
    in-memory ConnectionManager room membership does not survive a dropped
    connection (app.websocket.manager.ConnectionManager), so a client always
    re-joins after reconnecting, and that is where recovery naturally hooks
    in. A member's cursor starts at 0 (see
    app.conversations.models.ConversationMember), so a member joining for
    the first time - not just reconnecting - also catches up on history
    instead of only seeing messages sent from that point forward.

    All DB work (the query plus every receipt/cursor write) happens and is
    committed *before* the first socket write below. A dropped connection -
    including a test harness that stops reading frames as soon as it has
    what it wants - can only ever race the socket writes at the end, never
    an in-flight DB transaction: cancelling a task mid-write is recoverable
    (the next reconnect just replays it again), but cancelling one while
    SQLAlchemy's async bridge is mid-query is not - it can wedge the
    connection instead of raising, which previously showed up as this
    handler hanging forever on conversation:join when nothing was actually
    left to replay.

    Known race: a message broadcast live to this socket between
    manager.join() (in _handle_join, just before this call) and the query
    below can appear in both that live message:new and this replay batch,
    since its sequence_number is still > the cursor read here. Clients are
    expected to de-dup by message id, the same way they already de-dup
    their own retried sends by client_message_id.
    """
    async with session_factory() as session:
        member = await session.get(
            ConversationMember, {"conversation_id": conversation_id, "user_id": claims.user_id}
        )
        assert member is not None  # membership already checked in _handle_join
        replay = await get_missing_messages(
            session,
            conversation_id=conversation_id,
            after_sequence=member.last_acknowledged_sequence,
            limit=settings.message_replay_batch_limit,
        )
        if not replay.messages:
            return

        delivered_events: list[dict[str, Any]] = []
        for message in replay.messages:
            if message.sender_id == claims.user_id:
                # Own prior send: already has it, just needed the cursor
                # caught up (normally already true - see the cursor advance
                # in _handle_message_send - this is a defensive fallback).
                await advance_last_acknowledged_sequence(
                    session,
                    conversation_id=conversation_id,
                    user_id=claims.user_id,
                    sequence_number=message.sequence_number,
                )
                continue
            event = await _mark_delivered(
                session,
                conversation_id=conversation_id,
                message=message,
                recipient_id=claims.user_id,
            )
            if event is not None:
                delivered_events.append(event)

        replay_payload = {
            "type": "message:replay",
            "conversation_id": str(conversation_id),
            "has_more": replay.has_more,
            "messages": [_replay_item(message) for message in replay.messages],
        }

    # Session closed - everything above is committed. Only socket writes
    # remain, so a disconnect from here on can never lose or corrupt state.
    await websocket.send_json(replay_payload)
    for event in delivered_events:
        await _broadcast(manager, fanout, conversation_id, event)


def _replay_item(message: Message) -> dict[str, Any]:
    """Represent one replayed row reflecting its *current* state, never the
    original stale content - the same current-state view a client already
    gets from a live message:edited/message:deleted, so a client that missed
    the live edit/delete event while offline converges to the same picture
    on reconnect (see README's Offline recovery section).

    Delegates to MessageOut.from_message - the same current-state
    representation used by GET /conversations/{id}/messages (see
    app.conversations.router), so the two read paths can never disagree
    about what "current state" means for an edited/deleted message.
    """
    return MessageOut.from_message(message).model_dump(mode="json", exclude_none=True)


async def _handle_message_read(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: MessageReadEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="conversation_action",
        user_id=claims.user_id,
        max_events=settings.rate_limit_conversation_action_max_events,
        window_seconds=settings.rate_limit_conversation_action_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before marking read", event.conversation_id)
        return

    async with session_factory() as session:
        try:
            await get_conversation_for_member(session, event.conversation_id, claims.user_id)
        except ConversationAccessError:
            await _send_error(websocket, "conversation not found", event.conversation_id)
            return

        message = await session.get(Message, event.message_id)
        if message is None or message.conversation_id != event.conversation_id:
            await _send_error(websocket, "message not found", event.conversation_id)
            return

        try:
            result = await mark_read(session, message=message, user_id=claims.user_id)
        except ReceiptError:
            await _send_error(
                websocket, "cannot mark own message as read", event.conversation_id
            )
            return

    if not result.changed:
        return

    assert result.receipt.read_at is not None
    await _broadcast(
        manager,
        fanout,
        event.conversation_id,
        {
            "type": "message:read",
            "conversation_id": str(event.conversation_id),
            "message_id": str(event.message_id),
            "user_id": str(claims.user_id),
            "read_at": result.receipt.read_at.isoformat(),
        },
    )


async def _handle_message_edit(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: MessageEditEvent,
) -> None:
    # Shares message:send's rate-limit budget, not conversation_action's -
    # an edit is the same kind of write (persists a new body) and abuse
    # surface as a send, not a cheap membership-check event like join/read.
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="message_send",
        user_id=claims.user_id,
        max_events=settings.rate_limit_message_send_max_events,
        window_seconds=settings.rate_limit_message_send_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before editing", event.conversation_id)
        return

    async with session_factory() as session:
        try:
            await get_conversation_for_member(session, event.conversation_id, claims.user_id)
        except ConversationAccessError:
            await _send_error(websocket, "conversation not found", event.conversation_id)
            return

        message = await session.get(Message, event.message_id)
        if message is None or message.conversation_id != event.conversation_id:
            await _send_error(websocket, "message not found", event.conversation_id)
            return

        try:
            result = await edit_message(
                session, message=message, user_id=claims.user_id, body=event.body
            )
        except MessageMutationError as exc:
            await _send_error(websocket, str(exc), event.conversation_id)
            return

    if not result.changed:
        return  # idempotent no-op retry: nothing to broadcast, see edit_message

    assert result.message.edited_at is not None
    # No `exclude`: like message:read, the sender is still a room member and
    # this broadcast is their own confirmation - no separate ack event.
    await _broadcast(
        manager,
        fanout,
        event.conversation_id,
        {
            "type": "message:edited",
            "id": str(result.message.id),
            "conversation_id": str(result.message.conversation_id),
            "sender_id": str(result.message.sender_id),
            "sequence_number": result.message.sequence_number,
            "body": result.message.body,
            "created_at": result.message.created_at.isoformat(),
            "edited_at": result.message.edited_at.isoformat(),
        },
    )


async def _handle_message_delete(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: MessageDeleteEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="message_send",
        user_id=claims.user_id,
        max_events=settings.rate_limit_message_send_max_events,
        window_seconds=settings.rate_limit_message_send_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before deleting", event.conversation_id)
        return

    async with session_factory() as session:
        try:
            await get_conversation_for_member(session, event.conversation_id, claims.user_id)
        except ConversationAccessError:
            await _send_error(websocket, "conversation not found", event.conversation_id)
            return

        message = await session.get(Message, event.message_id)
        if message is None or message.conversation_id != event.conversation_id:
            await _send_error(websocket, "message not found", event.conversation_id)
            return

        try:
            result = await delete_message(session, message=message, user_id=claims.user_id)
        except MessageMutationError as exc:
            await _send_error(websocket, str(exc), event.conversation_id)
            return

    if not result.changed:
        return  # idempotent no-op retry: already deleted, see delete_message

    assert result.message.deleted_at is not None
    await _broadcast(
        manager,
        fanout,
        event.conversation_id,
        {
            "type": "message:deleted",
            "conversation_id": str(result.message.conversation_id),
            "message_id": str(result.message.id),
            "deleted_at": result.message.deleted_at.isoformat(),
        },
    )


async def _handle_typing_start(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: TypingStartEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="typing",
        user_id=claims.user_id,
        max_events=settings.rate_limit_typing_max_events,
        window_seconds=settings.rate_limit_typing_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before typing", event.conversation_id)
        return
    if not await _is_authorized_member(session_factory, event.conversation_id, claims.user_id):
        await _send_error(websocket, "conversation not found", event.conversation_id)
        return

    try:
        should_broadcast = await start_typing(
            redis,
            event.conversation_id,
            claims.user_id,
            ttl_seconds=settings.typing_indicator_ttl_seconds,
            rate_limit_seconds=settings.typing_rate_limit_seconds,
        )
    except Exception:
        # Best-effort, same reasoning as _refresh_presence_heartbeat: typing
        # state is ephemeral Redis-only state (never touches PostgreSQL - see
        # README), so a Redis hiccup here must not crash the connection.
        # Worst case, this one typing signal is silently dropped.
        redis_errors.labels(scope="typing").inc()
        logger.warning(
            "typing_state_write_failed conversation_id=%s user_id=%s",
            event.conversation_id,
            claims.user_id,
            exc_info=True,
        )
        return
    if not should_broadcast:
        return  # rate-limited: a start signal for this pair was just sent

    await _broadcast(
        manager,
        fanout,
        event.conversation_id,
        {
            "type": "typing:start",
            "conversation_id": str(event.conversation_id),
            "user_id": str(claims.user_id),
        },
        exclude=websocket,
    )


async def _handle_typing_stop(
    websocket: WebSocket,
    manager: ConnectionManager,
    fanout: EventFanout,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    settings: Settings,
    claims: TokenClaims,
    event: TypingStopEvent,
) -> None:
    if not await _enforce_rate_limit(
        websocket,
        redis,
        scope="typing",
        user_id=claims.user_id,
        max_events=settings.rate_limit_typing_max_events,
        window_seconds=settings.rate_limit_typing_window_seconds,
        conversation_id=event.conversation_id,
    ):
        return
    if not manager.is_member(websocket, event.conversation_id):
        await _send_error(websocket, "join conversation before typing", event.conversation_id)
        return
    if not await _is_authorized_member(session_factory, event.conversation_id, claims.user_id):
        await _send_error(websocket, "conversation not found", event.conversation_id)
        return

    try:
        was_typing = await stop_typing(redis, event.conversation_id, claims.user_id)
    except Exception:
        redis_errors.labels(scope="typing").inc()
        logger.warning(
            "typing_state_write_failed conversation_id=%s user_id=%s",
            event.conversation_id,
            claims.user_id,
            exc_info=True,
        )
        return
    if not was_typing:
        return  # nothing to announce: typing had already stopped or expired

    await _broadcast(
        manager,
        fanout,
        event.conversation_id,
        {
            "type": "typing:stop",
            "conversation_id": str(event.conversation_id),
            "user_id": str(claims.user_id),
        },
        exclude=websocket,
    )


async def _is_authorized_member(
    session_factory: async_sessionmaker[AsyncSession],
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
) -> bool:
    async with session_factory() as session:
        try:
            await get_conversation_for_member(session, conversation_id, user_id)
        except ConversationAccessError:
            return False
    return True


async def _enforce_rate_limit(
    websocket: WebSocket,
    redis: Redis,
    *,
    scope: str,
    user_id: uuid.UUID,
    max_events: int,
    window_seconds: int,
    conversation_id: uuid.UUID,
) -> bool:
    """Return whether `claims.user_id` is within the abuse-protection limit for `scope`.

    Keyed by the JWT-authenticated user_id, never anything client-supplied.
    Checked before any membership lookup or database work in the callers
    below, so a flood of events from a non-member (or a removed former
    member) cannot force repeated `get_conversation_for_member` /
    `_is_authorized_member` database round-trips - the whole point of
    protecting these events is to bound the *cost* of handling a flood, not
    just the number of successful broadcasts.
    """
    key = f"ratelimit:{scope}:{user_id}"
    try:
        decision = await check_rate_limit(
            redis, key, max_events=max_events, window_seconds=window_seconds
        )
    except Exception:
        # Fail open, same reasoning as app.core.rate_limit's HTTP dependency:
        # rate limiting is abuse protection, not a correctness guarantee, and
        # must not turn a Redis outage into every message:send/typing:*
        # failing outright. Logged loudly so this is visible, not silent.
        redis_errors.labels(scope="ws_rate_limit").inc()
        logger.warning("rate_limit_check_failed scope=%s key=%s", scope, key, exc_info=True)
        return True
    if decision.allowed:
        return True
    await websocket.send_json(
        {"type": "error", "detail": "rate limited", "conversation_id": str(conversation_id)}
    )
    return False


async def _send_error(websocket: WebSocket, detail: str, conversation_id: uuid.UUID) -> None:
    await websocket.send_json(
        {"type": "error", "detail": detail, "conversation_id": str(conversation_id)}
    )


async def _broadcast(
    manager: ConnectionManager,
    fanout: EventFanout,
    conversation_id: uuid.UUID,
    payload: dict[str, Any],
    *,
    exclude: WebSocket | None = None,
) -> None:
    """Deliver an event to this instance's local room members, then relay it
    to every other instance so their own local room members get it too.

    `exclude` only ever refers to a WebSocket object live in *this*
    process's memory, so it is meaningless to other instances - it is
    applied to the local broadcast only. Other instances hold entirely
    disjoint connection objects, so publishing the raw payload to them
    (with no exclude) is always correct: their own room members are simply
    whoever is actually joined and connected there.

    fanout.publish is deliberately not awaited: it schedules the
    cross-instance PUBLISH as a background task and returns immediately
    (see EventFanout.publish), so this function's caller is never blocked
    on a Redis round-trip after the local delivery above has already
    happened.
    """
    await manager.broadcast(conversation_id, payload, exclude=exclude)
    fanout.publish(conversation_id, payload)


async def handle_remote_event(
    manager: ConnectionManager,
    conversation_id: uuid.UUID,
    payload: dict[str, Any],
) -> None:
    """Fan an event published by another FastAPI instance into this instance's
    locally-connected sockets (see app.redis.pubsub.EventFanout.listen).

    Only this instance's own ConnectionManager is touched here - the event
    was already broadcast to the originating instance's local sockets
    before it was published, so this call only reaches the recipients this
    specific process can see.

    Deliberately does no database work. This coroutine runs on the
    background fanout-listener task (started in app.main's lifespan), not
    on a foreground per-connection task - and an earlier version of this
    function *did* re-run delivery marking here (a session.get plus
    mark_delivered writes) for message:new events, scoped to this
    instance's local room members, so a recipient connected to this
    instance still got marked DELIVERED even though the message arrived via
    another instance. That coupled this background task's lifecycle to
    in-flight database writes: SQLAlchemy's aiosqlite dialect wraps some of
    its own connection cleanup in asyncio.shield() (deliberately
    uncancellable - the same failure class documented on
    tests/websocket/test_presence.py::test_activity_refreshes_heartbeat for
    a different code path), so a shutdown or reconnect racing a dispatch
    mid-write could wedge instead of completing. In practice this produced
    an intermittent hang plus a receipt-ordering race under test load, and
    rather than keep chasing the exact interleaving, the DB write was
    removed from this path entirely.

    Consequence: a message pushed live to a recipient connected to a
    *different* instance than the sender is relayed here but not marked
    DELIVERED at that moment. It still converges correctly, just not
    instantly: DELIVERED and the recipient's replay cursor both catch up
    the next time *their own* instance handles a conversation:join for them
    (reconnect, a second device joining, etc.) - see
    _replay_missing_messages, which runs on that instance's own foreground
    request-handling task, where a database write is safe. This is a
    deliberate scope decision: cross-instance delivery status converges
    through the existing offline-recovery path rather than through a
    second, less-safe live-write path in the background listener.
    """
    await manager.broadcast(conversation_id, payload)
