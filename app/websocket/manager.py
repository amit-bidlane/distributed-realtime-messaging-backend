import asyncio
import uuid
from collections import defaultdict
from typing import Any

from fastapi import WebSocket


class ConnectionManager:
    """Tracks authenticated WebSocket connections and their conversation rooms.

    Membership in a room here only controls live broadcast fan-out; it is not
    an authorization decision. Authorization is re-checked against PostgreSQL
    (the source of truth) via app.conversations.service on every join and send.
    """

    def __init__(self) -> None:
        self._connections: dict[WebSocket, set[uuid.UUID]] = {}
        self._connection_users: dict[WebSocket, uuid.UUID] = {}
        self._rooms: dict[uuid.UUID, set[WebSocket]] = defaultdict(set)
        self._accepting = True
        self._lock = asyncio.Lock()

    @property
    def accepting_connections(self) -> bool:
        return self._accepting

    async def connect(self, websocket: WebSocket, user_id: uuid.UUID) -> None:
        async with self._lock:
            self._connections[websocket] = set()
            self._connection_users[websocket] = user_id

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            conversation_ids = self._connections.pop(websocket, set())
            self._connection_users.pop(websocket, None)
            for conversation_id in conversation_ids:
                self._discard_from_room(conversation_id, websocket)

    async def join(self, websocket: WebSocket, conversation_id: uuid.UUID) -> None:
        async with self._lock:
            self._connections[websocket].add(conversation_id)
            self._rooms[conversation_id].add(websocket)

    async def leave(self, websocket: WebSocket, conversation_id: uuid.UUID) -> None:
        async with self._lock:
            self._connections.get(websocket, set()).discard(conversation_id)
            self._discard_from_room(conversation_id, websocket)

    def is_member(self, websocket: WebSocket, conversation_id: uuid.UUID) -> bool:
        return conversation_id in self._connections.get(websocket, set())

    def room_user_ids(
        self, conversation_id: uuid.UUID, *, exclude_user_id: uuid.UUID | None = None
    ) -> set[uuid.UUID]:
        """Users with at least one connection currently joined to this room.

        Used to decide who a just-broadcast message was actually delivered
        to live: call this *after* broadcast() so any connection that failed
        mid-send (and was pruned as stale) is correctly excluded.
        """
        user_ids = {
            self._connection_users[connection]
            for connection in self._rooms.get(conversation_id, set())
            if connection in self._connection_users
        }
        if exclude_user_id is not None:
            user_ids.discard(exclude_user_id)
        return user_ids

    async def broadcast(
        self,
        conversation_id: uuid.UUID,
        message: dict[str, Any],
        *,
        exclude: WebSocket | None = None,
    ) -> None:
        async with self._lock:
            recipients = [
                connection
                for connection in self._rooms.get(conversation_id, set())
                if connection is not exclude
            ]
        stale: list[WebSocket] = []
        for connection in recipients:
            try:
                await connection.send_json(message)
            except Exception:
                stale.append(connection)
        for connection in stale:
            await self.disconnect(connection)

    async def begin_shutdown(self) -> list[WebSocket]:
        """Stop accepting new connections and notify/close everyone connected.

        Returns the connections that were notified, for callers that want to
        confirm delivery (mainly tests).
        """
        async with self._lock:
            self._accepting = False
            connections = list(self._connections)

        for connection in connections:
            try:
                await connection.send_json({"type": "server:shutting_down"})
            except Exception:
                continue
        for connection in connections:
            try:
                await connection.close(code=1001)
            except Exception:
                continue
        return connections

    def _discard_from_room(self, conversation_id: uuid.UUID, websocket: WebSocket) -> None:
        room = self._rooms.get(conversation_id)
        if room is None:
            return
        room.discard(websocket)
        if not room:
            del self._rooms[conversation_id]
