# Distributed Real-Time Messaging Backend

A real-time messaging backend built with **Python, FastAPI, and native WebSockets**, engineered
with genuine production discipline - real CI/CD, real load testing, a real security
vulnerability found and fixed - not a CRUD tutorial. It exists to demonstrate distributed-systems
fundamentals end to end: durable message persistence, exactly-once idempotency, strict ordering
under concurrent writers, delivery/read receipts, cross-device presence, offline recovery, and
horizontal scaling across multiple stateless instances behind a load balancer - with the
guarantees and failure modes of every one of those written down, not just claimed.

**PostgreSQL is the durable source of truth for everything a client cannot afford to lose.**
Redis is used only for ephemeral/distributed concerns - presence, cross-instance Pub/Sub, and
rate limiting - and the codebase treats a total Redis outage as a *degraded-service* event, never
a data-loss event. That split, and what happens when each half fails, is the throughline of this
document.

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Why these technologies](#why-these-technologies)
- [Setup](#setup)
- [Database schema](#database-schema)
- [HTTP API](#http-api)
- [WebSocket API](#websocket-api)
- [Message lifecycle: ordering & idempotency](#message-lifecycle-ordering--idempotency)
- [Delivery & read receipts](#delivery--read-receipts)
- [Message editing & deletion](#message-editing--deletion)
- [Presence & typing](#presence--typing-redis-backed-cross-instance)
- [Offline recovery & reconnection](#offline-recovery--reconnection)
- [Horizontal scaling & Redis Pub/Sub](#horizontal-scaling--redis-pubsub)
- [Rate limiting](#rate-limiting)
- [Contact sync](#contact-sync)
- [Observability](#observability)
- [Security](#security)
- [Testing & failure scenarios](#testing--failure-scenarios)
- [Load testing](#load-testing)
- [Trade-offs](#trade-offs)
- [Limitations](#limitations)
- [Future improvements](#future-improvements-documented-not-implemented)

## Features

- Email/password auth with Argon2id hashing, JWT access tokens, rotating refresh tokens, and
  per-device session revocation.
- Direct and group conversations with membership-based authorization on every read/write path.
- Persistent messaging with server-assigned per-conversation sequence numbers and
  client-generated idempotency keys - safe under concurrent senders on different instances.
- Delivery and read receipts with explicit, tested state-transition rules.
- Sender-only message editing and soft-deletion, with edited/deleted state correctly represented
  in both live broadcasts and offline-recovery replay - never stale plaintext, never a gap in the
  sequence.
- Redis-backed presence (online/offline, last seen, multi-device) and rate-limited typing
  indicators, both correct across multiple FastAPI instances.
- Offline recovery: a reconnecting client (or a client joining a conversation for the first time)
  replays everything it hasn't acknowledged yet, sourced from PostgreSQL.
- Horizontal scaling: two FastAPI instances behind Nginx, bridged by a Redis Pub/Sub fanout, with
  the fire-and-forget delivery characteristics of that bridge explicitly documented and tested.
- Redis-backed rate limiting on every abuse-prone HTTP endpoint and WebSocket event.
- Privacy-conscious contact sync (batched lookup, minimal response, nothing persisted).
- Structured JSON logging with request-ID correlation, Prometheus metrics, and graceful shutdown.
- A load test script producing real throughput/p99-latency numbers, not just a scaling claim.

## Architecture

```text
                              Clients
                                 |
                         HTTP + WebSocket
                                 |
                          +------v------+
                          |    Nginx    |   (least_conn, WS upgrade)
                          +------+------+
                                 |
                  +--------------+---------------+
                  |                               |
            +-----v-----+                   +-----v-----+
            | fastapi-1 |                   | fastapi-2 |   (stateless, identical)
            +-----+-----+                   +-----+-----+
                  |                               |
                  +---------------+---------------+
                                  |
                    +-------------+-------------+
                    |                           |
              +-----v-----+               +-----v-----+
              |   Redis   |               | PostgreSQL |
              | presence  |               | durable    |
              | pub/sub   |               | source of  |
              | rate limit|               | truth      |
              +-----------+               +-----------+
```

**Responsibility split:**

| Layer | Owns |
| --- | --- |
| **PostgreSQL** | Users, sessions/devices, conversations, memberships, messages, per-conversation sequence counters, delivery/read receipts. Everything a client must never lose. |
| **Redis** | Per-connection presence, cross-instance Pub/Sub fanout (`ws:events`), typing indicators, fixed-window rate-limit counters. Nothing here is ever the only copy of anything that matters. |
| **FastAPI (x2)** | HTTP API, WebSocket API, auth, authorization, all business logic. Fully stateless - either instance can serve any request; in-memory WebSocket connection state (`ConnectionManager`) exists only for the lifetime of a socket and never needs to survive a restart. |
| **Nginx** | Reverse proxy, `least_conn` load balancing, WebSocket upgrade, and the thing that makes "two backend instances" a real deployment topology instead of a paper claim. |

A one-off `migrate` service runs `alembic upgrade head` exactly once before either FastAPI
instance starts (see [Setup](#setup) for why that's a dedicated service rather than each instance
migrating itself).

## Why these technologies

- **FastAPI + native WebSockets, not Socket.IO.** A design goal of this project is to
  demonstrate the underlying mechanics - handshake auth, connection lifecycle, broadcast fanout -
  rather than let a higher-level library hide them. Socket.IO would remove exactly the parts this
  project exists to show.
- **PostgreSQL over "just use Redis for everything."** Every guarantee this project makes
  (idempotency, ordering, durability) needs ACID transactions and real constraints. Redis is
  excellent at ephemeral/distributed state but is the wrong tool for "the one copy of a message
  that must never disappear."
- **SQLAlchemy 2.0 async + Alembic** for a typed, migration-tracked schema instead of hand-rolled
  SQL strings scattered across the codebase.
- **Argon2id** (via `argon2-cffi`) over bcrypt/PBKDF2 - the current OWASP-recommended default for
  password hashing, with tunable memory cost that resists GPU cracking.
- **No microservices, no Kafka, no Kubernetes.** Two FastAPI processes behind Nginx already prove
  horizontal scaling and cross-instance coordination; none of these guarantees needed a bigger
  hammer, and the project's scope rules them out absent a concrete requirement.
- **Redis Pub/Sub over Redis Streams for cross-instance fanout**, deliberately, as a scoped
  trade-off - see [Horizontal scaling](#horizontal-scaling--redis-pubsub) for exactly what that
  costs and why Streams is the documented upgrade path if guaranteed delivery ever becomes a
  hard requirement.
- **Prometheus client + structured JSON logs** over a heavier observability stack - enough to
  answer "is this healthy, and if not, where" without adding infrastructure the project doesn't
  need to run locally in Docker Compose.

## Setup

```bash
cp .env.example .env
docker compose up --build
```

This starts `postgres`, `redis`, a one-off `migrate` service (runs `alembic upgrade head` once -
see the race it avoids in [Horizontal scaling](#horizontal-scaling--redis-pubsub)), `fastapi-1`,
`fastapi-2`, and `nginx`. Everything has a real `healthcheck`, and every `depends_on` uses
`condition: service_healthy` / `service_completed_successfully`, so `docker compose up --build`
from a clean checkout reaches a fully working state without a manual retry.

- Load-balanced entrypoint: `http://localhost:8080` (`GET /health`, `GET /ready`, everything below).
- Direct per-instance access (for tests that need to pin a client to a specific backend, e.g.
  proving cross-instance delivery deterministically): `http://localhost:8001` (`fastapi-1`),
  `http://localhost:8002` (`fastapi-2`).

**Local (non-Docker) development / running checks:**

```bash
source .venv/Scripts/activate   # Windows/Git Bash; use .venv/bin/activate elsewhere
pip install -e ".[dev]"
pytest --timeout=30 --timeout-method=thread
ruff check .
mypy app/
```

`pytest` runs against SQLite (`aiosqlite`) + `fakeredis` for `tests/unit` and
`tests/integration`, and a real ASGI app instance over `TestClient` WebSockets for
`tests/websocket` - no Docker required to run the suite. See [Testing](#testing--failure-scenarios)
for the one known flake and why it's tracked rather than "fixed" by masking it.

**CI** (`.github/workflows/ci.yml`) runs these exact same checks - `ruff check .`, `mypy app/`,
`pytest --timeout=30 --timeout-method=thread` (see Testing's cross-thread SQLite deadlock fix for
why), and a Docker build check - automatically on every push and pull request to `main`, with
`ruff`/`mypy`/`pytest` as separate jobs so a lint failure doesn't hide a typecheck or test
failure. Any job failing fails the workflow. The `test` job runs plain `pytest` with no Postgres/
Redis service containers alongside it: every test constructs its own `Settings` with an explicit
SQLite (`aiosqlite`) + `fakeredis` override rather than reading `DATABASE_URL`/`REDIS_URL`, so
real service containers would sit unused - added infrastructure and startup time for nothing
actually exercised. The Docker build job runs `docker build` against `docker/Dockerfile` with no
registry push - a build-check, not a deploy step; there's no deploy target for this project (see
[Limitations](#limitations)), and a build doesn't need Postgres/Redis running either. Python is
pinned to `3.13` in the workflow to match `docker/Dockerfile`'s `python:3.13-slim`, and pip
dependencies are cached between runs.

## Database schema

```text
users                          conversations                    conversation_sequence_counters
├─ id (pk)                     ├─ id (pk)                        ├─ conversation_id (pk, fk)
├─ email (unique)              ├─ kind (direct|group)             └─ last_sequence
├─ password_hash               ├─ title
└─ created_at                  ├─ direct_key (unique, nullable)
                                └─ created_by (fk -> users)
devices                        conversation_members
├─ id (pk)                     ├─ conversation_id (pk, fk)       messages
├─ user_id (fk -> users)       ├─ user_id (pk, fk)               ├─ id (pk)
└─ label                       ├─ role (owner|member)             ├─ conversation_id (fk)
                                ├─ joined_at                       ├─ sender_id (fk -> users)
sessions                       └─ last_acknowledged_sequence       ├─ client_message_id
├─ id (pk)                                                        ├─ sequence_number
├─ user_id (fk -> users)       message_receipts                   ├─ body
├─ device_id (fk -> devices)   ├─ message_id (pk, fk)              ├─ created_at
├─ refresh_token_digest        ├─ user_id (pk, fk)                 ├─ edited_at (nullable)
│  (unique - hash, not the     ├─ delivered_at (nullable)          └─ deleted_at (nullable)
│  raw refresh token)          └─ read_at (nullable)
├─ expires_at                                                     unique(conversation_id,
├─ revoked_at (nullable)                                            sender_id, client_message_id)
├─ created_at                                                     index(conversation_id,
└─ last_used_at                                                     sequence_number)
```

Notable design choices, not just structure:

- **`conversation_sequence_counters`** is a dedicated one-row-per-conversation counter, created
  alongside its conversation, incremented via a single atomic `UPDATE ... RETURNING`. This is
  what makes ordering safe under concurrent writers - see
  [Message lifecycle](#message-lifecycle-ordering--idempotency).
- **`uq_messages_conversation_sender_client`** (unique on `conversation_id, sender_id,
  client_message_id`) is the actual source of truth for idempotency, not an application-level
  check that could race.
- **`conversation_members.last_acknowledged_sequence`** doubles as the offline-recovery cursor -
  no separate tracking table. See [Offline recovery](#offline-recovery--reconnection).
- **`message_receipts`** has no stored SENT marker: a persisted `messages` row *is* "sent." A row
  keyed `(message_id, user_id)` only exists once at least DELIVERED.
- **`messages.edited_at` / `messages.deleted_at`** are nullable timestamps, not a separate table -
  edit/delete are in-place mutations of the same row, not new rows. A soft-deleted row keeps its
  `sequence_number` and clears `body` server-side, so the plaintext is gone but the conversation's
  ordering never has a gap. See [Message editing & deletion](#message-editing--deletion).
- **`sessions.refresh_token_digest`** stores a digest of the refresh token, never the raw token,
  and is what gets checked/invalidated on refresh and logout/revocation.
- **`direct_key`** is a unique, deterministic key derived from a direct conversation's two member
  IDs, used to make "create or reuse the existing 1:1 conversation" idempotent at the database
  level (`POST /conversations` returns `200` with the existing conversation instead of creating a
  duplicate - see [HTTP API](#http-api)).

Migrations live in `alembic/`; `alembic upgrade head` applies them (see [Setup](#setup) for how
this runs exactly once in Docker Compose).

## HTTP API

All endpoints except registration/login/refresh require `Authorization: Bearer <access_token>`.

| Method & path | Auth | Purpose |
| --- | --- | --- |
| `GET /health` | none | Liveness - always `200 {"status": "ok"}` if the process is up. |
| `GET /ready` | none | Readiness - `200` only if PostgreSQL and Redis both answer; `503` otherwise. |
| `GET /metrics` | none | Prometheus exposition format - see [Observability](#observability). |
| `POST /auth/register` | rate-limited | Create an account (email/password/device label) and receive an access + refresh token pair. `409` if the email is already registered. |
| `POST /auth/login` | rate-limited | Exchange credentials for a fresh token pair. `401` on invalid credentials. |
| `POST /auth/refresh` | refresh token in body | Rotate a refresh token for a new access + refresh pair. `401` if the refresh token is invalid, expired, or revoked. |
| `POST /auth/logout` | bearer | Revoke the current session. |
| `GET /auth/me` | bearer | The authenticated user's id. |
| `POST /conversations` | bearer | Create a direct or group conversation. Direct conversations are idempotent by member pair (`direct_key`) - a repeat call returns the existing conversation with `200` instead of creating a duplicate. |
| `GET /conversations` | bearer | Cursor-paginated list of the caller's conversations, newest first. `?limit=` (1-100, default 50) and `?cursor=`. |
| `GET /conversations/{id}` | bearer, member-only | Conversation detail including membership list. `404` if the caller isn't a member (never `403` - membership is not leaked to non-members). |
| `GET /conversations/{id}/messages` | bearer, member-only | Cursor-paginated message history, newest first (`sequence_number` descending). `?limit=` (1-100, default 50) and `?cursor=`. Each item reflects the message's *current* state (edited body, or `deleted_at` with no `body`) - see [Message editing & deletion](#message-editing--deletion). `404` if the caller isn't a member, same non-leaking pattern as `GET /conversations/{id}`. See [Offline recovery](#offline-recovery--reconnection) for how this differs from `message:replay`. |
| `POST /conversations/{id}/members` | bearer, owner-only | Add a member. `403` if the caller isn't authorized to manage membership, `422` if the target user doesn't exist. |
| `DELETE /conversations/{id}/members/{member_id}` | bearer, owner-only | Remove a member. |
| `POST /contacts/sync` | bearer, rate-limited | Batch-match submitted email identifiers against registered accounts. See [Contact sync](#contact-sync). |

Request/response bodies are Pydantic models (`app/*/schemas.py`); FastAPI serves interactive docs
at `/docs` for the exact shapes.

## WebSocket API

Connect to `GET /ws` (upgraded by Nginx at `ws://localhost:8080/ws`, or directly per-instance).
**The JWT access token goes in the handshake `Authorization: Bearer <token>` header - never in
the query string**, since query strings end up in proxy/access logs. A missing/invalid token
closes the connection with WebSocket close code `1008` (policy violation) before `accept()`. On
success the server sends `auth:authenticated` and the connection is live.

**Inbound events** (client → server), each validated against a discriminated Pydantic union -
anything that doesn't match one of these shapes gets back `{"type": "error", "detail": "invalid
event"}`:

| Event | Payload | Effect |
| --- | --- | --- |
| `conversation:join` | `conversation_id` | Joins the room (membership-checked); triggers offline-recovery replay of everything unacknowledged. |
| `conversation:leave` | `conversation_id` | Leaves the room. |
| `message:send` | `conversation_id, client_message_id, body` (1-4000 chars) | Persists, assigns a sequence number, broadcasts, ACKs. See [Message lifecycle](#message-lifecycle-ordering--idempotency). |
| `message:read` | `conversation_id, message_id` | Marks the message READ for the caller. Rejected if the caller is the sender. |
| `message:edit` | `conversation_id, message_id, body` (1-4000 chars) | Updates the message body in place and stamps `edited_at`. Only the original sender may edit; rejected for a deleted message. See [Message editing & deletion](#message-editing--deletion). |
| `message:delete` | `conversation_id, message_id` | Soft-deletes: stamps `deleted_at`, clears `body` server-side. Only the original sender may delete. See [Message editing & deletion](#message-editing--deletion). |
| `typing:start` / `typing:stop` | `conversation_id` | Ephemeral, rate-limited typing indicator. |
| `presence:heartbeat` | *(none)* | Explicit keep-alive for an otherwise-idle client; every inbound event already refreshes presence, so this exists only for clients with nothing else to send. |

**Outbound events** (server → client):

| Event | When |
| --- | --- |
| `auth:authenticated` | Handshake succeeded. |
| `conversation:joined` / `conversation:left` | Ack of a join/leave. |
| `message:new` | A message was persisted to a conversation this connection has joined (local or relayed from another instance). |
| `message:ack` | Reply to the sender's own `message:send`, carrying the assigned `id`, `sequence_number`, and a `duplicate` flag. |
| `message:delivered` | A recipient's connection received the live broadcast (or was caught up via replay). |
| `message:read` | A recipient marked a message read. |
| `message:edited` | A message was edited - same shape as `message:new` plus `edited_at`. |
| `message:deleted` | A message was deleted - `conversation_id, message_id, deleted_at` only, no `body`. |
| `message:replay` | Sent once after `conversation:join`, containing every message after the caller's `last_acknowledged_sequence` (`has_more` flags whether the batch was capped by `message_replay_batch_limit`). Each item reflects the message's *current* state - an edited item carries its current `body` and `edited_at`; a deleted item omits `body` and carries `deleted_at` instead. |
| `typing:start` / `typing:stop` | Another member (never the caller's own connection) started/stopped typing. |
| `server:shutting_down` | Sent to every connection during graceful shutdown, just before the socket is closed - see [Trade-offs](#trade-offs). |
| `error` | `{"detail": ..., "conversation_id": ...}` - malformed input, unauthorized access, not-joined, or rate-limited. |

## Message lifecycle: ordering & idempotency

```text
receive -> validate -> authenticate -> authorize -> idempotency check
        -> persist -> assign sequence -> broadcast -> ACK
```

**Persist before successful broadcast, always** - a message is never announced as sent until its
row is committed in PostgreSQL. A dropped connection or process crash after that point never
loses the message; the recipient (or the sender's own other device) picks it up via replay.

- **Ordering.** Each conversation has a `conversation_sequence_counters` row incremented by a
  single atomic `UPDATE ... RETURNING`. The database's own row-level write lock on that row
  serializes concurrent senders - same process or different instances - targeting the same
  conversation, so `sequence_number` is always contiguous starting at 1: no duplicates, no skips,
  even under concurrent writes from two FastAPI instances at once. (Deliberately not `SELECT
  MAX(sequence)+1`, which races under concurrency.)
- **Idempotency.** A database unique constraint on `(conversation_id, sender_id,
  client_message_id)` is the real source of truth for duplicate detection, not just an
  application-level check. A client retry after a lost ACK is caught by an upfront lookup in the
  common case; a genuine concurrent race (two sends with the same `client_message_id` whose
  lookups both miss) is still caught by the constraint, and the losing transaction rolls back -
  which also reverts its counter increment, so no sequence number is permanently lost to a
  rejected duplicate.
- Both guarantees hold across multiple FastAPI instances writing to the same PostgreSQL database;
  neither depends on any single-process assumption.

## Delivery & read receipts

Per-recipient receipt state lives in `message_receipts`, keyed by `(message_id, user_id)`. There
is no stored SENT marker - a persisted row in `messages` *is* "sent." From there, for a given
(message, recipient) pair:

| Transition | Trigger | Who can trigger it |
| --- | --- | --- |
| *(none)* -> DELIVERED | Server automatically records delivery the moment `message:new` is successfully broadcast to a recipient's currently-joined WebSocket connection. | Automatic; no client event exists for this. |
| *(none)* -> READ | Client sends `message:read` for a message it never received a live broadcast for (e.g. joined late). Both `delivered_at` and `read_at` are stamped, since a read implies delivery even if the live push was missed. | The recipient, via `message:read`. |
| DELIVERED -> READ | Client sends `message:read`; `read_at` is stamped, the original `delivered_at` is left untouched. | The recipient, via `message:read`. |
| READ -> READ | Repeat `message:read` calls are a no-op: idempotent, no timestamp change, no rebroadcast. | - |

A message's sender can never receive a delivery/read receipt for their own message (rejected with
an error). DELIVERED never regresses once READ. Delivery is computed strictly from *live*
WebSocket presence in the conversation's room right after broadcast - a recipient not currently
connected and joined stays in the SENT state until they read it; offline delivery replay is a
separate concern (see [Offline recovery](#offline-recovery--reconnection)). Every `message:read`
call is re-authorized against conversation membership the same way `message:send` is, so a member
removed mid-session can't mark messages read even with an open connection.

**Cross-instance caveat.** Automatic DELIVERED-marking only ever considers recipients connected
to the *same* instance that received the send. A recipient on a different instance still gets the
live `message:new` push (relayed via Redis Pub/Sub), but isn't marked DELIVERED at that moment;
their receipt and offline-recovery cursor both catch up the next time *their own* instance
handles a `conversation:join` for them. This is a deliberate scope decision, not an oversight: an
earlier version performed this DB write on the cross-instance path, and coupling that write to
the background Redis-listener task's lifecycle produced an intermittent shutdown hang under test
load (see `app.redis.pubsub.EventFanout` and `app.websocket.router.handle_remote_event`).
`message:read` is unaffected and does fan out live across instances, since marking READ is always
a foreground, client-triggered write on the recipient's own instance.

## Message editing & deletion

`message:edit` and `message:delete` mutate an existing `messages` row in place - neither creates a
new row nor removes one. Both reuse `message:send`'s rate-limit budget (same write cost, same
abuse surface) and the same sender-identity authorization pattern `message:read` already uses for
rejecting the sender: identity comes from the JWT-authenticated `user_id`, never anything
client-supplied, and only the row's original `sender_id` may mutate it.

**State model** (per message row):

| Transition | Trigger | Who can trigger it |
| --- | --- | --- |
| *(none)* -> edited | `message:edit`: `body` is overwritten, `edited_at` stamped. | The original sender only. |
| edited -> edited | Another `message:edit`: `body` overwritten again, `edited_at` re-stamped. | The original sender only. |
| *(none)* / edited -> deleted | `message:delete`: `deleted_at` stamped, `body` replaced server-side with an empty string so the plaintext is no longer retained (design rule: never retain plaintext beyond what's needed). The row and its `sequence_number` are kept - deletion never opens a gap in the conversation's ordering. | The original sender only. |
| deleted -> deleted | Repeat `message:delete`: idempotent no-op - `deleted_at` untouched, nothing re-broadcast. | - |
| deleted -> edited | Rejected (`"cannot edit a deleted message"`) - a deleted message can't be resurrected via edit. | - |

Both operations are idempotent against retries: re-submitting `message:edit` with a body identical
to the message's current body is a no-op (`edited_at` untouched, nothing broadcast) rather than an
error or a duplicate `message:edited`; re-submitting `message:delete` on an already-deleted message
is likewise a no-op. This mirrors the no-op-on-repeat pattern `message:read` already uses for
READ -> READ (see [Delivery & read receipts](#delivery--read-receipts)) - the same "a retried
client event should never double-apply or double-broadcast" principle, just without a
`client_message_id`-style dedup key, since the current row state itself is what's compared against.

**Deletion is for every member of the conversation.** There is no per-recipient "delete for me
only" - clients render a `message:deleted` (or a replayed deleted item) as a visible placeholder
("this message was deleted") at that row's position in the conversation, never a silent removal
from history. The row stays; only its content is gone.

**Broadcast, cross-instance, exactly like every other event.** `message:edited` and
`message:deleted` go through the same `_broadcast` helper as `message:new`/`message:read` - local
`ConnectionManager.broadcast` plus a Redis Pub/Sub `PUBLISH` relayed to every other instance (see
[Horizontal scaling](#horizontal-scaling--redis-pubsub)) - so an edit/delete made against one
FastAPI instance is correctly seen by a recipient connected to a different one. Neither broadcast
`exclude`s the acting connection: like `message:read`, the sender is still a room member, so the
broadcast itself is their confirmation - there's no separate ack frame.

**Offline-recovery interaction.** `message:replay` (see
[Offline recovery](#offline-recovery--reconnection)) represents every replayed row in its *current*
state, not the state it was in when originally sent - a client that was offline for an edit or a
delete must converge to the same picture a client that was online for the live event already has.
Concretely: an edited row in a replay batch carries its current `body` and `edited_at`, never the
original text; a deleted row omits `body` entirely and carries `deleted_at` instead, the same shape
`message:deleted` uses live. This is why deletion clears `body` at the database level rather than
just at broadcast time - a replay reads the row straight from PostgreSQL, so if the plaintext were
still sitting in the column, a client that missed the live delete and only catches it via replay
would see the "deleted" plaintext anyway. The row's `sequence_number` is unaffected either way, so
replay ordering has no gaps regardless of how many rows in a batch were edited or deleted.

**Explicitly out of scope for this step** (deliberate, not an oversight - see
[Future improvements](#future-improvements-documented-not-implemented)):

- **Edit history / version tracking.** Only the current `body` is kept; no record of prior
  versions.
- **"Delete for everyone" vs. "delete for me" as separate concepts.** Deletion is unconditionally
  for every member; there's no per-recipient hide.
- **A configurable edit/delete time window.** The sender can edit or delete their own message at
  any time after sending; there's no cutoff.

## Presence & typing (Redis-backed, cross-instance)

Presence is tracked per-connection in Redis, not per-user: `presence:connections:{user_id}` is a
hash of `connection_id -> last_heartbeat`, refreshed on connect and on every subsequent inbound
WebSocket event (or an explicit `presence:heartbeat` for an otherwise-idle client). **A user is
offline only once every connection's heartbeat is missing or older than
`presence_heartbeat_ttl_seconds`** - staleness is computed by comparing timestamps at read time,
not by relying on Redis key expiry (which can't cascade into removing one field from a shared
hash). A stale entry found during a read is pruned as a side effect. `connection_id` is generated
per physical WebSocket connection, not per login session, so two tabs on the same account are
tracked independently - closing one can never clear the other's presence (multi-device).
`presence:last_seen:{user_id}` is a separate key stamped on every heartbeat and on disconnect, so
"last seen" stays queryable after a user goes fully offline.

Typing indicators (`typing:start` / `typing:stop`) are conversation-scoped, ephemeral (a Redis key
with a short TTL, never touching PostgreSQL), and re-authorized against conversation membership on
every event - only current members can start or see typing. Broadcasts exclude the sender's own
connection. `typing:start` is rate-limited to at most one broadcast per
`typing_rate_limit_seconds` per (user, conversation) via a `SET ... NX EX` cooldown key, so a
client sending it on every keystroke doesn't flood the room; the underlying typing flag still
refreshes on every call so the indicator doesn't expire mid-type. An explicit `typing:stop` always
clears that cooldown too, so a genuine stop-then-restart is never mistaken for spam.

**Presence needs no Pub/Sub relay to work across instances** - the state above is written
directly to Redis, which every instance already shares, so any instance reading
`presence:connections:{user_id}` sees the same answer regardless of which instance the user's
connections actually live on. **Typing broadcasts do** need the relay, since the *live push* goes
through each instance's own in-memory `ConnectionManager` (see
[Horizontal scaling](#horizontal-scaling--redis-pubsub)) - the underlying typing-flag state itself
is already shared the same way presence is.

## Offline recovery & reconnection

Every `conversation_members` row carries `last_acknowledged_sequence`: the highest
`messages.sequence_number` in that conversation this member has acknowledged (sent themselves, or
been marked DELIVERED for - live or via replay). It starts at 0, so a member joining a
conversation for the *first* time also replays its full history, not just messages sent after
that point.

Recovery hooks into `conversation:join` - which is also the reconnect path, since the in-memory
`ConnectionManager` room membership does not survive a dropped connection, so a client always
re-joins after reconnecting:

```text
B disconnects
A sends 1, 2, 3           (persisted to PostgreSQL regardless of B's connection state)
B reconnects, sends conversation:join
B receives message:replay { messages: [1, 2, 3], has_more: false }
```

All of the DB work for a replay (the query plus every receipt/cursor write it triggers) is
committed *before* the first socket write. A dropped connection - including a test harness that
stops reading frames as soon as it has what it wants - can only ever race the socket writes at
the end, never an in-flight DB transaction: cancelling a task mid-write is recoverable (the next
reconnect just replays it again), but cancelling one mid-query is not, which previously showed up
as this handler hanging forever.

Replayed rows reflect their *current* state, not their state at send time - see
[Message editing & deletion](#message-editing--deletion) for exactly how an edited or deleted
message is represented in a replay batch.

Batch size is capped by `message_replay_batch_limit` (default 200); `message:replay.has_more`
tells the client whether to expect more history than fit in one batch. **Known race, by design:**
a message broadcast live to a socket between `manager.join()` and the replay query below can
appear in both that live `message:new` and the replay batch (its `sequence_number` is still above
the cursor read moments earlier). Clients are expected to de-dup by message `id`, the same way
they already de-dup their own retried sends by `client_message_id`. Server restarts are covered
too: since recovery state lives entirely in PostgreSQL (the cursor) and messages themselves, a
restarted instance recovers a reconnecting client exactly the same way, with no special-cased
"was this a restart" logic anywhere.

**REST history vs. WebSocket replay - two different mechanisms for two different purposes, not
duplicated functionality.** `message:replay` above answers "what have I missed since my cursor" -
it's WebSocket-only, fires automatically on `conversation:join`, is bounded by
`message_replay_batch_limit`, and as a side effect advances `last_acknowledged_sequence` and marks
messages DELIVERED. `GET /conversations/{id}/messages` (see [HTTP API](#http-api)) answers "let me
page arbitrarily far back through this conversation's history" - it's a plain cursor-paginated REST
endpoint, requires no live socket at all (e.g. rendering history in a web view before connecting),
never touches the replay cursor or delivery receipts, and orders newest-first rather than
replay's oldest-first (each ordering suits its own use case: replay streams forward from a known
point, history paging starts from "now" and walks backward). Both read the same underlying
`messages` rows through the same current-state representation (`MessageOut.from_message`, shared by
`_replay_item` and the REST endpoint), so an edited or deleted message always looks identical
through either path - never the original stale content on one side and current on the other.

Graceful shutdown participates in this story: on `SIGTERM`, the app stops accepting new
connections, sends every connected client a `server:shutting_down` frame, and closes sockets
cleanly - so a client on the losing end of a rolling deploy gets an explicit signal to reconnect
(and replay) rather than a silent drop.

## Horizontal scaling & Redis Pub/Sub

`docker compose up --build` runs two FastAPI instances (`fastapi-1`, `fastapi-2`) behind Nginx,
both stateless and both talking to the same Redis and PostgreSQL. Nginx load-balances plain HTTP
and WebSocket upgrades across both (`nginx/nginx.conf`, `least_conn`). Each instance is also
reachable directly on `localhost:8001` / `localhost:8002` for tests that need to pin a specific
client to a specific instance rather than hope `least_conn` happens to split two clients across
different backends.

**The problem this solves:** `app.websocket.manager.ConnectionManager` only ever knows about the
WebSocket connections attached to its own process - plain in-memory state, not shared. Two clients
in the same conversation connected to *different* instances would never see each other's live
messages without something bridging the two processes.

**The mechanism:** `app.redis.pubsub.EventFanout`. Every event an instance already broadcasts to
its own local sockets (`message:new`, `message:delivered`, `message:read`, `typing:start`,
`typing:stop`) is also `PUBLISH`ed to a single Redis channel (`ws:events`) that every instance
subscribes to, tagged with the publishing instance's own id. Each instance's background listener
task relays every *other* instance's publishes into its own local `ConnectionManager.broadcast`
(a publish tagged with an instance's own id is recognized and dropped by that same instance, so an
event is never delivered twice to the process that originated it). Proven end-to-end by
`tests/websocket/test_multi_instance.py`, which builds two independent app instances sharing one
Redis/one database and shows a message sent through instance 1 arriving at a socket that only
ever connected to instance 2 - `A -> FastAPI #1 -> Redis -> FastAPI #2 -> B`.

**Fire-and-forget, by design, in two senses:**

1. **No durability.** `PUBLISH` does not persist anything. If a subscribing instance's Redis
   connection is briefly down (or still reconnecting) when a publish happens, that instance's
   locally-connected clients simply never receive that specific live push - nothing is queued to
   replay it later. This is an accepted trade-off, not an oversight: PostgreSQL remains the
   source of truth for every message, so no data is lost, only a live delivery. A recipient who
   misses a live push still catches up on their next `conversation:join` via offline-recovery
   replay, which reads from PostgreSQL, not from this channel. If guaranteed at-least-once
   cross-instance delivery ever became a hard requirement, the documented upgrade path is
   **Redis Streams with consumer groups** (`XADD` / `XREADGROUP`), which durably queue entries
   per-consumer instead of dropping them when nobody is listening at publish time.
2. **Non-blocking.** `EventFanout.publish()` schedules the `PUBLISH` as a background task and
   returns immediately rather than awaiting the round-trip inline - a slow or briefly unreachable
   Redis never adds latency to a client's own send/typing/read request.

**Startup/shutdown.** Starting the background listener does not block the app from accepting
traffic - an instance with a momentarily-unreachable Redis still starts and serves everything
else, matching `/ready`'s own per-request Redis check rather than duplicating it as a hard
boot-time requirement. If Redis is unreachable at startup, that instance simply never activates
cross-instance fanout (no reconnect loop) - it degrades live cross-instance delivery, not
correctness. On shutdown, the listener is asked to stop cooperatively (a polled loop checking a
stop flag, not `asyncio.Task.cancel()`); a hard cancel landing mid-database-write, from an earlier
design where the listener did DB work, previously produced an intermittent shutdown hang.
`Task.cancel()` is kept only as a last-resort fallback if Redis itself is unresponsive.

**Cross-instance DELIVERED marking is deliberately out of scope for the background listener** -
see the caveat in [Delivery & read receipts](#delivery--read-receipts). That was the direct fix
for the shutdown hang above, not an unrelated simplification.

**Two infrastructure races fixed along the way**, both worth knowing about if you're reading the
Compose file:

- *Migration race.* Each FastAPI instance originally ran `alembic upgrade head` on its own
  startup - two instances doing this concurrently against a fresh database race to create
  `alembic_version`, and one loses with a `UniqueViolationError`. A dedicated one-off `migrate`
  service now runs it exactly once; neither FastAPI container migrates itself anymore.
- *Nginx startup race.* `postgres`/`redis` always had `healthcheck` + `depends_on:
  condition: service_healthy`; `fastapi-1`/`fastapi-2` didn't, so Nginx's `depends_on` only waited
  for the containers to *start*, not for uvicorn inside them to actually be accepting connections.
  Fixed by adding a healthcheck to each FastAPI service (polling its own `/health` via Python's
  `urllib`, since the `python:3.13-slim` base image has neither `curl` nor `wget`) and switching
  Nginx to the same `condition: service_healthy` form.

## Rate limiting

Redis-backed, fixed-window counters protect the endpoints/events with the highest abuse
potential: `POST /auth/login`, `POST /auth/register`, `message:send` (`message:edit`/
`message:delete` share this same budget - same write cost, same abuse surface as a send),
`typing:start`/`stop`, and `conversation:join`/`message:read`. The core primitive is
`app.redis.rate_limit.check_rate_limit`:
an `INCR` + `EXPIRE ... NX` pipeline that atomically increments a per-key counter and, only on
that key's first use, attaches a TTL - so a crash between the two calls can never leave a counter
incremented with no expiry. This is a **fixed** window, not a sliding one: up to roughly `2x
max_events` can land across a window boundary in the worst case. That known trade-off is accepted
for an implementation simple enough to explain and test in one call.

**HTTP endpoints** (`app.core.rate_limit`): keyed by client IP (`X-Forwarded-For`, set by Nginx,
falling back to `request.client.host`), running as route `dependencies=[...]` so a caller over
the limit gets `429` with `Retry-After` before the handler (and its DB work) ever runs. Login and
registration are tracked under separate keys.

**WebSocket events** (`app.websocket.router._enforce_rate_limit`): keyed by the
JWT-authenticated `user_id` - never anything client-supplied - and checked *before* any
conversation-membership lookup, so a flood from a caller targeting a conversation they don't
belong to is still bounded, not just floods that reach a successful send. `conversation:join` and
`message:read` share one budget with each other (same reasoning as typing below); `join` in
particular can trigger an up-to-`message_replay_batch_limit`-row replay query, making repeated
joins the more expensive of the two. A rejected event gets `{"type": "error", "detail": "rate
limited", ...}`.

This is deliberately independent of `typing_rate_limit_seconds` (the per-conversation `SET ... NX
EX` cooldown that coalesces `typing:start` broadcasts): that cooldown throttles how often a
broadcast goes out; this rate limit bounds how many typing events per user are even accepted for
processing, protecting Redis/CPU from a client that ignores the cooldown.

All six limits (login, register, message send, typing, conversation join/read, contact sync) are
configurable via `Settings` (`rate_limit_*_max_attempts` / `_max_events` /
`_max_requests` + `_window_seconds` pairs - see `app/core/config.py`).

## Contact sync

`POST /contacts/sync` accepts a batch of client-submitted contact identifiers and returns which
ones belong to a registered account, without the client ever needing to know a user's internal ID
up front.

**Identifiers are emails**, normalized the same way registration/login already normalize them
(trim + lowercase). An identifier that doesn't normalize to a valid email (a phone-only contact, a
blank entry - real address books are messy) is silently skipped rather than failing the whole
batch.

**Privacy-conscious by construction, not by filtering after the fact:**

- The response contains only matches, each with just `identifier` (the caller's own submitted
  string, echoed back - never the normalized form, never anything read from the matched user's
  row) and `user_id`. No profile field of the matched account is ever returned.
- A single batched `SELECT ... WHERE email IN (...)` covers every identifier in the request, so
  there's no per-identifier round-trip whose timing could be used to probe registration status
  identifier-by-identifier.
- The requester's own account is filtered out of its own results.
- Requests are IP-rate-limited (`contacts_rate_limit`), bounding how fast any caller can run
  repeated matching sweeps.

**Nothing submitted is stored.** `match_contacts` is a stateless, read-only lookup - not even a
normalized copy of the submitted identifiers is written anywhere. There's no concrete requirement
here that needs persistence, and a full address book should never be stored without one.

## Observability

- **Structured JSON logs** (`app.core.logging.JsonFormatter`): every line is
  `{timestamp, level, logger, message, request_id?, error_type?, exc_info?}`.
- **Request-ID correlation**: one ID per HTTP request (middleware-assigned) or per WebSocket
  connection (the connection's own `connection_id`, bound for its entire lifetime), attached to
  every log line emitted while handling it - including several call frames deep (a presence or
  rate-limit failure, for instance) - via a `ContextVar`, so nothing needs to thread a
  `request_id` parameter through every function signature.
- **Prometheus metrics** at `GET /metrics`: `active_connections` (gauge), `messages_sent_total` /
  `messages_failed_total` (counters), `message_send_latency_seconds` (histogram, receive-to-ACK
  for successful sends only - rejections never enter it, to avoid mixing "DB took a while" with
  "client got rejected instantly" in the same histogram), and `redis_errors_total` /
  `database_errors_total` (counters labeled by call-site `scope`, e.g.
  `presence_heartbeat`, `message_persist`, `ws_rate_limit`).
- **WebSocket lifecycle logs**: connect/disconnect (with duration), auth rejections, and every
  degraded-Redis warning below.
- **Never logged**: passwords, tokens/secrets, or plaintext message content - see
  [Security](#security).

## Security

- **Passwords**: Argon2id (`argon2-cffi`), never stored or logged in plaintext.
- **Tokens**: short-lived JWT access tokens (default 15 min) plus rotating refresh tokens
  (default 30 days); refresh tokens are stored as a digest (`sessions.refresh_token_digest`), not
  the raw value, and are per-device (`devices`/`sessions`), so logout/revocation targets one
  session without affecting a user's other devices.
- **WebSocket auth**: the JWT goes in the handshake `Authorization` header, never a query-string
  parameter - query strings land in proxy/access logs. An invalid/missing token closes the
  connection with WS close code `1008` before `accept()`.
- **Authorization on every path**: conversation membership is re-checked on every relevant HTTP
  and WebSocket action (`get_conversation_for_member`), not just at join time - a member removed
  mid-session loses access on their very next action even if their socket is still open. Client-
  supplied user IDs are never trusted for identity; the JWT-authenticated `user_id` is the only
  source of truth used for authorization and rate-limit keys.
- **Never logged**: passwords, JWTs/refresh tokens, or plaintext message bodies. Log lines
  reference IDs (`user_id`, `conversation_id`, `message_id`), never content.
- **Found and fixed: SQL-parameter logging leak.** Every `logger.*` call site in this codebase
  was already careful to reference only IDs, never message content - but that turned out not to
  be the whole story. SQLAlchemy's own `StatementError.__str__` embeds a failing statement's
  *bound parameters* (a `[parameters: ...]` suffix) into the exception's own string
  representation by default, and that string is exactly what `logger.exception(...)` /
  `exc_info=True` renders into the traceback text once `JsonFormatter` actually looks at
  `exc_info`. Concretely: any unexpected DB
  failure logged from `app.websocket.router._handle_message_send` (a genuine `IntegrityError`,
  say - not just a caught duplicate) would put the sender's plaintext message body into the
  structured log, in violation of the never-log-message-content rule, with no individual `logger.*` call needing to do
  anything wrong for it to happen. This wasn't theorized and left as a note - it was reproduced:
  `test_engine_hides_bound_parameters_so_message_bodies_never_leak_via_exceptions`
  (`tests/integration/test_observability.py`) forces a real `IntegrityError` on a duplicate
  message insert carrying a sensitive body, logs it exactly the way the WebSocket handler does,
  and asserts the body text is absent from the rendered log line. The fix is at the
  engine-construction level, not the logging level, so it can't be re-introduced by a future
  call site forgetting a precaution: `create_async_engine(..., hide_parameters=True)`
  (`app/db/session.py`) strips bound parameters from every `StatementError`'s string
  representation for the life of the engine. That test is now a permanent regression guard, not
  a one-off repro.
- **Audited, not assumed: Redis exceptions don't have the same risk.** The same question was
  asked of the Redis client - could a command failure's exception string embed, say, a rate-limit
  key or presence data the way SQLAlchemy embeds bound SQL parameters? `redis-py`'s exceptions
  (`redis.exceptions.*`) don't carry command arguments in their string representation the way
  SQLAlchemy's `StatementError` does - there's no equivalent `hide_parameters` needed, and none
  of the Redis-touching content logged anywhere here (rate-limit keys, presence connection ids)
  is sensitive in the first place. Recorded here because it was checked, not because there was
  nothing to check.
- **Rate limiting** on every abuse-prone surface (see [Rate limiting](#rate-limiting)), fixed-window
  fail-open under Redis outage rather than fail-closed - see
  [Testing & failure scenarios](#testing--failure-scenarios) for why that's a deliberate choice,
  not an oversight.
- **Production secrets guard**: `Settings` refuses to start with `APP_ENV=production` if either
  `JWT_SECRET` or `REFRESH_TOKEN_PEPPER` is still the checked-in development default.
- **Contact sync** is privacy-conscious by construction - see [Contact sync](#contact-sync).
- **E2E encryption is explicitly out of scope** - see
  [Future improvements](#future-improvements-documented-not-implemented).

## Testing & failure scenarios

**Suite layout.** `tests/unit/` covers pure functions with no FastAPI app involved (JWT
encode/decode and claim validation, Argon2 password hashing, the `check_rate_limit` fixed-window
primitive). `tests/integration/` exercises the HTTP API and service-layer functions against a real
(SQLite-backed) database and `fakeredis`. `tests/websocket/` drives the full app end-to-end over
real WebSocket connections (FastAPI's `TestClient`), including multi-instance and
failure-scenario tests. Every critical flow - unit, integration, WebSocket, Redis,
multi-instance, idempotency, reconnect, offline recovery, multi-device presence - has coverage.
The automated suite runs on SQLite and fakeredis; PostgreSQL behavior is exercised through
`docker compose` and `scripts/load_test.py`, not by `pytest`.

**Manual exercising.** `scripts/demo.py` is an interactive two-terminal WebSocket client for
manually driving the whole system - auth, messaging, typing, receipts, edit/delete, offline
recovery, REST history - without a frontend. Run `.venv/Scripts/python scripts/demo.py --as alice`
in one terminal and `--as bob` in another; add `--port` to pin a terminal to a specific instance
(`8001`/`8002`) and prove cross-instance delivery the same way `test_multi_instance.py` does.

**Redis failure.** An earlier version of this code let an unreachable Redis raise straight out of
the WebSocket handler on the very next inbound event (presence heartbeat ran unconditionally
before every dispatch), killing the connection - and then failed a *second* time inside its own
disconnect cleanup, which also touched Redis. This was fixed, not just documented: presence
heartbeat/cleanup, the WebSocket and HTTP rate-limit checks, and typing-indicator writes now all
catch a Redis failure, log a `WARNING` naming it (visibility into how often this happens in
practice was a hard requirement), and degrade gracefully:

- **Presence and typing** are ephemeral Redis-only state to begin with - a missed heartbeat or
  typing update is simply skipped.
- **Rate limiting fails open, not closed**, on both the WebSocket and HTTP paths: rate limiting is
  abuse protection, not a security boundary the way authentication is, so a Redis outage must not
  take down login or messaging entirely just because Redis - never the source of truth for
  anything here - is briefly unreachable. Abuse protection is temporarily unenforced during the
  outage; that's judged a smaller cost than a full functional outage, and it's loud (a `WARNING`
  per occurrence), not a silent gap.

`test_websocket_survives_a_full_redis_outage_via_fail_open_degradation` proves this end-to-end
against a Redis double that raises on every command: heartbeat, send, typing, and disconnect all
survive, and every degraded call site is confirmed to have logged. A companion test sends well
past the configured login rate limit while Redis is down and confirms every request still
succeeds - proving the limit is genuinely unenforced, not merely still within budget.

**PostgreSQL failure.** `_handle_message_send` wraps `persist_message` in a try/except (log, send
`{"type": "error", "detail": "failed to send message"}`, return) -
`test_message_send_returns_graceful_error_when_database_fails` confirms the client gets that
explicit error frame, not a crash or a silently dropped message, and that the same connection
keeps working for the next send afterward.

**Instance failure.**
`test_client_recovers_through_a_surviving_instance_after_its_own_instance_fails` disconnects a
client from instance 1 for good (standing in for it crashing) while the other party keeps sending
through instance 2, then reconnects the first client *through instance 2 instead*.
Offline-recovery replay delivers everything missed, exactly as an ordinary reconnect would -
proving recovery has no dependency on which instance a client lands back on, because everything
it needs lives in PostgreSQL/Redis, not in any one instance's in-memory `ConnectionManager`.

**Client disconnect, duplicate retry, simultaneous messages, offline recipient, and server
restart** are covered by `test_disconnect_cleans_up_connection_manager`,
`test_message_send_duplicate_client_message_id_is_not_rebroadcast`,
`test_concurrent_sends_get_distinct_sequential_sequence_numbers` /
`test_concurrent_identical_retries_produce_exactly_one_message`,
`test_message_not_delivered_when_recipient_not_joined`, and
`test_server_restart_preserves_offline_recovery_state`.

**Known test flake (tracked, not root-caused).** Running `tests/websocket` repeatedly shows an
occasional failure where a test receives WebSocket frames in an order it didn't expect (for
example `message:replay`, `message:delivered` or `message:read` arriving where another frame was
expected). Measured: 3 failing runs out of 60 on the two affected test files
(`tests/websocket/test_message_edit_delete.py` and `tests/websocket/test_receipts.py`), the same
with and without the session shield described below; an earlier 20-run loop of the full suite
(with the shield) saw 3 failures. It has been seen with the two affected files run together as
well as in the full suite; earlier checks did not see it with a failing test run alone. A 5-run
baseline against the pre-fanout code showed none, which is too few runs to say anything at this
rate. Working theory (untested): `EventFanout.listen()`'s background task wakes on every publish
(including this instance's own, which it discards), adding enough scheduling activity to
occasionally perturb two sockets' relative handler ordering in a test with no real headroom on
that assumption. Not a data-correctness issue, since PostgreSQL is authoritative regardless of
WebSocket delivery order.

**Found and fixed: cross-thread SQLite connection-pooling deadlock in CI.** The WebSocket test
suite occasionally hung indefinitely in CI (reproducible there, not locally) partway through
`tests/websocket/test_offline_recovery.py`. Root cause: `app.db.session.create_engine` used
SQLAlchemy's default pooled engine for every dialect, including the SQLite (`aiosqlite`) engine
these tests run against. `TestClient.websocket_connect()` opens each WebSocket connection under
its own `anyio` blocking-portal thread, and a pooled aiosqlite connection checked out under one
portal's event loop could still be mid-teardown (`asyncio.shield(...)`) when a different portal
thread tried to check it back out - a cross-thread race that deadlocked both. This surfaced
specifically in `test_offline_recovery.py` because it's the only test file performing three
sequential connect -> disconnect -> reconnect cycles in one test, the connection churn needed to
trigger the race reliably. Fixed for that cause: `create_engine` now forces `poolclass=NullPool`
for any `sqlite` URL (a fresh connection per checkout, never shared across portal threads), while
PostgreSQL in production keeps the tuned pool from the load-testing results below.
`pytest-timeout` (`--timeout=30 --timeout-method=thread`, wired into CI and available locally) was
added alongside the fix as a permanent safety net, so a future hang of this *kind* fails fast with
a named test and traceback instead of silently consuming CI minutes. NullPool alone was not
sufficient: a later hang in `test_activity_refreshes_heartbeat`, which already used NullPool,
shows pooling was not the only cause (next entry).

**Found and fixed: websocket test hang from a cancel landing mid-query.** When a
`TestClient.websocket_connect(...)` block exits, the test client cancels the handler task. If that
cancel lands while a database statement is in flight, SQLAlchemy treats the `CancelledError` as a
connection failure and runs its aiosqlite cleanup path; the handler task was then observed never to
finish, and the run hung until the 30s timeout killed it. The exact point where it wedges was not
isolated. Fix: an autouse fixture in `tests/websocket/conftest.py` runs every `AsyncSession` block
inside a shielded `anyio` cancel scope, so a cancel is deferred until the session closes. No change
under `app/`. Evidence (local, Windows, Python 3.13, CI package versions): the full
`tests/websocket` suite hung in 8 of 20 runs without the fixture (this count was taken with the
regression test also running unshielded in the same suite) and in 0 of 20 with it; 10 of 10 CI
runs of the fix commit passed (the original push plus 9 re-runs).
`test_cancel_mid_insert_on_websocket_exit` fails without the fixture (the in-flight INSERT is
lost) and passes with it. Not established: behavior on PostgreSQL/asyncpg (16 clean runs of
`test_activity_refreshes_heartbeat` against PostgreSQL before this fix showed no hang, but a
cancel mid-query was not forced there), and whether real HTTP traffic through the
`@app.middleware("http")` layer can trigger the same cancel.

**Found and fixed: a lost receipt race rolled back the whole session.** When two writers raced to
create the same message receipt, the loser called `session.rollback()`, which expires every object
already loaded in that session even with `expire_on_commit=False`, causing `MissingGreenlet` on later
attribute access. The insert now runs inside a SAVEPOINT (`begin_nested()`), so only the insert is
undone. Regression test: `test_receipt_race_rollback_does_not_corrupt_other_loaded_objects`.

**Found and fixed during load testing: connection pool sizing.** Running the load test at
increasing concurrency (10 -> 50 -> 100 sender/receiver pairs) originally showed p50 latency
staying low while p99 grew sharply (25ms -> 706ms -> 1328ms) - a queueing signature. Root cause:
`app.db.session.create_engine` used SQLAlchemy's unconfigured pool defaults (5 + 10 = 15
connections per instance); each `message:send` opens two separate sessions, so 100 concurrent
senders on one instance were plausibly queueing for a pool slot. Confirmed, not just hypothesized:
`Settings.db_pool_size` / `db_max_overflow` were raised to 20 + 20 = 40 per instance (80 total
across both instances, comfortably under PostgreSQL's default `max_connections=100`), and
re-running the identical load test before/after isolated the effect - see the table below. p99 at
100 pairs dropped from 1327.64ms to 177.88ms (7.5x); the 10-pair baseline (already well under the
old limit) barely moved, confirming this targeted the actual bottleneck rather than being a
placebo.

## Load testing

`scripts/load_test.py` measures throughput and delivery latency under N concurrent WebSocket
connections split across both FastAPI instances - every sender is on `fastapi-1` and every
receiver is on `fastapi-2`, so **every measured message crosses the Redis Pub/Sub fanout** (the
harder cross-instance path, not the easy same-instance case). It runs in two phases with a hard
barrier between them: registration/conversation-setup/join first (not timed - Argon2id hashing is
deliberately expensive and would otherwise pollute the latency numbers with registration
contention), then the timed send/receive phase. Each sender paces its sends (`--send-interval`,
default 0.6s) to stay under the default message-send rate limit, with jittered start times so
100+ pairs don't fire in synchronized lockstep bursts.

**Results** (local Docker Desktop, 20 messages/sender, `--send-interval 0.6`, zero errors and
100% delivery at every level, before/after the connection-pool fix above):

| Pairs | Connections | | Throughput (msg/s) | p50 | p95 | p99 | max |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 10 | 20 | before | 16.05 | 14.95 ms | 20.91 ms | 24.84 ms | 32.17 ms |
| 10 | 20 | after | 16.28 | 15.67 ms | 24.29 ms | 29.99 ms | 31.53 ms |
| 50 | 100 | before | 61.66 | 58.54 ms | 485.09 ms | 705.51 ms | 755.53 ms |
| 50 | 100 | after | 78.19 | 20.51 ms | 78.57 ms | 99.61 ms | 109.96 ms |
| 100 | 200 | before | 82.15 | 783.95 ms | 1205.26 ms | 1327.64 ms | 1421.57 ms |
| 100 | 200 | after | 153.02 | 42.71 ms | 112.16 ms | 177.88 ms | 204.78 ms |

Throughput scales with concurrency in both configurations (the horizontal-scaling claim this
script exists to substantiate), but the pool-sizing fix changes the *shape* of the latency curve,
not just its scale: before, p50 stayed low while p95/p99 grew sharply from 50 pairs onward; after,
p50/p95/p99 all grow together much more gently, and the 100-pair p99 (177.88ms) is now *better*
than the 50-pair p99 was before (705.51ms). These are single local-machine runs, not averaged over
multiple trials, and absolute numbers will vary with host hardware - the relative shape and the
magnitude of the before/after gap are the reproducible, defensible claims here, not the exact
millisecond values.

Reproduce with:

```bash
docker compose up --build -d
.venv/Scripts/python scripts/load_test.py --pairs 100 --messages 20
```

## Trade-offs

Deliberate scope/design decisions, each with a documented reason above - collected here for a
quick scan:

| Trade-off | Chosen because | Cost accepted |
| --- | --- | --- |
| Redis Pub/Sub, not Streams, for cross-instance fanout | Simple to explain/operate; PostgreSQL already backstops correctness | A live push can be missed during a brief subscriber Redis outage (recovered via offline replay, not lost) |
| Fixed-window rate limiting, not sliding/token-bucket | One `INCR`+`EXPIRE` call, easy to reason about and test | Up to ~2x burst at a window boundary |
| Rate limiting fails open on Redis outage | Abuse protection isn't a security boundary; a Redis outage shouldn't take down login/messaging | Abuse protection is briefly unenforced (logged loudly) during an outage |
| Cross-instance DELIVERED marking deferred to next reconnect, not written live | The live-write version caused an intermittent shutdown hang | DELIVERED status for a cross-instance recipient lags briefly, not lost |
| No E2E encryption | Keeps scope on distributed-systems fundamentals rather than splitting effort into cryptography | Server can read message bodies (see [Future improvements](#future-improvements-documented-not-implemented)) |
| Soft-delete (row + `sequence_number` kept, `body` cleared), not a hard row delete | Keeps replay ordering gap-free without a tombstone/placeholder row of a different shape | A deleted row stays in `messages` forever, just with an empty `body` |
| No edit history, no "delete for me" vs. "for everyone", no edit/delete time window | Keeps the feature to the core mutate-in-place mechanism | No audit trail of prior edits; deletion is always for every member; a sender can edit/delete arbitrarily long after sending |
| No microservices/Kafka/Kubernetes | Two FastAPI processes + Nginx already prove horizontal scaling and cross-instance coordination | Wouldn't hold up to a much larger deployment as-is - see [Limitations](#limitations) |

## Limitations

- **In-memory `ConnectionManager` state is per-process.** A given WebSocket connection's room
  membership lives only in the instance it's connected to; it does not survive that instance
  restarting (the client reconnects and replays instead - see
  [Offline recovery](#offline-recovery--reconnection) - but there's no "hot" handoff between
  instances for a single live connection).
- **Single Redis / single PostgreSQL, no HA.** Both are single-instance in `docker-compose.yml`.
  Redis being unreachable degrades gracefully (see [Testing](#testing--failure-scenarios));
  PostgreSQL being unreachable does not - it's the source of truth, and there's no failover
  target to degrade to. Neither is a production topology; this project's scope is the application
  layer, not database HA.
- **No edit history and no "delete for me only."** Editing overwrites `body` in place with no
  version trail, and deletion is unconditionally for every member - see
  [Message editing & deletion](#message-editing--deletion) and
  [Future improvements](#future-improvements-documented-not-implemented).
- **Fixed-window rate limiting** allows up to ~2x the configured burst at a window boundary - see
  [Trade-offs](#trade-offs).
- **Known WebSocket test flake** (roughly 1 in 20 runs, ordering only, never a correctness issue)
  - see [Testing & failure scenarios](#testing--failure-scenarios).
- **E2E encryption is not implemented** - the server can read message bodies. See
  [Future improvements](#future-improvements-documented-not-implemented).

## Future improvements (documented, not implemented)

- **End-to-end encryption** for 1:1 messaging - client-side key generation, server sees ciphertext
  only, using established crypto libraries rather than hand-rolled primitives. Cut to keep scope
  focused on the distributed-systems fundamentals this project demonstrates (ordering,
  idempotency, presence, horizontal scaling) rather than splitting effort into cryptography.
- **Redis Streams with consumer groups** in place of Pub/Sub for cross-instance fanout, if
  guaranteed at-least-once live delivery ever becomes a hard requirement (see
  [Horizontal scaling](#horizontal-scaling--redis-pubsub)).
- **Sliding-window or token-bucket rate limiting**, if the fixed-window boundary-burst trade-off
  ever stops being acceptable.
- **Message edit history / version tracking** - only the current `body` is kept today; no record
  of what an edited message used to say.
- **Per-recipient "delete for me" only**, as a concept distinct from the current unconditional
  "delete for everyone" - see [Message editing & deletion](#message-editing--deletion).
- **A configurable edit/delete time window**, if letting a sender mutate a message arbitrarily long
  after sending ever stops being acceptable.
