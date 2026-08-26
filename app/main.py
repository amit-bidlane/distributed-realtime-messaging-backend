import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.health import InfrastructureReadiness
from app.api.health import router as health_router
from app.api.metrics import router as metrics_router
from app.auth.router import router as auth_router
from app.contacts.router import router as contacts_router
from app.conversations.router import router as conversations_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.middleware import install_request_id_middleware
from app.db.session import create_engine, create_session_factory
from app.redis.client import create_redis_client
from app.redis.pubsub import EventFanout
from app.websocket.manager import ConnectionManager
from app.websocket.router import handle_remote_event
from app.websocket.router import router as websocket_router

# Upper bound on graceful shutdown of the fanout listener (see
# EventFanout.stop). Comfortably longer than EventFanout's own poll
# interval, so this should only ever be reached if Redis itself is
# unresponsive - the fallback below still guarantees shutdown completes.
FANOUT_SHUTDOWN_TIMEOUT_SECONDS = 5


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(app.state.settings.log_level)
    logging.getLogger(__name__).info("application_started")

    fanout: EventFanout = app.state.fanout

    async def _dispatch(conversation_id: uuid.UUID, payload: dict[str, Any]) -> None:
        await handle_remote_event(app.state.connection_manager, conversation_id, payload)

    # Not awaited: app startup does not block on Redis being reachable, the
    # same way /health never has and /ready already signals Redis
    # availability per-request rather than at process boot (see
    # app.api.health.InfrastructureReadiness). Blocking startup here would
    # make every app instance - including ones that only need PostgreSQL,
    # like most of this test suite's fixtures - hard-fail to construct
    # whenever Redis isn't reachable. A caller that specifically needs to
    # know the subscription is active before proceeding (e.g. a test
    # proving cross-instance delivery) can await app.state.fanout.ready
    # itself - see tests/websocket/test_multi_instance.py.
    listener_task = asyncio.create_task(fanout.listen(_dispatch))

    yield

    # Uvicorn routes SIGTERM (and SIGINT) into this shutdown phase, so graceful
    # WebSocket shutdown lives here rather than in a competing signal handler:
    # stop accepting new connections, tell connected clients, then close them.
    #
    # fanout.stop() + a bounded wait (not listener_task.cancel()) is
    # deliberate: see the long comment on EventFanout.listen for the
    # cancellation-safety reasoning behind polling instead of blocking.
    # cancel() is kept only as a last-resort fallback in case Redis itself is
    # unresponsive and listen() can't even get back around to checking the
    # stop flag.
    fanout.stop()
    try:
        await asyncio.wait_for(listener_task, timeout=FANOUT_SHUTDOWN_TIMEOUT_SECONDS)
    except TimeoutError:
        logging.getLogger(__name__).warning("event_fanout_listener_shutdown_timed_out")
        listener_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener_task
    await fanout.wait_idle()
    closed = await app.state.connection_manager.begin_shutdown()
    logging.getLogger(__name__).info("websocket_shutdown_closed count=%d", len(closed))
    await app.state.redis.aclose()
    await app.state.db_engine.dispose()
    logging.getLogger(__name__).info("application_stopped")


def create_app(settings: Settings | None = None, redis_client: Redis | None = None) -> FastAPI:
    runtime_settings = settings or get_settings()
    db_engine: AsyncEngine = create_engine(runtime_settings)
    session_factory: async_sessionmaker[AsyncSession] = create_session_factory(db_engine)
    redis_client = redis_client or create_redis_client(runtime_settings)

    app = FastAPI(title=runtime_settings.app_name, lifespan=lifespan)
    app.state.settings = runtime_settings
    app.state.db_engine = db_engine
    app.state.session_factory = session_factory
    app.state.redis = redis_client
    app.state.readiness = InfrastructureReadiness(session_factory, redis_client)
    app.state.connection_manager = ConnectionManager()
    app.state.fanout = EventFanout(redis_client)
    install_request_id_middleware(app)
    app.include_router(health_router)
    app.include_router(metrics_router)
    app.include_router(auth_router)
    app.include_router(conversations_router)
    app.include_router(contacts_router)
    app.include_router(websocket_router)
    return app


app = create_app()
