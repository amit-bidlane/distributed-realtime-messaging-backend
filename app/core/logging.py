import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime

# Correlates every log line emitted while handling one HTTP request or one
# WebSocket connection, without threading a request_id parameter through
# every function signature in between. See bind_request_id and
# RequestIdFilter.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


@contextmanager
def bind_request_id(request_id: str) -> Iterator[None]:
    """Scope `request_id_var` to one HTTP request or WebSocket connection.

    app.core.middleware binds one per HTTP request; app.websocket.router
    binds one (the connection's own connection_id) for the lifetime of a
    WebSocket connection, so e.g. that connection's
    "presence_heartbeat_failed" and "message_persist_failed" lines can be
    correlated back to the same connection in the structured log output.
    """
    token = request_id_var.set(request_id)
    try:
        yield
    finally:
        request_id_var.reset(token)


class RequestIdFilter(logging.Filter):
    """Attaches the active request_id (if any) to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


class JsonFormatter(logging.Formatter):
    """Minimal structured formatter for service logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = getattr(record, "request_id", None)
        if request_id is not None:
            payload["request_id"] = request_id
        if record.exc_info:
            exc_type = record.exc_info[0]
            # error_type gives dashboards/alerts a stable field to group and
            # count errors by (the same grouping the redis_errors/database_errors
            # metrics use) without parsing the traceback text itself.
            payload["error_type"] = exc_type.__name__ if exc_type is not None else "Unknown"
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
