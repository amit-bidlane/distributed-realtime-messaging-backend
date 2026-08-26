"""Process-global metrics, for this project's observability setup.

Registered once at import time in the default prometheus_client registry -
deliberately not per-app-instance state, since a real deployment scrapes
each FastAPI process's own /metrics endpoint independently (see
app.api.metrics) and there is exactly one process per instance.
"""

from prometheus_client import Counter, Gauge, Histogram

active_connections = Gauge(
    "active_connections", "WebSocket connections currently held open by this instance"
)
messages_sent = Counter(
    "messages_sent_total", "message:send events successfully persisted and broadcast"
)
messages_failed = Counter(
    "messages_failed_total", "message:send events that failed to persist"
)
message_send_latency_seconds = Histogram(
    "message_send_latency_seconds",
    "Time from receiving message:send to sending message:ack",
)
redis_errors = Counter(
    "redis_errors_total",
    "Redis operations that raised instead of completing, by call site",
    ["scope"],
)
database_errors = Counter(
    "database_errors_total",
    "Database operations that raised instead of completing, by call site",
    ["scope"],
)
