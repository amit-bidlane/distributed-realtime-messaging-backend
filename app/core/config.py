from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    app_env: str = "development"
    app_name: str = "Scalable Messaging Platform"
    log_level: str = "INFO"
    database_url: str = "postgresql+asyncpg://messaging:messaging@localhost:5432/messaging"
    # Defaults raised from SQLAlchemy's own defaults (5 + 10 = 15) after
    # scripts/load_test.py showed p99 delivery latency growing sharply (not
    # linearly) from 50 concurrent sender/receiver pairs onward - a queueing
    # signature consistent with this instance's connection pool becoming a
    # shared bottleneck (see README's Load testing section). 20 + 20 = 40 per
    # instance, 80 total across fastapi-1/fastapi-2, comfortably under
    # PostgreSQL's own default max_connections=100 (docker-compose.yml's
    # postgres service does not override it) with headroom for the one-off
    # migrate service and any direct/admin connections.
    db_pool_size: int = Field(default=20, gt=0)
    db_max_overflow: int = Field(default=20, ge=0)
    redis_url: str = "redis://localhost:6379/0"
    jwt_secret: SecretStr = SecretStr("development-only-jwt-secret-change-before-production")
    refresh_token_pepper: SecretStr = SecretStr(
        "development-only-refresh-pepper-change-before-production"
    )
    access_token_ttl_minutes: int = Field(default=15, gt=0)
    refresh_token_ttl_days: int = Field(default=30, gt=0)
    presence_heartbeat_ttl_seconds: int = Field(default=30, gt=0)
    typing_indicator_ttl_seconds: int = Field(default=6, gt=0)
    typing_rate_limit_seconds: int = Field(default=3, gt=0)
    message_replay_batch_limit: int = Field(default=200, gt=0)
    rate_limit_login_max_attempts: int = Field(default=5, gt=0)
    rate_limit_login_window_seconds: int = Field(default=60, gt=0)
    rate_limit_register_max_attempts: int = Field(default=10, gt=0)
    rate_limit_register_window_seconds: int = Field(default=3600, gt=0)
    rate_limit_message_send_max_events: int = Field(default=20, gt=0)
    rate_limit_message_send_window_seconds: int = Field(default=10, gt=0)
    rate_limit_typing_max_events: int = Field(default=20, gt=0)
    rate_limit_typing_window_seconds: int = Field(default=10, gt=0)
    rate_limit_contacts_max_requests: int = Field(default=10, gt=0)
    rate_limit_contacts_window_seconds: int = Field(default=60, gt=0)
    # Shared by conversation:join and message:read (the same "cheap, related
    # events, one budget" precedent as typing:start/typing:stop above) - both
    # hit PostgreSQL on every event (a membership JOIN plus, for join, a
    # potential replay query) and were previously unrated, unlike every other
    # DB-touching WebSocket event. See README's Rate limiting section.
    rate_limit_conversation_action_max_events: int = Field(default=20, gt=0)
    rate_limit_conversation_action_window_seconds: int = Field(default=10, gt=0)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @model_validator(mode="after")
    def reject_development_secrets_in_production(self) -> "Settings":
        if self.app_env.lower() == "production" and (
            self.jwt_secret.get_secret_value().startswith("development-only")
            or self.refresh_token_pepper.get_secret_value().startswith("development-only")
        ):
            raise ValueError("production JWT secrets must be configured")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
