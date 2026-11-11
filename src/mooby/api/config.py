"""Fail-closed configuration for the app-facing gateway."""

from __future__ import annotations

from dataclasses import dataclass
import os
from collections.abc import Mapping


class ApiConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class ApiSettings:
    database_url: str
    bearer_token: str
    connect_timeout_seconds: int = 3
    statement_timeout_ms: int = 3000

    @classmethod
    def from_env(cls) -> "ApiSettings":
        return cls.from_mapping(os.environ)

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "ApiSettings":
        """Validate a child environment without mutating the supervisor's environment."""
        database_url = values.get("APP_API_DATABASE_URL", "").strip()
        bearer_token = values.get("APP_API_BEARER_TOKEN", "").strip()
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ApiConfigurationError("APP_API_DATABASE_URL is missing or invalid")
        if len(bearer_token) < 32:
            raise ApiConfigurationError("APP_API_BEARER_TOKEN is missing or too short")
        try:
            connect_timeout = int(values.get("APP_API_CONNECT_TIMEOUT_SECONDS", "3"))
            statement_timeout = int(values.get("APP_API_STATEMENT_TIMEOUT_MS", "3000"))
        except (TypeError, ValueError) as exc:
            raise ApiConfigurationError("API timeout configuration is invalid") from exc
        if not 1 <= connect_timeout <= 15:
            raise ApiConfigurationError("APP_API_CONNECT_TIMEOUT_SECONDS is out of range")
        if not 100 <= statement_timeout <= 10000:
            raise ApiConfigurationError("APP_API_STATEMENT_TIMEOUT_MS is out of range")
        return cls(database_url, bearer_token, connect_timeout, statement_timeout)
