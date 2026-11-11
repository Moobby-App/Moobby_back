"""Request-local, allowlisted diagnostics. No handlers, exporters or HTTP changes."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
import inspect
import json
import logging
from threading import Lock
from time import monotonic
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import ResponseValidationError
from pydantic import ValidationError
import psycopg

from mooby.api.config import ApiConfigurationError


logger = logging.getLogger(__name__)
_EVENTS = frozenset({"request_admitted", "request_stage", "request_completed",
                     "request_failed", "request_cancelled", "request_aborted"})
_ROUTES = frozenset({"/health/live", "/health/ready", "/api/v1/disruptions",
                     "/api/v1/mock/disruptions"})
_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"})
_STAGES = frozenset({"authentication", "endpoint_execution", "feed_read",
                     "database_connect", "database_session", "database_timestamp", "database_feed",
                     "database_readiness", "projection_readiness", "row_mapping",
                     "response_validation", "response_send", "fixture_read",
                     "parameter_validation", "application"})
_CATEGORIES = frozenset({"results", "no_match", "source_unavailable", "available",
                         "authentication_failure", "parameter_validation_failure",
                         "http_error", "unexpected_failure", "cancelled", "aborted"})


@dataclass
class _RequestState:
    scope: dict = field(repr=False)
    request_id: str = field(default_factory=lambda: uuid4().hex)
    started: float = field(default_factory=monotonic)
    status: int | None = None
    category: str | None = None
    failure_stage: str | None = None
    exception_category: str | None = None
    response_started: bool = False
    response_finished: bool = False
    disconnect_observed: bool = False
    # A mutable per-request object crosses copied thread contexts. Never rely on
    # ContextVar.set() inside a sync endpoint propagating back to its ASGI caller.
    lock: Lock = field(default_factory=Lock, repr=False)


_current: ContextVar[_RequestState | None] = ContextVar("mooby_request", default=None)


def current_request_id() -> str | None:
    state = _current.get()
    return state.request_id if state else None


def _emit(state: _RequestState, event: str, *, stage=None, duration_ms=None):
    """Only locally constructed primitives reach logging; handler errors are isolated."""
    try:
        if event not in _EVENTS:
            return
        route = getattr(state.scope.get("route"), "path", None)
        method = state.scope.get("method")
        with state.lock:
            record = {
                "event": event,
                "request_id": state.request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "route": route if route in _ROUTES else None,
                "method": method if method in _METHODS else "OTHER",
                "status": state.status,
                "category": state.category,
                "duration_ms": max(0.0, (monotonic() - state.started) * 1000),
                "failure_stage": state.failure_stage,
                "exception_category": state.exception_category,
                "response_started": state.response_started,
                "disconnect_observed": state.disconnect_observed,
            }
        if stage in _STAGES:
            record["stage"] = stage
            record["stage_duration_ms"] = max(0.0, duration_ms)
        logger.info(json.dumps(record, separators=(",", ":")))
    except Exception:
        # Do not recursively log an exporter/formatter/handler failure.
        pass


def set_category(category: str):
    state = _current.get()
    if state is not None and category in _CATEGORIES:
        with state.lock:
            state.category = category


def _exception_category(exc: BaseException, stage: str) -> str:
    if isinstance(exc, asyncio.CancelledError):
        return "cancellation"
    if isinstance(exc, HTTPException):
        return "http_error"
    if isinstance(exc, (ValidationError, ResponseValidationError)):
        return "validation_error"
    if isinstance(exc, ApiConfigurationError):
        return "configuration_error"
    if isinstance(exc, (psycopg.Error, OSError)):
        if stage == "database_connect":
            return "connection_error"
        if stage.startswith("database_") or stage == "projection_readiness":
            return "database_error"
        if stage == "response_send":
            return "transport_error"
        return "dependency_error"
    if isinstance(exc, (KeyError, TypeError, ValueError)) and stage in {
        "row_mapping", "response_validation"
    }:
        return "mapping_error"
    return "unexpected_error"


def record_failure(stage: str, exc: BaseException | None = None):
    """Keep the innermost failure even if existing code catches/converts it."""
    state = _current.get()
    if isinstance(exc, ResponseValidationError):
        stage = "response_validation"
    if isinstance(exc, HTTPException) and exc.status_code in {401, 422}:
        stage = "authentication" if exc.status_code == 401 else "parameter_validation"
    if state is not None:
        with state.lock:
            if state.failure_stage is None:
                state.failure_stage = stage if stage in _STAGES else "application"
                state.exception_category = (
                    _exception_category(exc, state.failure_stage) if exc is not None else None
                )


@contextmanager
def timed_stage(stage: str):
    state = _current.get()
    if state is None:
        yield
        return
    stage = stage if stage in _STAGES else "application"
    started = monotonic()
    try:
        yield
    except BaseException as exc:
        record_failure(stage, exc)
        raise
    finally:
        _emit(state, "request_stage", stage=stage,
              duration_ms=(monotonic() - started) * 1000)


def observe_sync(stage: str):
    """Preserve FastAPI's resolved signature and synchronous dispatch behavior."""
    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            with timed_stage(stage):
                return function(*args, **kwargs)
        observed.__signature__ = inspect.signature(function, eval_str=True)
        return observed
    return decorate


class RequestObservabilityMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        state = _RequestState(scope)
        token = _current.set(state)
        _emit(state, "request_admitted")
        terminal = "request_aborted"

        async def observed_receive():
            # Only observe messages the application itself requests; never poll
            # or consume the channel in a second task.
            message = await receive()
            if message["type"] == "http.disconnect":
                state.disconnect_observed = True
            return message

        async def observed_send(message):
            if message["type"] == "http.response.start":
                state.status = message["status"]
                state.response_started = True
            with timed_stage("response_send"):
                await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                state.response_finished = True

        try:
            await self.app(scope, observed_receive, observed_send)
            if state.disconnect_observed or not state.response_finished:
                terminal = "request_aborted"
                set_category("aborted")
            else:
                terminal = "request_failed" if (state.status or 0) >= 400 else "request_completed"
        except asyncio.CancelledError as exc:
            record_failure("application", exc)
            set_category("cancelled")
            terminal = "request_cancelled"
            raise
        except BaseException as exc:
            record_failure("application", exc)
            terminal = ("request_aborted" if state.failure_stage == "response_send"
                        or not isinstance(exc, Exception) else "request_failed")
            set_category("aborted" if terminal == "request_aborted" else "unexpected_failure")
            raise
        finally:
            try:
                if state.category is None:
                    if state.status == 401:
                        set_category("authentication_failure")
                        record_failure("authentication")
                    elif state.status == 422:
                        set_category("parameter_validation_failure")
                        record_failure("parameter_validation")
                    elif (state.status or 0) >= 500:
                        set_category("source_unavailable")
                    elif (state.status or 0) >= 400:
                        set_category("http_error")
                    else:
                        set_category("available")
                _emit(state, terminal)
            finally:
                _current.reset(token)


class ObservedFastAPI(FastAPI):
    def build_middleware_stack(self):
        # Outside Starlette's existing error handler: observe its real 500 send,
        # then preserve the original exception re-raise and public response.
        return RequestObservabilityMiddleware(super().build_middleware_stack())
