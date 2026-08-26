import logging
import time
import uuid
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response

from app.core.logging import bind_request_id

logger = logging.getLogger(__name__)

RequestResponseEndpoint = Callable[[Request], Awaitable[Response]]


def install_request_id_middleware(app: FastAPI) -> None:
    """Tag every HTTP request with a correlation id and log its outcome.

    The id is echoed back as `X-Request-ID` (a client-supplied one is kept
    rather than replaced, so a caller that already generates its own
    correlation id keeps using it end to end) and bound to
    app.core.logging.request_id_var for the duration of the request, so
    every log line emitted while handling it - including ones several call
    frames deep, like a rate-limit or database failure - is automatically
    tagged without threading the id through every function signature.
    """

    @app.middleware("http")
    async def request_id_middleware(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        started = time.monotonic()
        with bind_request_id(request_id):
            try:
                response = await call_next(request)
            except Exception:
                # Reaching here means no route/exception handler dealt with
                # it - FastAPI/Starlette's ExceptionMiddleware already turns
                # HTTPException (and anything with a registered handler)
                # into a Response before call_next returns, so this branch
                # is always an unexpected failure, worth its own loud log
                # with a full traceback before Starlette's default handler
                # turns it into a bare 500 response.
                logger.exception(
                    "http_request_failed method=%s path=%s", request.method, request.url.path
                )
                raise
            duration_ms = (time.monotonic() - started) * 1000
            logger.info(
                "http_request_completed method=%s path=%s status=%d duration_ms=%.1f",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
            )
            response.headers["X-Request-ID"] = request_id
            return response
