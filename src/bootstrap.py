from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import FastAPI, Request, Response

from src.presenters.rest.api.routes import build_router
from src.presenters.rest.runtime import lifespan


def create_app() -> FastAPI:
    app = FastAPI(
        title="OPC UA Client Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def bind_request_id(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        return response

    app.include_router(build_router())
    return app
