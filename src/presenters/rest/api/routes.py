from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from src.config.models import NodeRegistryEntry
from src.domain.entities.errors import (
    BrowseError,
    ConnectionError,
    EndpointNotFoundError,
    NodeNotFoundError,
    NodeReadError,
    NodeWriteError,
    WriteNotAllowedError,
)
from src.domain.entities.models import (
    BrowseRequest,
    ReadRequest,
    WriteRequest,
)
from src.runtime import AppRuntime

bearer_scheme = HTTPBearer(auto_error=False)


class NodesConfigUpdate(BaseModel):
    nodes: list[NodeRegistryEntry]


async def get_runtime(request: Request) -> AppRuntime:
    return cast(AppRuntime, request.app.state.runtime)


async def authorize_request(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    runtime = cast(AppRuntime, request.app.state.runtime)
    token = runtime.config.api.management_token
    if token is None:
        return
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Недостаточно прав.")
    if credentials.credentials != token.get_secret_value():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Недостаточно прав.")


def build_router() -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health(_: None = Depends(authorize_request)) -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    async def ready(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> Response:
        ready_flag, payload = runtime.health.readiness(runtime.connections.statuses(), runtime.buffer_ready)
        return JSONResponse(
            content=payload,
            status_code=status.HTTP_200_OK if ready_flag else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    @router.get("/metrics")
    async def metrics(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> Response:
        return Response(content=runtime.metrics.render(), media_type="text/plain; version=0.0.4")

    @router.get("/connections")
    async def connections(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return [status_item.model_dump(mode="json") for status_item in runtime.connections.statuses()]

    @router.get("/subscriptions")
    async def subscriptions(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return [status_item.model_dump(mode="json") for status_item in runtime.registry.statuses()]

    @router.get("/config/nodes")
    async def config_nodes(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return [node.model_dump(mode="json") for node in runtime.registry.all()]

    @router.put("/config/nodes")
    async def replace_config_nodes(
        payload: NodesConfigUpdate,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        duplicate_ids = _duplicate_node_ids(payload.nodes)
        if duplicate_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": "Duplicate node config ids.", "ids": duplicate_ids},
            )
        try:
            results = await runtime.replace_nodes_config(payload.nodes)
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return {
            "nodes": [node.model_dump(mode="json") for node in runtime.registry.all()],
            "results": [item.model_dump(mode="json") for item in results],
        }

    @router.get("/buffer/stats")
    async def buffer_stats(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, int]:
        return await runtime.buffer.stats()

    @router.get("/dead-letter")
    async def dead_letter(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return [item.model_dump(mode="json") for item in await runtime.buffer.dead_letters()]

    @router.post("/connections/{endpoint_id}/reconnect", status_code=status.HTTP_202_ACCEPTED)
    async def reconnect(
        endpoint_id: str,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, str]:
        await runtime.connections.reconnect(endpoint_id)
        return {"status": "scheduled", "endpoint_id": endpoint_id}

    @router.post("/browse")
    async def browse(
        payload: BrowseRequest,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        try:
            results = await runtime.connections.browse(
                endpoint_id=payload.endpoint_id,
                node_id=payload.node_id,
                max_depth=payload.max_depth,
                include_variables=payload.include_variables,
                include_objects=payload.include_objects,
            )
            return [item.model_dump(mode="json") for item in results]
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except (BrowseError, NodeNotFoundError) as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    @router.post("/read")
    async def read(
        payload: ReadRequest,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        try:
            result = await runtime.connections.read(payload.endpoint_id, payload.node_id)
            return result.model_dump(mode="json")
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except NodeNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except NodeReadError as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    @router.post("/write")
    async def write(
        payload: WriteRequest,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        try:
            result = await runtime.connections.write(payload.endpoint_id, payload.node_id, payload.value)
            return result.model_dump(mode="json")
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except WriteNotAllowedError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        except NodeWriteError as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    return router


def _duplicate_node_ids(nodes: list[NodeRegistryEntry]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for node in nodes:
        if node.id in seen:
            duplicates.add(node.id)
        seen.add(node.id)
    return sorted(duplicates)
