from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from src.config.models import EndpointConfig, NodeRegistryEntry, _mask_endpoint
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
    MethodCallRequest,
    ReadRequest,
    WriteRequest,
)
from src.domain.services.raw_capture import RawCaptureAlreadyActiveError, RawCaptureFileExistsError
from src.runtime import AppRuntime

bearer_scheme = HTTPBearer(auto_error=False)


class NodesConfigUpdate(BaseModel):
    nodes: list[NodeRegistryEntry]


class NodesEnabledUpdate(BaseModel):
    node_ids: list[str]
    enabled: bool


class StatusOverloadCounterUpdate(BaseModel):
    enabled: bool


class RawCaptureStartRequest(BaseModel):
    duration_seconds: float = Field(default=2.0, gt=0, le=60)
    output: str | None = None
    endpoint_id: str | None = None
    max_records: int = Field(default=100000, ge=1, le=1_000_000)


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

    @router.patch("/config/nodes/enabled")
    async def update_config_nodes_enabled(
        payload: NodesEnabledUpdate,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        known_ids = {node.id for node in runtime.registry.all()}
        unknown_ids = sorted(set(payload.node_ids) - known_ids)
        if unknown_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": "Unknown node config ids.", "ids": unknown_ids},
            )
        try:
            results = await runtime.update_nodes_enabled(payload.node_ids, payload.enabled)
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

    @router.get("/publish/audit")
    async def publish_audit(
        limit: int = 500,
        endpoint_id: str | None = None,
        node_id: str | None = None,
        decision: str | None = None,
        status_code: int | None = None,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return await runtime.diagnostics.publish_audit(
            limit=limit,
            endpoint_id=endpoint_id,
            node_id=node_id,
            decision=decision,
            status_code=status_code,
        )

    @router.get("/publish/stats")
    async def publish_stats(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        return await runtime.diagnostics.publish_stats()

    @router.get("/status-overload-counter")
    async def status_overload_counter(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        payload = await runtime.diagnostics.status_overload_counter()
        statuses = runtime.registry.statuses()
        payload["enabled_nodes"] = sum(1 for item in statuses if item.enabled)
        payload["active_nodes"] = sum(1 for item in statuses if item.enabled and item.active)
        payload["total_nodes"] = len(statuses)
        return payload

    @router.put("/status-overload-counter")
    async def update_status_overload_counter(
        payload: StatusOverloadCounterUpdate,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        result = await runtime.diagnostics.set_status_overload_counter_enabled(payload.enabled)
        statuses = runtime.registry.statuses()
        result["enabled_nodes"] = sum(1 for item in statuses if item.enabled)
        result["active_nodes"] = sum(1 for item in statuses if item.enabled and item.active)
        result["total_nodes"] = len(statuses)
        return result

    @router.get("/status-alarms")
    async def status_alarms(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return await runtime.diagnostics.status_alarms()

    @router.get("/status-alarms/history")
    async def status_alarm_history(
        limit: int = 200,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return await runtime.diagnostics.status_alarm_history(limit=limit)

    @router.post("/debug/raw-capture/start")
    async def start_raw_capture(
        payload: RawCaptureStartRequest,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        if payload.endpoint_id is not None and payload.endpoint_id not in {item.id for item in runtime.config.endpoints}:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Endpoint {payload.endpoint_id} not found.")
        try:
            return await runtime.raw_capture.start_capture(
                duration_seconds=payload.duration_seconds,
                output_path=payload.output,
                endpoint_id=payload.endpoint_id,
                max_records=payload.max_records,
            )
        except RawCaptureAlreadyActiveError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except RawCaptureFileExistsError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @router.get("/connection-events")
    async def connection_events(
        limit: int = 200,
        endpoint_id: str | None = None,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return await runtime.connections.connection_events(limit=limit, endpoint_id=endpoint_id)

    @router.get("/events")
    async def events(
        endpoint_id: str | None = None,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        try:
            return runtime.connections.event_notifications(endpoint_id)
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @router.get("/alarms")
    async def alarms(
        endpoint_id: str | None = None,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        try:
            return runtime.connections.alarm_notifications(endpoint_id)
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @router.get("/capabilities")
    async def capabilities(
        endpoint_id: str | None = None,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        try:
            return runtime.connections.capabilities(endpoint_id)
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

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

    @router.post("/methods/call")
    async def call_method(
        payload: MethodCallRequest,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        try:
            result = await runtime.connections.call_method(
                payload.endpoint_id,
                payload.object_node_id,
                payload.method_node_id,
                payload.input_arguments,
            )
            return result.model_dump(mode="json")
        except EndpointNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except NodeWriteError as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    @router.get("/config/endpoints")
    async def config_endpoints(
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> list[dict[str, object]]:
        return runtime.connections.endpoints_masked()

    @router.post("/config/endpoints", status_code=status.HTTP_201_CREATED)
    async def create_endpoint(
        payload: EndpointConfig,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        existing_ids = {e.id for e in runtime.connections.endpoints_config()}
        if payload.id in existing_ids:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Endpoint {payload.id} уже существует.",
            )
        new_endpoints = [*runtime.connections.endpoints_config(), payload]
        await runtime.replace_endpoints_config(new_endpoints)
        return _mask_endpoint(payload)

    @router.put("/config/endpoints/{endpoint_id}")
    async def update_endpoint(
        endpoint_id: str,
        payload: EndpointConfig,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> dict[str, object]:
        existing = next((e for e in runtime.connections.endpoints_config() if e.id == endpoint_id), None)
        if existing is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Endpoint {endpoint_id} не найден.")
        # Preserve existing password when client sends null
        if payload.auth.password is None and existing.auth.password is not None:
            updated_auth = payload.auth.model_copy(update={"password": existing.auth.password})
            payload = payload.model_copy(update={"id": endpoint_id, "auth": updated_auth})
        else:
            payload = payload.model_copy(update={"id": endpoint_id})
        updated = [payload if e.id == endpoint_id else e for e in runtime.connections.endpoints_config()]
        await runtime.replace_endpoints_config(updated)
        return _mask_endpoint(payload)

    @router.delete("/config/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_endpoint(
        endpoint_id: str,
        runtime: AppRuntime = Depends(get_runtime),
        _: None = Depends(authorize_request),
    ) -> None:
        existing_ids = {e.id for e in runtime.connections.endpoints_config()}
        if endpoint_id not in existing_ids:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Endpoint {endpoint_id} не найден.")
        remaining = [e for e in runtime.connections.endpoints_config() if e.id != endpoint_id]
        await runtime.replace_endpoints_config(remaining)

    return router


def _duplicate_node_ids(nodes: list[NodeRegistryEntry]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for node in nodes:
        if node.id in seen:
            duplicates.add(node.id)
        seen.add(node.id)
    return sorted(duplicates)
