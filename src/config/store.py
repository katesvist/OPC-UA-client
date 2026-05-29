from __future__ import annotations

import os
from pathlib import Path

import yaml

from src.config.models import EndpointConfig, NodeRegistryEntry


class YamlConfigStore:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def _write(self, data: dict) -> None:
        assert self.path is not None
        content = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        try:
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.replace(self.path)
        except OSError:
            # Docker Desktop on macOS forbids rename across bind-mounted files.
            # Fall back to direct overwrite (non-atomic but functional for dev).
            tmp_path.unlink(missing_ok=True)
            self.path.write_text(content, encoding="utf-8")

    @property
    def writable(self) -> bool:
        if self.path is None:
            return False
        if self.path.exists():
            return os.access(self.path, os.W_OK)
        return os.access(self.path.parent, os.W_OK)

    def save_nodes(self, nodes: list[NodeRegistryEntry]) -> None:
        if self.path is None:
            raise RuntimeError("OPC_CONFIG_FILE is not configured; runtime changes cannot be persisted.")

        data: dict = {}
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
                if not isinstance(loaded, dict):
                    raise RuntimeError("Configuration YAML must be an object.")
                data = loaded

        data["nodes"] = [
            node.model_dump(mode="json", exclude_none=True)
            for node in sorted(nodes, key=lambda item: (item.endpoint_id, item.id))
        ]

        self._write(data)

    def save_endpoints(self, endpoints: list[EndpointConfig]) -> None:
        if self.path is None:
            raise RuntimeError("OPC_CONFIG_FILE is not configured; runtime changes cannot be persisted.")

        data: dict = {}
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
                if not isinstance(loaded, dict):
                    raise RuntimeError("Configuration YAML must be an object.")
                data = loaded

        serialized = []
        for endpoint in endpoints:
            raw = endpoint.model_dump(mode="json", exclude_none=True)
            # SecretStr serializes as '**********' — restore the real value
            if endpoint.auth.password is not None:
                raw.setdefault("auth", {})["password"] = endpoint.auth.password.get_secret_value()
            serialized.append(raw)
        data["endpoints"] = serialized

        self._write(data)
