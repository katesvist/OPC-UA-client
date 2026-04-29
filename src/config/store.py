from __future__ import annotations

import os
from pathlib import Path

import yaml

from src.config.models import NodeRegistryEntry


class YamlConfigStore:
    def __init__(self, path: Path | None) -> None:
        self.path = path

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

        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)
        tmp_path.replace(self.path)
