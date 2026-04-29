from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from src.config.models import AppConfigModel


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], path: Path | None) -> None:
        super().__init__(settings_cls)
        self.path = path

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        data = self._read_data()
        return data.get(field_name), field_name, False

    def prepare_field_value(self, field_name: str, field: Any, value: Any, value_is_complex: bool) -> Any:
        return value

    def __call__(self) -> dict[str, Any]:
        return self._read_data()

    def _read_data(self) -> dict[str, Any]:
        if self.path is None or not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            raise ValueError("Конфигурационный YAML должен быть объектом верхнего уровня.")
        return loaded


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OPC_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    service: dict[str, Any] = {}
    api: dict[str, Any] = {}
    logging: dict[str, Any] = {}
    buffer: dict[str, Any] = {}
    publisher: dict[str, Any] = {}
    endpoints: list[dict[str, Any]] = []
    nodes: list[dict[str, Any]] = []

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        config_path = os.getenv("OPC_CONFIG_FILE")
        yaml_source = YamlConfigSettingsSource(settings_cls, Path(config_path) if config_path else None)
        return (init_settings, yaml_source, env_settings, dotenv_settings, file_secret_settings)

    def to_model(self) -> AppConfigModel:
        return AppConfigModel.model_validate(self.model_dump())


def load_settings() -> AppConfigModel:
    return AppSettings().to_model()


def get_config_path() -> Path | None:
    config_path = os.getenv("OPC_CONFIG_FILE")
    return Path(config_path) if config_path else None
