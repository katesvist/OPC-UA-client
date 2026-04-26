from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from src.config.models import NodeConfig
from src.domain.entities.errors import DatatypeMappingError


class ValueNormalizer:
    def normalize(self, raw_value: Any, node: NodeConfig) -> tuple[Any, str, str | None]:
        expected_type = node.expected_type

        if node.value_shape == "array":
            if not self._is_sequence_value(raw_value):
                raise DatatypeMappingError(f"Не удалось привести значение {raw_value!r} к массиву {expected_type}.")
            value = [self._coerce_scalar(item, expected_type) for item in raw_value]
            value = self._apply_numeric_transform(value, expected_type, node)
            value_type = f"{expected_type}[]"
        elif node.value_shape == "object":
            value = raw_value
            value_type = "object"
        else:
            value = self._coerce_scalar(raw_value, expected_type)
            value = self._apply_numeric_transform(value, expected_type, node)
            value_type = expected_type

        unit = node.value_transform.target_unit or node.unit
        return value, value_type, unit

    def _coerce_scalar(self, value: Any, expected_type: str) -> Any:
        if value is None:
            return None
        if expected_type == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.lower()
                if lowered in {"true", "1"}:
                    return True
                if lowered in {"false", "0"}:
                    return False
            raise DatatypeMappingError(f"Не удалось привести значение {value!r} к bool.")
        if expected_type == "int":
            return int(value)
        if expected_type == "float":
            return float(value)
        if expected_type == "str":
            return str(value)
        if expected_type == "char":
            if isinstance(value, str):
                if len(value) == 1:
                    return value
                if value.isdigit():
                    return chr(int(value))
                raise DatatypeMappingError(f"Не удалось привести значение {value!r} к char.")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                code_point = int(value)
                if 0 <= code_point <= 0x10FFFF:
                    return chr(code_point)
            raise DatatypeMappingError(f"Не удалось привести значение {value!r} к char.")
        if expected_type == "datetime":
            if isinstance(value, datetime):
                return value
            raise DatatypeMappingError(f"Не удалось привести значение {value!r} к datetime.")
        raise DatatypeMappingError(f"Неподдерживаемый ожидаемый тип: {expected_type}.")

    def _apply_numeric_transform(self, value: Any, expected_type: str, node: NodeConfig) -> Any:
        if isinstance(value, list):
            return [self._apply_numeric_transform(item, expected_type, node) for item in value]

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            value = (float(value) * node.value_transform.scale_factor) + node.value_transform.offset
            if expected_type == "int":
                value = int(value)
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                raise DatatypeMappingError("После нормализации получено значение NaN или Infinity.")
        return value

    def _is_sequence_value(self, value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
