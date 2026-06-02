#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

JsonObject = dict[str, Any]


@dataclass(frozen=True)
class MatchItem:
    index: int
    raw: JsonObject
    matched_text: str


@dataclass(frozen=True)
class PlannedChange:
    index: int
    action: str
    node_id: str
    browse_name: str
    parameter_code: str
    dict_param_id: str
    config_id: str


class ScriptError(RuntimeError):
    pass


def main() -> int:
    args = parse_args()
    timestamp = time.strftime("%Y%m%d-%H%M%S")

    try:
        run(args, timestamp)
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Cancelled by user.", file=sys.stderr)
        return 130
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Safely add OPC UA array element nodes to client config and assign dictionary parameters by ARRAY[index]. "
            "Dry-run is the default; use --apply to persist changes."
        )
    )
    parser.add_argument("--client-url", default=os.getenv("OPC_CLIENT_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--dashboard-url", default=os.getenv("OPC_DASHBOARD_URL", "http://127.0.0.1:8090"))
    parser.add_argument("--token", default=os.getenv("OPC_CLIENT_TOKEN", "secret-token"))
    parser.add_argument("--endpoint", required=True, help="Client endpoint id, e.g. remote-opc-server.")
    parser.add_argument("--parent-node-id", required=True, help='OPC UA parent node id to browse, e.g. ns=3;s="_DB_FOR_TEST".')
    parser.add_argument("--array-name", default="ARRAY", help="Array name used in ARRAY[index] matching.")
    parser.add_argument("--param-contains", action="append", default=[], help="Required substring in parameter name/description.")
    parser.add_argument("--node-contains", action="append", default=[], help="Required substring in browse node fields.")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, required=True, help="Inclusive end index.")
    parser.add_argument("--expect-count", type=int, default=None)
    parser.add_argument("--browse-depth", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--sampling-interval-ms", type=int, default=1000)
    parser.add_argument("--polling-interval-seconds", type=float, default=5.0)
    parser.add_argument("--stale-after-seconds", type=int, default=30)
    parser.add_argument("--group-path", default=None, help='Optional slash-separated group path, e.g. "_DB_FOR_TEST/ARRAY".')
    parser.add_argument("--overwrite", action="store_true", help="Update existing target node mappings when they differ.")
    parser.add_argument("--apply", action="store_true", help="Persist changes. Without this flag the script only validates.")
    parser.add_argument(
        "--backup-dir",
        default="backups/array-node-assignment",
        help="Directory for backups and reports, relative to current working directory unless absolute.",
    )
    return parser.parse_args()


def run(args: argparse.Namespace, timestamp: str) -> None:
    validate_args(args)
    client_url = args.client_url.rstrip("/")
    dashboard_url = args.dashboard_url.rstrip("/")
    expected_indexes = set(range(args.start, args.end + 1))
    expected_count = args.expect_count if args.expect_count is not None else len(expected_indexes)

    print("Checking client readiness...")
    ready = http_json("GET", f"{client_url}/ready", token=args.token, timeout=args.timeout)
    if isinstance(ready, dict) and not ready.get("ready", False):
        raise ScriptError(f"Client is not ready: {json.dumps(ready, ensure_ascii=False)}")

    print("Loading current client node configuration...")
    current_nodes = load_config_nodes(client_url, args.token, args.timeout)

    print("Loading dictionary from dashboard...")
    dictionary = load_dictionary(dashboard_url, args.timeout)

    print("Browsing OPC UA parent node...")
    browse_items = browse_parent(client_url, args, args.timeout)

    param_matches = build_match_map(
        items=dictionary,
        array_name=args.array_name,
        fields=("name", "description"),
        required_substrings=args.param_contains,
        expected_indexes=expected_indexes,
        label="dictionary parameter",
    )
    node_matches = build_match_map(
        items=browse_items,
        array_name=args.array_name,
        fields=("browse_name", "display_name", "node_id"),
        required_substrings=args.node_contains,
        expected_indexes=expected_indexes,
        label="OPC UA browse node",
    )

    validate_match_count(param_matches, expected_indexes, expected_count, "dictionary parameters")
    validate_match_count(node_matches, expected_indexes, expected_count, "OPC UA browse nodes")

    planned_nodes, changes = build_planned_nodes(args, current_nodes, node_matches, param_matches)
    report_dir = Path(args.backup_dir)
    write_report(report_dir, timestamp, changes)

    print_summary(args, current_nodes, planned_nodes, changes)

    if not args.apply:
        print("DRY RUN: no changes were saved. Re-run with --apply to persist.")
        return

    backup_path = write_backup(report_dir, timestamp, current_nodes)
    print(f"Backup written: {backup_path}")
    print("Saving updated node configuration to client...")
    response = http_json(
        "PUT",
        f"{client_url}/config/nodes",
        token=args.token,
        timeout=args.timeout,
        body={"nodes": planned_nodes},
    )
    saved_nodes = response.get("nodes", []) if isinstance(response, dict) else []
    print(f"Saved successfully. Client returned {len(saved_nodes)} configured nodes.")


def validate_args(args: argparse.Namespace) -> None:
    if args.end < args.start:
        raise ScriptError("--end must be greater than or equal to --start.")
    if args.expect_count is not None and args.expect_count <= 0:
        raise ScriptError("--expect-count must be positive.")
    if args.browse_depth < 1:
        raise ScriptError("--browse-depth must be at least 1.")


def http_json(
    method: str,
    url: str,
    *,
    token: str | None = None,
    timeout: float = 60.0,
    body: JsonObject | None = None,
) -> Any:
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
            if not raw:
                return None
            return json.loads(raw.decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"{method} {url} failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise ScriptError(f"{method} {url} failed: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ScriptError(f"{method} {url} returned invalid JSON: {exc}") from exc


def load_config_nodes(client_url: str, token: str, timeout: float) -> list[JsonObject]:
    response = http_json("GET", f"{client_url}/config/nodes", token=token, timeout=timeout)
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict) and isinstance(response.get("nodes"), list):
        return [item for item in response["nodes"] if isinstance(item, dict)]
    raise ScriptError("Unexpected /config/nodes response format.")


def load_dictionary(dashboard_url: str, timeout: float) -> list[JsonObject]:
    response = http_json("GET", f"{dashboard_url}/api/dictionary", timeout=timeout)
    if isinstance(response, dict) and isinstance(response.get("params"), list):
        return [item for item in response["params"] if isinstance(item, dict)]
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    raise ScriptError("Unexpected /api/dictionary response format.")


def browse_parent(client_url: str, args: argparse.Namespace, timeout: float) -> list[JsonObject]:
    response = http_json(
        "POST",
        f"{client_url}/browse",
        token=args.token,
        timeout=timeout,
        body={
            "endpoint_id": args.endpoint,
            "node_id": args.parent_node_id,
            "max_depth": args.browse_depth,
            "include_variables": True,
            "include_objects": True,
        },
    )
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict) and isinstance(response.get("items"), list):
        return [item for item in response["items"] if isinstance(item, dict)]
    raise ScriptError("Unexpected /browse response format.")


def build_match_map(
    *,
    items: list[JsonObject],
    array_name: str,
    fields: tuple[str, ...],
    required_substrings: list[str],
    expected_indexes: set[int],
    label: str,
) -> dict[int, MatchItem]:
    pattern = re.compile(rf"{re.escape(array_name)}\s*\[\s*(\d+)\s*\]")
    matches: dict[int, MatchItem] = {}
    duplicates: dict[int, list[str]] = {}

    for item in items:
        haystack = [string_value(item.get(field)) for field in fields]
        if required_substrings and not contains_all(haystack, required_substrings):
            continue
        matched = first_index_match(pattern, haystack)
        if matched is None:
            continue
        index, matched_text = matched
        if index not in expected_indexes:
            continue
        if index in matches:
            duplicates.setdefault(index, [matches[index].matched_text]).append(matched_text)
            continue
        matches[index] = MatchItem(index=index, raw=item, matched_text=matched_text)

    if duplicates:
        sample = "; ".join(f"{idx}: {values[:3]}" for idx, values in sorted(duplicates.items())[:10])
        raise ScriptError(f"Duplicate {label} indexes found: {sample}")
    return matches


def contains_all(values: list[str], required_substrings: list[str]) -> bool:
    normalized_values = [value.lower() for value in values]
    for required in required_substrings:
        needle = required.lower()
        if not any(needle in value for value in normalized_values):
            return False
    return True


def first_index_match(pattern: re.Pattern[str], values: list[str]) -> tuple[int, str] | None:
    for value in values:
        match = pattern.search(value)
        if match:
            return int(match.group(1)), value
    return None


def validate_match_count(
    matches: dict[int, MatchItem],
    expected_indexes: set[int],
    expected_count: int,
    label: str,
) -> None:
    missing = sorted(expected_indexes - set(matches))
    extra_count = len(matches) - expected_count
    if len(matches) != expected_count or missing or extra_count:
        parts = [f"matched {len(matches)}, expected {expected_count}"]
        if missing:
            parts.append(f"missing indexes: {compact_indexes(missing)}")
        raise ScriptError(f"Invalid {label} match count ({'; '.join(parts)}).")


def build_planned_nodes(
    args: argparse.Namespace,
    current_nodes: list[JsonObject],
    node_matches: dict[int, MatchItem],
    param_matches: dict[int, MatchItem],
) -> tuple[list[JsonObject], list[PlannedChange]]:
    planned_nodes = [dict(node) for node in current_nodes]
    existing_by_node_key = {
        (string_value(node.get("endpoint_id")), string_value(node.get("node_id"))): idx
        for idx, node in enumerate(planned_nodes)
    }
    existing_by_id = {string_value(node.get("id")): idx for idx, node in enumerate(planned_nodes) if node.get("id")}
    existing_param_owner = build_existing_param_owner(planned_nodes, args.endpoint)

    changes: list[PlannedChange] = []
    for index in sorted(node_matches):
        browse_node = node_matches[index].raw
        param = param_matches[index].raw
        node_id = require_string(browse_node, "node_id", f"browse node index {index}")
        parameter_code = require_string(param, "name", f"dictionary parameter index {index}")
        dict_param_id = string_value(param.get("id"))
        existing_index = existing_by_node_key.get((args.endpoint, node_id))
        existing = planned_nodes[existing_index] if existing_index is not None else None

        validate_parameter_not_used_elsewhere(
            existing_param_owner=existing_param_owner,
            endpoint_id=args.endpoint,
            node_id=node_id,
            parameter_code=parameter_code,
            dict_param_id=dict_param_id,
            index=index,
        )

        desired = build_node_config(args, index, browse_node, param, previous=existing)
        config_id = string_value(desired.get("id"))
        colliding_id_index = existing_by_id.get(config_id)
        if colliding_id_index is not None and colliding_id_index != existing_index:
            raise ScriptError(f"Generated config id collision for index {index}: {config_id}")

        if existing is None:
            planned_nodes.append(desired)
            existing_by_node_key[(args.endpoint, node_id)] = len(planned_nodes) - 1
            existing_by_id[config_id] = len(planned_nodes) - 1
            action = "add"
        elif is_same_mapping(existing, parameter_code, dict_param_id):
            action = "unchanged"
        elif args.overwrite:
            planned_nodes[existing_index] = desired
            action = "update"
        else:
            raise ScriptError(
                "Existing node mapping differs for "
                f"index {index}: node_id={node_id}, current={existing.get('parameter_code')}, desired={parameter_code}. "
                "Use --overwrite if this replacement is intentional."
            )

        changes.append(
            PlannedChange(
                index=index,
                action=action,
                node_id=node_id,
                browse_name=string_value(browse_node.get("browse_name") or browse_node.get("display_name")),
                parameter_code=parameter_code,
                dict_param_id=dict_param_id,
                config_id=config_id,
            )
        )

    return planned_nodes, changes


def build_existing_param_owner(nodes: list[JsonObject], endpoint_id: str) -> dict[tuple[str, str], str]:
    result: dict[tuple[str, str], str] = {}
    for node in nodes:
        if node.get("endpoint_id") != endpoint_id:
            continue
        node_id = string_value(node.get("node_id"))
        for key_name in ("dict_param_id", "parameter_code"):
            value = string_value(node.get(key_name))
            if value:
                result[(key_name, value)] = node_id
    return result


def validate_parameter_not_used_elsewhere(
    *,
    existing_param_owner: dict[tuple[str, str], str],
    endpoint_id: str,
    node_id: str,
    parameter_code: str,
    dict_param_id: str,
    index: int,
) -> None:
    del endpoint_id
    checks = [("parameter_code", parameter_code)]
    if dict_param_id:
        checks.append(("dict_param_id", dict_param_id))
    for key_name, value in checks:
        owner_node_id = existing_param_owner.get((key_name, value))
        if owner_node_id and owner_node_id != node_id:
            raise ScriptError(
                f"Parameter for index {index} is already used by another node: "
                f"{key_name}={value}, owner_node_id={owner_node_id}, target_node_id={node_id}"
            )


def build_node_config(
    args: argparse.Namespace,
    index: int,
    browse_node: JsonObject,
    param: JsonObject,
    *,
    previous: JsonObject | None,
) -> JsonObject:
    parameter_code = require_string(param, "name", f"dictionary parameter index {index}")
    unit = first_present(param, "unit_symbol", "unit_name")
    datatype = string_value(param.get("datatype_name"))
    group_path = resolve_group_path(args)
    group_id = make_node_config_id(args.endpoint, "/".join(group_path))

    return {
        **(previous or {}),
        "id": string_value(previous.get("id")) if previous else make_node_config_id(args.endpoint, parameter_code),
        "endpoint_id": args.endpoint,
        "node_id": require_string(browse_node, "node_id", f"browse node index {index}"),
        "namespace_uri": previous.get("namespace_uri") if previous else None,
        "browse_name": first_present(browse_node, "browse_name"),
        "display_name": first_present(browse_node, "display_name"),
        "acquisition_mode": "subscription",
        "read_enabled": True,
        "write_enabled": False,
        "sampling_interval_ms": args.sampling_interval_ms,
        "polling_interval_seconds": args.polling_interval_seconds,
        "parameter_code": parameter_code,
        "parameter_name": string_value(param.get("description") or parameter_code),
        "dict_param_id": string_value(param.get("id")) or None,
        "type_by_dict": datatype or None,
        "unit_by_dict": unit or None,
        "expected_type": map_datatype_to_expected_type(datatype),
        "value_shape": "scalar",
        "unit": unit or None,
        "group_id": group_id,
        "group_path": group_path,
        "group_display_name": group_path[-1] if group_path else args.array_name,
        "input_control": {
            "stale_after_seconds": args.stale_after_seconds,
            "suppress_duplicates": False,
        },
        "value_transform": {
            "scale_factor": 1,
            "offset": 0,
            "target_unit": unit or None,
        },
        "metadata": {
            **((previous or {}).get("metadata") or {}),
            "opcua_browse_name": first_present(browse_node, "browse_name"),
            "opcua_display_name": first_present(browse_node, "display_name"),
            "opcua_data_type": first_present(browse_node, "data_type"),
            "opcua_value_rank": browse_node.get("value_rank"),
            "opcua_array_dimensions": browse_node.get("array_dimensions") or [],
            "dict_param_name": parameter_code,
            "dict_param_description": string_value(param.get("description")) or None,
            "array_index": index,
        },
        "tags": (previous or {}).get("tags") or [],
    }


def resolve_group_path(args: argparse.Namespace) -> list[str]:
    if args.group_path:
        return [part for part in args.group_path.split("/") if part]
    if args.param_contains:
        return [args.param_contains[0], args.array_name]
    return [args.array_name]


def is_same_mapping(node: JsonObject, parameter_code: str, dict_param_id: str) -> bool:
    same_code = node.get("parameter_code") == parameter_code
    same_id = not dict_param_id or node.get("dict_param_id") == dict_param_id
    return same_code and same_id


def map_datatype_to_expected_type(datatype: str) -> str:
    normalized = datatype.lower()
    if normalized in {"integer", "int", "dint", "long", "int16", "int32", "int64", "uint16", "uint32", "uint64"}:
        return "int"
    if normalized in {"boolean", "bool"}:
        return "bool"
    if normalized in {"string", "text"}:
        return "str"
    if normalized in {"char", "byte"}:
        return "char"
    if normalized in {"datetime", "date_time", "date"}:
        return "datetime"
    return "float"


def make_node_config_id(endpoint_id: str, param_name: str) -> str:
    raw = f"{endpoint_id}-{param_name}".lower()
    chars: list[str] = []
    previous_dash = False
    for char in raw:
        if char.isalnum():
            chars.append(char)
            previous_dash = False
        elif not previous_dash:
            chars.append("-")
            previous_dash = True
    value = "".join(chars).strip("-")[:96].strip("-")
    return value or f"node-{int(time.time())}"


def write_backup(report_dir: Path, timestamp: str, current_nodes: list[JsonObject]) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"config-nodes-before-array-assignment-{timestamp}.json"
    path.write_text(json.dumps({"nodes": current_nodes}, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_report(report_dir: Path, timestamp: str, changes: list[PlannedChange]) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"array-assignment-report-{timestamp}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("index", "action", "node_id", "browse_name", "parameter_code", "dict_param_id", "config_id"),
        )
        writer.writeheader()
        for change in changes:
            writer.writerow(
                {
                    "index": change.index,
                    "action": change.action,
                    "node_id": change.node_id,
                    "browse_name": change.browse_name,
                    "parameter_code": change.parameter_code,
                    "dict_param_id": change.dict_param_id,
                    "config_id": change.config_id,
                }
            )
    print(f"Report written: {path}")
    return path


def print_summary(
    args: argparse.Namespace,
    current_nodes: list[JsonObject],
    planned_nodes: list[JsonObject],
    changes: list[PlannedChange],
) -> None:
    counts: dict[str, int] = {}
    for change in changes:
        counts[change.action] = counts.get(change.action, 0) + 1
    print("")
    print("Summary")
    print("-------")
    print(f"Endpoint: {args.endpoint}")
    print(f"Parent node: {args.parent_node_id}")
    print(f"Array: {args.array_name}[{args.start}..{args.end}]")
    print(f"Current config nodes: {len(current_nodes)}")
    print(f"Planned config nodes: {len(planned_nodes)}")
    print(f"Add: {counts.get('add', 0)}")
    print(f"Update: {counts.get('update', 0)}")
    print(f"Unchanged: {counts.get('unchanged', 0)}")
    print("")


def compact_indexes(indexes: list[int]) -> str:
    if len(indexes) <= 20:
        return ", ".join(str(item) for item in indexes)
    head = ", ".join(str(item) for item in indexes[:10])
    tail = ", ".join(str(item) for item in indexes[-10:])
    return f"{head}, ... , {tail}"


def first_present(item: JsonObject, *keys: str) -> str | None:
    for key in keys:
        value = string_value(item.get(key))
        if value:
            return value
    return None


def require_string(item: JsonObject, key: str, label: str) -> str:
    value = string_value(item.get(key))
    if not value:
        raise ScriptError(f"Missing required field '{key}' in {label}.")
    return value


def string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
