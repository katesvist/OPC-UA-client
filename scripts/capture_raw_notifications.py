#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ScriptError(RuntimeError):
    pass


def main() -> int:
    args = parse_args()
    try:
        result = start_capture(args)
    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Cancelled by user.", file=sys.stderr)
        return 130

    output = result.get("output")
    duration = float(result.get("duration_seconds") or args.duration)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print()
    print(f"Capture started for {duration:g}s.")
    if args.wait:
        time.sleep(duration + args.settle_seconds)
        print("Capture window finished.")
    if output:
        print()
        print("View from server/container:")
        print(f"docker compose -f docker-compose.server.yml exec -T opcua-client tail -200 {output}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a short raw OPC UA datachange capture inside the running client process."
    )
    parser.add_argument("--base-url", default=os.getenv("OPC_CLIENT_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--token", default=os.getenv("OPC_CLIENT_TOKEN", "secret-token"))
    parser.add_argument("--duration", type=float, default=2.0, help="Capture duration in seconds, max 60.")
    parser.add_argument("--endpoint-id", default=None, help="Optional endpoint filter.")
    parser.add_argument("--max-records", type=int, default=100000)
    parser.add_argument("--output", default=None, help="Path inside opcua-client container. Defaults to /tmp/*.jsonl.")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--settle-seconds", type=float, default=0.5)
    parser.add_argument("--no-wait", action="store_false", dest="wait", help="Return immediately after start.")
    parser.set_defaults(wait=True)
    return parser.parse_args()


def start_capture(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "duration_seconds": args.duration,
        "endpoint_id": args.endpoint_id,
        "max_records": args.max_records,
    }
    if args.output:
        payload["output"] = args.output
    else:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        payload["output"] = f"/tmp/opcua-raw-capture-{timestamp}.jsonl"

    url = f"{args.base_url.rstrip('/')}/debug/raw-capture/start"
    data = json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {args.token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=args.timeout) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ScriptError(f"HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise ScriptError(f"Client API is unavailable: {exc}") from exc

    loaded = json.loads(body)
    if not isinstance(loaded, dict):
        raise ScriptError(f"Unexpected response: {body}")
    return loaded


if __name__ == "__main__":
    raise SystemExit(main())
