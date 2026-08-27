"""Read-only access to an operator-configured Lovel2 customer JSONL stream.

The official customer client owns authentication and writes one
``L2_CUSTOMER_RECORD_V2`` JSON object per line.  This tool never connects to a
remote service and never accepts a path from the model: the operator pins the
only readable file through ``LOVEL2_JSONL_PATH`` in the centralized EnvConfig.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.agent.tools import BaseTool
from src.config.accessor import get_env_config


_SCHEMA = "L2_CUSTOMER_RECORD_V2"
_PRICE_SCALE = 10_000
_DEFAULT_SCAN_BYTES = 32 * 1024 * 1024
_MIN_SCAN_BYTES = 64 * 1024
_MAX_SCAN_BYTES = 256 * 1024 * 1024
_DEFAULT_STALE_SECONDS = 15.0
_MAX_EVENTS = 100
_SHANGHAI_TZ = timezone(timedelta(hours=8))
_SERVICE_URL = "https://www.lovel2.site/"
_CHANNEL_BY_MODE = {
    "snapshot": "market",
    "orders": "order",
    "transactions": "tran",
}
_MODE_ALIASES = {
    "market": "snapshot",
    "order": "orders",
    "tran": "transactions",
    "transaction": "transactions",
}


def _error(message: str, **extra: Any) -> str:
    payload = {"ok": False, "source": "lovel2_local_jsonl", "error": message}
    payload.update(extra)
    return json.dumps(payload, ensure_ascii=False, default=str)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _price(value: Any) -> float | None:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number / _PRICE_SCALE


def _normalize_symbol(value: Any) -> tuple[str, str] | None:
    text = str(value or "").strip().upper().replace("-", ".").replace("_", ".")
    if not text:
        return None
    code = ""
    venue = ""
    parts = [part for part in text.split(".") if part]
    if len(parts) == 2:
        if parts[0].isdigit():
            code, venue = parts
        elif parts[1].isdigit():
            venue, code = parts
    elif text[:2] in {"SH", "SZ"} and text[2:].isdigit():
        venue, code = text[:2], text[2:]
    elif text.isdigit():
        code = text
    if len(code) != 6 or not code.isdigit():
        return None
    if venue in {"SH", "SSE", "XSHG"}:
        suffix = "SH"
    elif venue in {"SZ", "SZSE", "XSHE"}:
        suffix = "SZ"
    elif venue:
        return None
    elif code.startswith(("5", "6", "9", "11")):
        suffix = "SH"
    else:
        suffix = "SZ"
    return code, suffix


def _exchange_datetime(day: str, clock: str) -> str | None:
    day_digits = "".join(char for char in str(day) if char.isdigit())
    clock_digits = "".join(char for char in str(clock) if char.isdigit()).zfill(9)
    if len(day_digits) != 8 or len(clock_digits) < 6:
        return None
    try:
        value = datetime.strptime(day_digits + clock_digits[:6], "%Y%m%d%H%M%S")
        value = value.replace(tzinfo=_SHANGHAI_TZ)
        value += timedelta(milliseconds=int((clock_digits[6:9] or "0").ljust(3, "0")))
    except ValueError:
        return None
    return value.isoformat()


def _iso_from_ns(value: Any) -> str | None:
    nanoseconds = _coerce_int(value)
    if nanoseconds <= 0:
        return None
    try:
        return datetime.fromtimestamp(
            nanoseconds / 1_000_000_000,
            tz=timezone.utc,
        ).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _read_recent_lines(path: Path, max_bytes: int) -> tuple[list[bytes], int, bool]:
    """Return complete lines from a bounded tail window, newest first."""

    with path.open("rb") as handle:
        handle.seek(0, 2)
        size = handle.tell()
        start = max(0, size - max_bytes)
        handle.seek(start)
        data = handle.read()

    if start and data:
        newline = data.find(b"\n")
        data = data[newline + 1 :] if newline >= 0 else b""
    if data and not data.endswith(b"\n"):
        last_newline = data.rfind(b"\n")
        data = data[: last_newline + 1] if last_newline >= 0 else b""
    lines = [line for line in data.splitlines() if line.strip()]
    return list(reversed(lines)), len(data), start > 0


def _decode_records(row: dict[str, Any]) -> list[tuple[str, tuple[str, ...]]]:
    if row.get("schema") != _SCHEMA:
        return []
    channel = str(row.get("channel", ""))
    expected = {"market": 66, "order": 13, "tran": 14}.get(channel)
    if expected is None:
        return []
    try:
        raw = base64.b64decode(str(row.get("payload_base64", "")), validate=True)
        payload = raw.decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, TypeError):
        return []

    records: list[tuple[str, tuple[str, ...]]] = []
    first_packet = ""
    for index, record in enumerate(payload.split("#")):
        if not record:
            continue
        fields = tuple(field.strip() for field in record.split(","))
        if index == 0 and fields:
            first_packet = fields[0]
        if len(fields) == expected - 1 and first_packet:
            fields = (first_packet, *fields)
        if len(fields) == expected:
            records.append((channel, fields))
    return records


def _market_record(fields: tuple[str, ...]) -> dict[str, Any]:
    symbol = _normalize_symbol(fields[1]) or _normalize_symbol(fields[2])
    display = f"{symbol[0]}.{symbol[1]}" if symbol else fields[2]
    return {
        "symbol": display,
        "exchange_time": _exchange_datetime(fields[3], fields[4]),
        "status": fields[5],
        "previous_close": _price(fields[6]),
        "open": _price(fields[7]),
        "high": _price(fields[8]),
        "low": _price(fields[9]),
        "last": _price(fields[10]),
        "asks": [
            {"level": level + 1, "price": _price(fields[11 + level]), "volume": _coerce_int(fields[21 + level])}
            for level in range(10)
        ],
        "bids": [
            {"level": level + 1, "price": _price(fields[31 + level]), "volume": _coerce_int(fields[41 + level])}
            for level in range(10)
        ],
        "trade_count": _coerce_int(fields[51]),
        "trade_volume": _coerce_int(fields[52]),
        "trade_amount_raw": _coerce_int(fields[53]),
        "total_bid_volume": _coerce_int(fields[54]),
        "total_ask_volume": _coerce_int(fields[55]),
        "average_bid_price": _price(fields[56]),
        "average_ask_price": _price(fields[57]),
        "limit_up": _price(fields[58]),
        "limit_down": _price(fields[59]),
        "total_buy_orders": _coerce_int(fields[60]),
        "total_sell_orders": _coerce_int(fields[61]),
        "buy_cancel_orders": _coerce_int(fields[62]),
        "buy_cancel_volume": _coerce_int(fields[63]),
        "sell_cancel_orders": _coerce_int(fields[64]),
        "sell_cancel_volume": _coerce_int(fields[65]),
    }


def _order_record(fields: tuple[str, ...]) -> dict[str, Any]:
    symbol = _normalize_symbol(fields[1]) or _normalize_symbol(fields[2])
    return {
        "symbol": f"{symbol[0]}.{symbol[1]}" if symbol else fields[2],
        "exchange_time": _exchange_datetime(fields[3], fields[4]),
        "order_number": fields[5],
        "price": _price(fields[6]),
        "volume": _coerce_int(fields[7]),
        "order_type": fields[8],
        "side": fields[9],
        "original_order_number": fields[10],
        "sequence_number": fields[11],
        "channel_number": fields[12],
    }


def _transaction_record(fields: tuple[str, ...]) -> dict[str, Any]:
    symbol = _normalize_symbol(fields[1]) or _normalize_symbol(fields[2])
    return {
        "symbol": f"{symbol[0]}.{symbol[1]}" if symbol else fields[2],
        "exchange_time": _exchange_datetime(fields[3], fields[4]),
        "trade_number": fields[5],
        "price": _price(fields[6]),
        "volume": _coerce_int(fields[7]),
        "amount_raw": _coerce_int(fields[8]),
        "side": fields[9],
        "trade_type": fields[10],
        "original_number": fields[11],
        "ask_order_sequence": fields[12],
        "bid_order_sequence": fields[13],
    }


def _freshness(row: dict[str, Any], stale_seconds: float) -> dict[str, Any]:
    local_ns = _coerce_int(row.get("local_process_ns"))
    age_seconds: float | None = None
    if local_ns > 0:
        age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - local_ns / 1e9)
    return {
        "source_event_at": _iso_from_ns(row.get("source_event_ns")),
        "local_process_at": _iso_from_ns(local_ns),
        "age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "stale_after_seconds": stale_seconds,
        "is_stale": age_seconds > stale_seconds if age_seconds is not None else None,
    }


class Lovel2L2Tool(BaseTool):
    """Inspect a local, authorized A-share Level-2 JSONL stream."""

    name = "get_lovel2_l2"
    description = (
        "Read the operator-configured Lovel2 A-share Level-2 JSONL stream. "
        "Modes: status; snapshot (latest 10-level book); orders (recent raw "
        "order events); transactions (recent trade events). Supports SSE/SZSE "
        "stocks, ETFs and convertible bonds when entitled. Read-only; the "
        "model cannot select a filesystem path. Amount fields ending in _raw "
        "are not normalized because provider units can vary by instrument."
    )
    parameters = {
        "type": "object",
        "properties": {
            "mode": {
                "type": "string",
                "enum": ["status", "snapshot", "orders", "transactions"],
                "default": "snapshot",
            },
            "symbol": {
                "type": "string",
                "description": "A-share symbol such as 600000.SH or 000001.SZ; not required for status.",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": _MAX_EVENTS,
                "default": 20,
                "description": "Maximum recent rows for orders/transactions.",
            },
        },
        "additionalProperties": False,
    }
    repeatable = True
    is_readonly = True

    def execute(self, **kwargs: Any) -> str:
        mode = str(kwargs.get("mode") or "snapshot").strip().lower()
        mode = _MODE_ALIASES.get(mode, mode)
        if mode not in {"status", *_CHANNEL_BY_MODE}:
            return _error("mode must be status, snapshot, orders, or transactions")

        config = get_env_config().data
        raw_path = config.lovel2_jsonl_path.strip()
        if not raw_path:
            return _error(
                "Lovel2 is not configured; set LOVEL2_JSONL_PATH to the official client's local JSONL output",
                setup_url=_SERVICE_URL,
            )
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            return _error(
                "The configured LOVEL2_JSONL_PATH is not a readable file",
                configured_file=path.name,
            )

        scan_bytes = min(
            _MAX_SCAN_BYTES,
            max(_MIN_SCAN_BYTES, _coerce_int(config.lovel2_scan_bytes, _DEFAULT_SCAN_BYTES)),
        )
        stale_seconds = float(config.lovel2_stale_seconds)
        if not math.isfinite(stale_seconds) or stale_seconds < 0:
            stale_seconds = _DEFAULT_STALE_SECONDS

        stat = path.stat()
        if mode == "status":
            return json.dumps(
                {
                    "ok": True,
                    "source": "lovel2_local_jsonl",
                    "mode": "status",
                    "configured_file": path.name,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                    "scan_limit_bytes": scan_bytes,
                    "service_url": _SERVICE_URL,
                    "notice": "Local read-only integration; data access is governed by the customer's service agreement.",
                },
                ensure_ascii=False,
            )

        wanted = _normalize_symbol(kwargs.get("symbol"))
        if wanted is None:
            return _error("symbol must be a six-digit SSE/SZSE code such as 600000.SH")
        display_symbol = f"{wanted[0]}.{wanted[1]}"
        try:
            lines, scanned_bytes, truncated = _read_recent_lines(path, scan_bytes)
        except OSError as exc:
            return _error(f"failed to read configured Lovel2 JSONL: {exc}")

        target_channel = _CHANNEL_BY_MODE[mode]
        limit = min(_MAX_EVENTS, max(1, _coerce_int(kwargs.get("limit"), 20)))
        matches: list[tuple[dict[str, Any], tuple[str, ...]]] = []
        invalid_lines = 0
        for raw_line in lines:
            try:
                row = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                invalid_lines += 1
                continue
            if not isinstance(row, dict):
                invalid_lines += 1
                continue
            for channel, fields in reversed(_decode_records(row)):
                if channel != target_channel:
                    continue
                record_symbol = _normalize_symbol(fields[1]) or _normalize_symbol(fields[2])
                if record_symbol == wanted:
                    matches.append((row, fields))
                    if mode == "snapshot" or len(matches) >= limit:
                        break
            if (mode == "snapshot" and matches) or len(matches) >= limit:
                break

        scan_meta = {
            "scanned_bytes": scanned_bytes,
            "scan_truncated": truncated,
            "invalid_lines_skipped": invalid_lines,
        }
        if not matches:
            return _error(
                f"no {target_channel} record for {display_symbol} in the bounded tail window",
                mode=mode,
                symbol=display_symbol,
                **scan_meta,
            )

        if mode == "snapshot":
            row, fields = matches[0]
            data: dict[str, Any] | list[dict[str, Any]] = _market_record(fields)
            freshness = _freshness(row, stale_seconds)
        else:
            normalizer = _order_record if mode == "orders" else _transaction_record
            data = [normalizer(fields) for _, fields in matches]
            freshness = _freshness(matches[0][0], stale_seconds)

        return json.dumps(
            {
                "ok": True,
                "source": "lovel2_local_jsonl",
                "mode": mode,
                "symbol": display_symbol,
                "count": 1 if mode == "snapshot" else len(matches),
                "newest_first": mode != "snapshot",
                "freshness": freshness,
                "data": data,
                "units": {
                    "price": "CNY per security unit (source integer divided by 10000)",
                    "volume": "provider raw quantity",
                    "*_raw": "provider raw integer; do not assume a universal amount scale",
                },
                **scan_meta,
            },
            ensure_ascii=False,
        )

