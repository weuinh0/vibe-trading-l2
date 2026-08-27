"""Tests for the local, read-only Lovel2 L2 tool."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from src.config.accessor import reset_env_config
from src.tools.lovel2_l2_tool import Lovel2L2Tool


def _payloads() -> list[tuple[str, str]]:
    market = [
        "1", "600000.SH", "600000", "20260828", "093000123", "T",
        "100000", "101000", "102000", "99500", "101500",
        *[str(value) for value in range(101600, 102600, 100)],
        *[str(value) for value in range(1000, 2000, 100)],
        *[str(value) for value in range(101400, 100400, -100)],
        *[str(value) for value in range(1100, 2100, 100)],
        "25", "3000", "304500000", "50000", "48000", "101200", "101700",
        "111600", "90400", "120", "130", "2", "300", "3", "400",
    ]
    order = [
        "2", "600000.SH", "600000", "20260828", "093000124", "9001",
        "101500", "200", "A", "B", "0", "10001", "1",
    ]
    transaction = [
        "3", "600000.SH", "600000", "20260828", "093000125", "8001",
        "101500", "100", "10150000", "B", "T", "0", "10001", "10002",
    ]
    return [("market", ",".join(market)), ("order", ",".join(order)), ("tran", ",".join(transaction))]


def _write_fixture(path: Path, *, partial_tail: bool = False) -> None:
    rows = []
    for sequence, (channel, payload) in enumerate(_payloads(), start=1):
        rows.append(
            json.dumps(
                {
                    "schema": "L2_CUSTOMER_RECORD_V2",
                    "channel": channel,
                    "symbol": "600000.SH",
                    "trading_day": "20260828",
                    "stream_id": "synthetic",
                    "stream_sequence": sequence,
                    "seller_packet_number": sequence,
                    "seller_session_id": "synthetic",
                    "source_event_ns": 0,
                    "local_process_ns": 0,
                    "delivery_path": "fixture",
                    "payload_base64": base64.b64encode(payload.encode()).decode(),
                }
            )
        )
    text = "\n".join(rows) + "\n"
    if partial_tail:
        text += '{"schema":"L2_CUSTOMER_RECORD_V2"'
    path.write_text(text, encoding="utf-8")


def _configure(monkeypatch, path: Path) -> None:
    monkeypatch.setenv("LOVEL2_JSONL_PATH", str(path))
    reset_env_config()


def test_unconfigured_response_is_actionable(monkeypatch) -> None:
    monkeypatch.delenv("LOVEL2_JSONL_PATH", raising=False)
    reset_env_config()

    payload = json.loads(Lovel2L2Tool().execute(mode="status"))
    assert payload["ok"] is False
    assert "LOVEL2_JSONL_PATH" in payload["error"]
    assert payload["setup_url"] == "https://www.lovel2.site/"


def test_status_does_not_expose_full_local_path(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "synthetic_l2.jsonl"
    _write_fixture(path)
    _configure(monkeypatch, path)

    raw = Lovel2L2Tool().execute(mode="status")
    payload = json.loads(raw)
    assert payload["ok"] is True
    assert payload["configured_file"] == path.name
    assert str(tmp_path) not in raw


def test_snapshot_returns_all_ten_levels_and_raw_amount(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "synthetic_l2.jsonl"
    _write_fixture(path, partial_tail=True)
    _configure(monkeypatch, path)

    payload = json.loads(Lovel2L2Tool().execute(mode="snapshot", symbol="600000.SH"))
    assert payload["ok"] is True
    assert payload["symbol"] == "600000.SH"
    assert payload["data"]["last"] == 10.15
    assert len(payload["data"]["asks"]) == 10
    assert len(payload["data"]["bids"]) == 10
    assert payload["data"]["trade_amount_raw"] == 304_500_000
    assert payload["freshness"]["is_stale"] is None


def test_order_and_transaction_modes_are_typed(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "synthetic_l2.jsonl"
    _write_fixture(path)
    _configure(monkeypatch, path)
    tool = Lovel2L2Tool()

    orders = json.loads(tool.execute(mode="orders", symbol="600000", limit=5))
    transactions = json.loads(tool.execute(mode="transactions", symbol="600000.SH", limit=5))
    assert orders["data"][0]["order_number"] == "9001"
    assert orders["data"][0]["price"] == 10.15
    assert transactions["data"][0]["trade_number"] == "8001"
    assert transactions["data"][0]["amount_raw"] == 10_150_000


def test_invalid_symbol_fails_closed(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "synthetic_l2.jsonl"
    _write_fixture(path)
    _configure(monkeypatch, path)

    payload = json.loads(Lovel2L2Tool().execute(mode="snapshot", symbol="../../secret"))
    assert payload["ok"] is False
    assert "six-digit" in payload["error"]

