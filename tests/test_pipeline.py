from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest

import clickhouse_admin_panel as panel


def test_transform_preserves_numeric_percentages_and_uint64_counts():
    logical, physical = panel.transform(
        [("events", 2**63 + 1, 1, 0.5, 50.0)],
        [("events", "50.00 B", "100.00 B", 0.5, 50.0)],
    )
    assert logical["Event_Count"][0] == 2**63 + 1
    assert physical.schema["Physical Compression"] == pl.Float64
    assert physical["Physical Compression"][0] == 50.0


def test_transform_empty_results_keeps_column_contract():
    logical, physical = panel.transform([], [])
    assert logical.shape == physical.shape == (0, 5)
    assert logical.schema["Ratio"] == physical.schema["Ratio"] == pl.Float64


def test_extract_executes_final_query_without_trailing_sentinel(config):
    client = Mock()
    client.query.side_effect = [
        SimpleNamespace(result_rows=[("first",)]),
        SimpleNamespace(result_rows=[("last",)]),
        SimpleNamespace(result_rows=[("physical",)]),
    ]
    task = panel.ClickHouseDataExtraction(client=client, config=config)
    logical, physical = task.extract(
        [panel.CompressionQuery("first", {}), panel.CompressionQuery("last", {})],
        [panel.CompressionQuery("physical", {})],
    )
    assert logical == [("first",), ("last",)]
    assert physical == [("physical",)]
    assert client.query.call_count == 3


def test_client_uses_configured_transport_without_insert_settings(config, monkeypatch):
    factory = Mock()
    monkeypatch.setattr(panel.clickhouse_connect, "get_client", factory)
    panel.create_clickhouse_client(config)
    assert factory.call_args.kwargs["secure"] is False
    assert factory.call_args.kwargs["database"] == "default"
    assert "settings" not in factory.call_args.kwargs


def test_query_failures_propagate_and_connection_is_closed(config, monkeypatch):
    client = Mock()
    client.query.side_effect = RuntimeError("query failed")
    monkeypatch.setattr(panel, "Settings", lambda: config)
    monkeypatch.setattr(panel, "create_clickhouse_client", lambda config: client)
    monkeypatch.setattr(panel.st, "title", lambda title: None)
    with pytest.raises(RuntimeError, match="query failed"):
        panel.run()
    client.close.assert_called_once()
