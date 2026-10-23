from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from catalog import filter_catalog, load_catalog, summarize


@pytest.fixture
def snapshot():
    client = Mock()
    client.query.side_effect = [
        SimpleNamespace(
            result_rows=[
                ("alpha", "events", "MergeTree", 10, 1000, False),
                ("beta", "events", "MergeTree", 20, 2000, False),
                ("alpha", "empty", "MergeTree", 0, 0, False),
                ("system", "tables", "SystemTables", None, None, False),
            ]
        ),
        SimpleNamespace(
            result_rows=[
                ("alpha", "events", 100, 1000, 1),
                ("beta", "events", 900, 10000, 2),
            ]
        ),
    ]
    client.command.return_value = "26.9.25.15"
    return load_catalog(client)


def test_catalog_keeps_same_named_tables_in_separate_databases(snapshot):
    rows = {row["Relation"]: row for row in snapshot.tables.to_dicts()}
    assert rows["alpha.events"]["Compressed bytes"] == 100
    assert rows["beta.events"]["Compressed bytes"] == 900
    assert rows["alpha.events"]["Compression (%)"] == 90
    assert rows["beta.events"]["Compression (%)"] == 91


def test_virtual_metadata_remains_unknown_and_empty_storage_is_zero(snapshot):
    rows = {row["Relation"]: row for row in snapshot.tables.to_dicts()}
    assert rows["system.tables"]["Rows"] is None
    assert rows["system.tables"]["Compressed bytes"] is None
    assert rows["alpha.empty"]["Compressed bytes"] == 0
    assert rows["alpha.empty"]["Compression (%)"] is None


def test_summary_weights_compression_by_bytes(snapshot):
    stats = summarize(snapshot.tables)
    assert stats["tables"] == 4
    assert stats["databases"] == 3
    assert stats["rows"] == 30
    assert stats["compressed"] == 1000
    assert stats["compression"] == pytest.approx((1 - 1000 / 11000) * 100)


def test_catalog_filters_preserve_database_scope_and_literal_search(snapshot):
    result = filter_catalog(snapshot.tables, ["alpha"], search="ALPHA.EVENTS")
    assert result["Relation"].to_list() == ["alpha.events"]
    assert filter_catalog(snapshot.tables, ["alpha"], search="[").is_empty()
    assert filter_catalog(snapshot.tables, []).is_empty()
    assert filter_catalog(snapshot.tables, ["system"], "MergeTree family").is_empty()
    assert filter_catalog(snapshot.tables, ["system"], "Other engines").height == 1


def test_all_unknown_metadata_is_unavailable_in_summary(snapshot):
    virtual_tables = filter_catalog(snapshot.tables, ["system"], "Other engines")
    stats = summarize(virtual_tables)
    assert stats["rows"] is None
    assert stats["compressed"] is None
    assert stats["compression"] is None
