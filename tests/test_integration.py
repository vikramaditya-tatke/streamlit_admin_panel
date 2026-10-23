from contextlib import closing
from pathlib import Path

import clickhouse_connect
import polars as pl
import pytest
from conftest import sql
from streamlit.testing.v1 import AppTest

from clickhouse_admin_panel import ClickHouseDataExtraction, transform

pytestmark = pytest.mark.integration
APP_PATH = Path(__file__).resolve().parents[1] / "clickhouse_admin_panel.py"
QUOTED_TABLE = "odd 'quote;`name"
LOGICAL_TABLES = {
    "events_alpha",
    "events_beta",
    "empty_events",
    "zero_events",
    "big_events",
    QUOTED_TABLE,
}
PHYSICAL_TABLES = LOGICAL_TABLES | {"without_event_count"}


@pytest.fixture
def frames(live_client, dataset):
    task = ClickHouseDataExtraction(client=live_client, config=dataset)
    return transform(*task.extract(*task.make_queries()))


def test_discovery_and_known_logical_results(frames):
    logical, physical = frames
    assert set(logical["Table"]) == LOGICAL_TABLES
    assert set(physical["Table"]) == PHYSICAL_TABLES
    rows = {row["Table"]: row for row in logical.to_dicts()}
    assert rows["events_alpha"]["Event_Count"] == 30
    assert rows["events_alpha"]["Row_Count"] == 2
    assert rows["events_alpha"]["Logical_Compression"] == pytest.approx(93.3333333333)
    assert rows["events_beta"]["Logical_Compression"] == 50.0
    assert rows[QUOTED_TABLE]["Logical_Compression"] == 75.0
    assert rows["big_events"]["Event_Count"] == 2**63 + 1


def test_physical_results_are_per_table_database_and_active_parts(
    frames, live_client, dataset
):
    _, physical = frames
    inactive = live_client.query(
        sql("inactive_parts.sql"), parameters={"database": dataset.clickhouse_database}
    ).first_row[0]
    assert inactive > 0
    rows = {row["Table"]: row for row in physical.to_dicts()}
    for table in ["events_alpha", "events_beta", QUOTED_TABLE]:
        compressed, uncompressed, compressed_text, uncompressed_text = (
            live_client.query(
                sql("part_sizes.sql"),
                parameters={"database": dataset.clickhouse_database, "table": table},
            ).first_row
        )
        assert rows[table]["Compressed Size"] == compressed_text
        assert rows[table]["Uncompressed Size"] == uncompressed_text
        assert rows[table]["Ratio"] == pytest.approx(compressed / uncompressed)
        assert rows[table]["Physical Compression"] == pytest.approx(
            (1 - compressed / uncompressed) * 100
        )
    assert rows["events_alpha"]["Ratio"] != rows["events_beta"]["Ratio"]
    assert physical.schema["Physical Compression"] == pl.Float64


@pytest.mark.parametrize("table", ["empty_events", "zero_events"])
def test_zero_denominators_are_null(frames, table):
    logical, physical = frames
    row = logical.filter(pl.col("Table") == table).row(0, named=True)
    assert row["Ratio"] is None
    assert row["Logical_Compression"] is None
    if table == "empty_events":
        row = physical.filter(pl.col("Table") == table).row(0, named=True)
        assert row["Ratio"] is None
        assert row["Physical Compression"] is None


def test_pipeline_with_readonly_requests(dataset):
    with closing(
        clickhouse_connect.get_client(
            host=dataset.clickhouse_base_url,
            port=dataset.clickhouse_port,
            username=dataset.clickhouse_user,
            password=dataset.clickhouse_password,
            secure=dataset.clickhouse_secure,
            database=dataset.clickhouse_database,
            settings={"readonly": 1},
        )
    ) as client:
        assert client.query(sql("readonly.sql")).first_row[0] == "1"
        task = ClickHouseDataExtraction(client=client, config=dataset)
        logical, physical = task.extract(*task.make_queries())
        assert len(logical) == len(LOGICAL_TABLES)
        assert len(physical) == len(PHYSICAL_TABLES)


def test_streamlit_catalog_filters_existing_databases_and_tables(dataset, monkeypatch):
    for key, value in dataset.model_dump().items():
        monkeypatch.setenv(key.upper(), str(value))
    app = AppTest.from_file(str(APP_PATH), default_timeout=30).run()
    assert not app.exception
    assert "system" in app.sidebar.multiselect(key="databases").options
    app.sidebar.multiselect(key="databases").set_value(
        [dataset.clickhouse_database]
    ).run()
    assert not app.exception
    frame = app.dataframe[0].value
    assert set(frame["Database"]) == {dataset.clickhouse_database}
    assert "ordinary_view" in set(frame["Table"])
    assert "memory_events" in set(frame["Table"])
    app.text_input(key="table_search").set_value("events_alpha").run()
    assert not app.exception
    assert app.dataframe[0].value["Table"].tolist() == ["events_alpha"]
    app.text_input(key="table_search").set_value("").run()
    app.sidebar.selectbox(key="engine_family").set_value("Other engines").run()
    assert not app.exception
    assert set(app.dataframe[0].value["Table"]) == {
        "ordinary_view",
        "materialized_without_suffix",
        "memory_events",
    }
    app.sidebar.radio(key="workspace").set_value("Tables").run()
    assert not app.exception
    assert len(app.dataframe) == 1


def test_streamlit_with_empty_database_selection(dataset, monkeypatch):
    for key, value in dataset.model_dump().items():
        monkeypatch.setenv(key.upper(), str(value))
    app = AppTest.from_file(str(APP_PATH), default_timeout=30).run()
    app.sidebar.multiselect(key="databases").set_value([]).run()
    assert not app.exception
    assert app.dataframe[0].value.empty
