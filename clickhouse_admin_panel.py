from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import clickhouse_connect
import polars as pl
import streamlit as st
from clickhouse_connect.driver import Client

from settings import Settings

SQL_DIRECTORY = Path(__file__).parent / "sql"
TABLES_QUERY = (SQL_DIRECTORY / "discover_tables.sql").read_text(encoding="utf-8")
LOGICAL_QUERY = (SQL_DIRECTORY / "logical_compression.sql").read_text(encoding="utf-8")
PHYSICAL_QUERY = (SQL_DIRECTORY / "physical_compression.sql").read_text(
    encoding="utf-8"
)


@dataclass(frozen=True)
class CompressionQuery:
    sql: str
    parameters: dict[str, str]


def quote_identifier(value: str) -> str:
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def create_clickhouse_client(config: Settings | None = None) -> Client:
    """Create a client suitable for a read-only dashboard user."""
    config = config or Settings()
    return clickhouse_connect.get_client(
        host=config.clickhouse_base_url,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        port=config.clickhouse_port,
        database=config.clickhouse_database,
        secure=config.clickhouse_secure,
        compress=config.clickhouse_compression_protocol,
    )


class ClickHouseDataExtraction:
    def __init__(self, client: Client | None = None, config: Settings | None = None):
        self.config = config or Settings()
        self.client = (
            client if client is not None else create_clickhouse_client(self.config)
        )

    def make_queries(self) -> tuple[list[CompressionQuery], list[CompressionQuery]]:
        """Discover stored tables and prepare each applicable metric query."""
        tables = self.client.query(
            TABLES_QUERY,
            parameters={"database": self.config.clickhouse_database},
        ).result_rows
        logical_queries = []
        physical_queries = []
        for table, has_event_count in tables:
            parameters = {"database": self.config.clickhouse_database, "table": table}
            if has_event_count:
                logical_sql = LOGICAL_QUERY.replace(
                    "{database:Identifier}",
                    quote_identifier(self.config.clickhouse_database),
                ).replace("{table:Identifier}", quote_identifier(table))
                logical_queries.append(CompressionQuery(logical_sql, parameters))
            physical_queries.append(CompressionQuery(PHYSICAL_QUERY, parameters))
        return logical_queries, physical_queries

    def extract(
        self,
        logical_queries: Sequence[CompressionQuery],
        physical_queries: Sequence[CompressionQuery],
    ) -> tuple[list[Sequence[Any]], list[Sequence[Any]]]:
        """Execute every query, preserving rows and native result types."""
        logical = []
        for query in logical_queries:
            logical.extend(
                self.client.query(query.sql, parameters=query.parameters).result_rows
            )
        physical = []
        for query in physical_queries:
            physical.extend(
                self.client.query(query.sql, parameters=query.parameters).result_rows
            )
        return logical, physical


def transform(logical: list[Sequence[Any]], physical: list[Sequence[Any]]):
    """Create typed frames, including when discovery returns no tables."""
    df1 = pl.DataFrame(
        logical,
        schema={
            "Table": pl.String,
            "Event_Count": pl.UInt64,
            "Row_Count": pl.UInt64,
            "Ratio": pl.Float64,
            "Logical_Compression": pl.Float64,
        },
        orient="row",
    )
    df2 = pl.DataFrame(
        physical,
        schema={
            "Table": pl.String,
            "Compressed Size": pl.String,
            "Uncompressed Size": pl.String,
            "Ratio": pl.Float64,
            "Physical Compression": pl.Float64,
        },
        orient="row",
    )
    return df1, df2


def run():
    st.title("ClickHouse Compression Analysis")
    config = Settings()
    with closing(create_clickhouse_client(config)) as client:
        task = ClickHouseDataExtraction(client=client, config=config)
        logical, physical = task.extract(*task.make_queries())
        df1, df2 = transform(logical, physical)

    if df2.is_empty():
        st.info("No eligible MergeTree tables were found.")
    elif df1.is_empty():
        st.info(
            "Logical compression requires an Event Count column; "
            "these tables only have physical metrics."
        )
    st.caption(
        "Logical compression = (1 − stored rows / total Event Count) × 100. "
        "Physical compression uses active parts on the connected server. "
        "A zero denominator is shown as an unavailable value."
    )
    st.sidebar.header("Filter Options")
    tables = df2["Table"].to_list()
    table_filter = st.sidebar.multiselect("Select Tables", tables, default=tables)
    filtered_df1 = df1.filter(pl.col("Table").is_in(table_filter))
    filtered_df2 = df2.filter(pl.col("Table").is_in(table_filter))

    st.write("Logical Compression Results:")
    st.dataframe(filtered_df1)
    st.write("Physical Compression Results:")
    st.dataframe(filtered_df2)


if __name__ == "__main__":
    run()
