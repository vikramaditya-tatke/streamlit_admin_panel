from typing import List, Any, Sequence

import polars as pl
import clickhouse_connect
from clickhouse_connect.driver import Client
import streamlit as st
from settings import settings


def create_clickhouse_client() -> Client:
    """Initializes and returns a ClickHouse client."""
    try:
        client: Client = clickhouse_connect.get_client(
            host=settings.clickhouse_base_url,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            port=settings.clickhouse_port,
            secure=True,
            compress=settings.clickhouse_compression_protocol,
            settings={"insert_deduplicate": False},
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        raise


class ClickHouseDataExtraction:
    def __init__(self):
        self.client = create_clickhouse_client()

    def make_queries(self):
        """Generates SQL queries for logical compression from ClickHouse tables."""
        try:
            logical_query_result = self.client.command(
                """
                WITH user_tables AS (
                    SELECT
                        DISTINCT name as Table
                    FROM
                        system.tables
                    WHERE
                        database = 'default'
                        AND name NOT ILIKE '%MV'
                )
                SELECT
                    'SELECT ''' || Table || ''' AS Table, ' || 'SUM(`Event Count`) AS `Event Count`, ' || 'COUNT(*) AS `Row Count`, ' || '(`Row Count` / `Event Count`) AS `Ratio`, ' || '((1 - (`Row Count` / `Event Count`)) * 100) AS `Logical Compression`' || 'FROM default.' || Table || ';' AS generated_sql
                FROM
                    user_tables;
                """
            )
            physical_query_result = self.client.command(
                """
                WITH user_tables AS (
                    SELECT
                        DISTINCT name as Table
                    FROM
                        system.tables
                    WHERE
                        database = 'default'
                        AND name NOT ILIKE '%MV'
                )
                SELECT
                    'SELECT ''' || Table || ''' AS Table, ' || 'formatReadableSize(SUM(data_compressed_bytes)) AS `Compressed Size`, ' || 'formatReadableSize(SUM(data_uncompressed_bytes)) AS `Uncompressed Size`, ' || '(SUM(data_compressed_bytes) / SUM(data_uncompressed_bytes)) AS `Ratio`, ' || '((1 - (SUM(data_compressed_bytes) / SUM(data_uncompressed_bytes))) * 100) AS `Physical Compression`' || 'FROM system.parts' || ';' AS generated_sql
                FROM user_tables;
                """
            )
            logical_queries = [row for row in logical_query_result.replace("\\'", "'").replace("\n", "").split(";")]
            physical_queries = [row for row in physical_query_result.replace("\\'", "'").replace("\n", "").split(";")]
            return logical_queries, physical_queries
        except Exception as e:
            st.error(f"Failed to generate queries: {e}")
            raise

    def extract(self, logical_queries, physical_queries: list[str]):
        """Executes the generated queries and collects the results."""
        logical = []
        for query in logical_queries[:-1]:
            result = self.client.query(query).result_set
            logical.extend(result)

        physical = []
        for query in physical_queries[:-1]:
            result = self.client.query(query).result_set
            physical.extend(result)

        return logical, physical


def transform(logical: list[Sequence[Any]], physical: list[Sequence[Any]]):
    """Transforms the raw query results into a Polars DataFrame."""
    df1 = pl.DataFrame(
        logical,
        schema={
            "Table": pl.Utf8,
            "Event_Count": pl.Int64,
            "Row_Count": pl.Int64,
            "Ratio": pl.Float64,
            "Logical_Compression": pl.Float64,
        },
        orient="row",
    )
    df2 = pl.DataFrame(
        physical,
        schema={
            "Table": pl.Utf8,
            "Compressed Size": pl.Utf8,
            "Uncompressed Size": pl.Utf8,
            "Ratio": pl.Float64,
            "Physical Compression": pl.Utf8,
        },
        orient="row",
    )
    return df1, df2


def run():
    task = ClickHouseDataExtraction()
    logical_queries, physical_queries = task.make_queries()
    logical, physical = task.extract(logical_queries, physical_queries)
    df1, df2 = transform(logical, physical)
    # Streamlit app for displaying the results
    st.title("ClickHouse Logical Compression Analysis")

    # Filter section
    st.sidebar.header("Filter Options")
    table_filter = st.sidebar.multiselect(
        "Select Tables",
        df1["Table"].unique().to_list(),
        default=df1["Table"].unique().to_list(),
    )

    # Apply filter
    filtered_df1 = df1.filter(pl.col("Table").is_in(table_filter))
    filtered_df2 = df2.filter(pl.col("Table").is_in(table_filter))

    # Display both DataFrames
    st.write("Logical Compression Results:")
    st.dataframe(filtered_df1)

    st.write("Physical Compression Results:")
    st.dataframe(filtered_df2)


if __name__ == "__main__":
    run()
