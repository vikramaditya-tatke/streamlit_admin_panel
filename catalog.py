"""Read the live ClickHouse catalog without scanning application tables."""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from clickhouse_connect.driver import Client

SQL_DIRECTORY = Path(__file__).parent / "sql"
CATALOG_QUERY = (SQL_DIRECTORY / "catalog.sql").read_text(encoding="utf-8")
STORAGE_QUERY = (SQL_DIRECTORY / "storage_summary.sql").read_text(encoding="utf-8")
VERSION_QUERY = (SQL_DIRECTORY / "server_version.sql").read_text(encoding="utf-8")


@dataclass(frozen=True)
class CatalogSnapshot:
    tables: pl.DataFrame
    version: str
    captured_at: datetime


def load_catalog(client: Client) -> CatalogSnapshot:
    catalog = pl.DataFrame(
        client.query(CATALOG_QUERY).result_rows,
        schema={
            "Database": pl.String,
            "Table": pl.String,
            "Engine": pl.String,
            "Rows": pl.UInt64,
            "Disk bytes": pl.UInt64,
            "Has event count": pl.Boolean,
        },
        orient="row",
    )
    storage = pl.DataFrame(
        client.query(STORAGE_QUERY).result_rows,
        schema={
            "Database": pl.String,
            "Table": pl.String,
            "Compressed bytes": pl.UInt64,
            "Uncompressed bytes": pl.UInt64,
            "Active parts": pl.UInt64,
        },
        orient="row",
    )
    tables = (
        catalog.join(storage, on=["Database", "Table"], how="left", validate="1:1")
        .with_columns(
            [
                pl.when(pl.col("Engine").str.ends_with("MergeTree"))
                .then(pl.col(column).fill_null(0))
                .otherwise(None)
                .alias(column)
                for column in ["Compressed bytes", "Uncompressed bytes", "Active parts"]
            ]
        )
        .with_columns(
            pl.when(pl.col("Uncompressed bytes") > 0)
            .then((1 - pl.col("Compressed bytes") / pl.col("Uncompressed bytes")) * 100)
            .otherwise(None)
            .alias("Compression (%)"),
            pl.concat_str(["Database", "Table"], separator=".").alias("Relation"),
        )
    )
    return CatalogSnapshot(
        tables=tables,
        version=client.command(VERSION_QUERY),
        captured_at=datetime.now(UTC),
    )


def filter_catalog(
    tables: pl.DataFrame,
    databases: list[str],
    engine_family: str = "All engines",
    search: str = "",
) -> pl.DataFrame:
    result = tables.filter(pl.col("Database").is_in(databases))
    if engine_family == "MergeTree family":
        result = result.filter(pl.col("Engine").str.ends_with("MergeTree"))
    elif engine_family == "Other engines":
        result = result.filter(~pl.col("Engine").str.ends_with("MergeTree"))
    if search:
        result = result.filter(
            pl.col("Relation")
            .str.to_lowercase()
            .str.contains(search.lower(), literal=True)
        )
    return result


def metadata_total(values: pl.Series) -> int | None:
    if values.len() and values.count() == 0:
        return None
    return values.sum()


def summarize(tables: pl.DataFrame) -> dict[str, int | float | None]:
    compressed = metadata_total(tables["Compressed bytes"])
    uncompressed = metadata_total(tables["Uncompressed bytes"])
    return {
        "tables": tables.height,
        "databases": tables["Database"].n_unique(),
        "rows": metadata_total(tables["Rows"]),
        "compressed": compressed,
        "uncompressed": uncompressed,
        "compression": (1 - compressed / uncompressed) * 100 if uncompressed else None,
    }
