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
        # st.error(f"Failed to connect to ClickHouse: {e}") # Removed
        raise


class ClickHouseDataExtraction:
    def __init__(self):
        self.client = create_clickhouse_client()

    def make_queries(self):
        """Generates a base SQL query for logical compression and a SQL query for physical compression."""
        try:
            base_logical_query = """
                SELECT name AS Table, total_rows AS `Row_Count`
                FROM system.tables
                WHERE database = 'default' AND name NOT ILIKE '%MV';
            """
            physical_query = """
                SELECT
                    table AS Table,
                    formatReadableSize(sum(data_compressed_bytes)) AS `Compressed Size`,
                    formatReadableSize(sum(data_uncompressed_bytes)) AS `Uncompressed Size`,
                    CASE
                        WHEN sum(data_uncompressed_bytes) = 0 THEN 0 -- Avoid division by zero
                        ELSE sum(data_compressed_bytes) / sum(data_uncompressed_bytes)
                    END AS `Ratio`,
                    CASE
                        WHEN sum(data_uncompressed_bytes) = 0 THEN 0 -- Avoid division by zero
                        ELSE (1 - (sum(data_compressed_bytes) / sum(data_uncompressed_bytes))) * 100
                    END AS `Physical Compression`
                FROM system.parts
                WHERE database = 'default' AND table NOT ILIKE '%MV' AND active
                GROUP BY table;
            """
            return base_logical_query, physical_query
        except Exception as e:
            # st.error(f"Failed to generate queries: {e}") # Removed
            raise

    def extract(self, base_logical_query: str, physical_query: str):
        """Executes the generated queries and collects the results."""
        try:
            physical_result_set = self.client.query(physical_query).result_set
            
            table_row_counts = self.client.query(base_logical_query).result_set
            
            logical_results = []
            for table_data in table_row_counts:
                table_name = table_data[0]
                row_count = table_data[1]
                
                event_count = 0  # Default event_count
                try:
                    # Ensure table_name is properly quoted if it contains special characters or keywords.
                    # ClickHouse typically uses backticks for quoting identifiers.
                    safe_table_name = f"`{table_name.replace('`', '``')}`"
                    event_count_query = f"SELECT SUM(`Event Count`) AS `Event_Count_Value` FROM default.{safe_table_name}"
                    event_count_result = self.client.query(event_count_query).result_set
                    if event_count_result and event_count_result[0][0] is not None:
                        event_count = int(event_count_result[0][0])
                except Exception as e:
                    # st.warning(f"Could not fetch Event Count for table {table_name}: {e}. Using 0.")
                    # Cannot use st.warning here, will proceed with event_count = 0
                    pass # event_count remains 0

                if event_count > 0:
                    ratio = row_count / event_count
                    logical_compression = (1 - ratio) * 100.0
                else: # Avoid division by zero if event_count is 0
                    ratio = 0.0
                    logical_compression = 0.0
                
                logical_results.append((table_name, event_count, row_count, ratio, logical_compression))
            
            return logical_results, physical_result_set
        except Exception as e:
            # st.error(f"Failed to extract data: {e}") # Removed
            raise


import re # For parsing readable size

def parse_readable_size(size_str: str) -> float:
    """Converts a human-readable size string (e.g., "10.0 KiB", "5.0 MiB") to bytes."""
    if not isinstance(size_str, str): # Handle potential non-string inputs if data is messy
        return 0.0

    size_str = size_str.strip()
    if not size_str or size_str == "0": # Handle empty string or plain "0"
        return 0.0

    units = {
        "bytes": 1,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "pb": 1024**5, # Petabytes, just in case
        "eb": 1024**6  # Exabytes, just in case
    }
    
    # Regex to capture the number and the unit, case-insensitive for unit
    match = re.match(r"(\d+(\.\d+)?)\s*([a-zA-Z]+)", size_str)
    if match:
        value_str, _, unit_str = match.groups()
        value = float(value_str)
        unit = unit_str.lower()
        
        if unit in units:
            return value * units[unit]
        # Handle cases like "KB", "MB" if formatReadableSize can output them (KiB, MiB are standard)
        # For simplicity, assuming KiB, MiB, etc. as per ClickHouse's formatReadableSize
        # Add more unit variations if necessary based on actual ClickHouse output
        
    # Fallback for "0 bytes" or if no unit is specified (assume bytes)
    if size_str.lower() == "0 bytes":
        return 0.0
    try:
        return float(size_str) # If it's just a number (e.g. from "0")
    except ValueError:
        # st.warning(f"Cannot parse size string: {size_str}. Returning 0.0")
        # Cannot use st.warning. Log or handle appropriately in a real app.
        return 0.0 # Default for unparseable formats

def format_bytes_for_display(num_bytes: float, suffix: str = "B") -> str:
    """Converts a number of bytes to a human-readable string (e.g., 10.0 KiB)."""
    if num_bytes is None:
        return "N/A"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unit}{suffix}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} Yi{suffix}" # Yottabytes

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
        schema={ # Initial schema, size columns will be transformed
            "Table": pl.Utf8,
            "Compressed Size": pl.Utf8, # Read as string first
            "Uncompressed Size": pl.Utf8, # Read as string first
            "Ratio": pl.Float64,
            "Physical Compression": pl.Float64, # Changed to Float64
        },
        orient="row",
    )

    # Transform size columns from string to numeric (bytes)
    df2 = df2.with_columns([
        pl.col("Compressed Size").apply(parse_readable_size, return_dtype=pl.Float64).alias("Compressed Size"),
        pl.col("Uncompressed Size").apply(parse_readable_size, return_dtype=pl.Float64).alias("Uncompressed Size")
    ])
    
    return df1, df2


def run():
    st.set_page_config(page_title="ClickHouse Compression Analysis", layout="wide")
    st.title("ClickHouse Storage & Compression Analysis")

    try:
        task = ClickHouseDataExtraction()
        logical_queries, physical_queries = task.make_queries()
        logical, physical = task.extract(logical_queries, physical_queries)
        df1, df2 = transform(logical, physical)
    
    except Exception as e:
        st.error(f"Failed to load or process data from ClickHouse: {e}")
        return # Stop further execution if an error occurs

    if df1.is_empty() or df1["Table"].is_empty():
        st.warning("No tables found or data is empty.")
        return

    # Filter section
    st.sidebar.header("Filter Options")
    table_list = df1["Table"].unique().to_list()
    table_filter = st.sidebar.multiselect(
        "Select Tables",
        table_list,
        default=table_list,
    )

    # Apply filter
    filtered_df1 = df1.filter(pl.col("Table").is_in(table_filter))
    filtered_df2 = df2.filter(pl.col("Table").is_in(table_filter))

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Logical Compression")
        if not filtered_df1.is_empty():
            filtered_df1_pandas = filtered_df1.to_pandas()
            # Apply conditional formatting & also format the numbers as strings with % for display
            # This is because Styler often works best when data is already in its final string form for display
            styled_df1 = filtered_df1_pandas.style.background_gradient(
                subset=['Logical_Compression'],
                cmap='RdYlGn', # Red-Yellow-Green: Green for high, Red for low
                vmin=0,  # Assuming compression is always >= 0
                vmax=100 # Assuming compression is <= 100
            ).format({
                "Logical_Compression": "{:.2f}%",
                "Ratio": "{:.2f}",
                # Event_Count and Row_Count formatting will be handled by column_config if possible
            })
            st.dataframe(
                styled_df1,
                use_container_width=True,
                column_config={
                    # "Ratio": st.column_config.NumberColumn(format="%.2f"), # Covered by style.format
                    # "Logical_Compression": st.column_config.NumberColumn(format="%.2f%%"), # Covered by style.format
                    "Event_Count": st.column_config.NumberColumn(format="%d events"),
                    "Row_Count": st.column_config.NumberColumn(format="%d rows"),
                }
            )
        else:
            st.dataframe(filtered_df1, use_container_width=True) # Display empty dataframe

    with col2:
        st.subheader("💾 Physical Compression")
        # Prepare df2 for display with formatted byte sizes
        if not filtered_df2.is_empty():
            display_df2 = filtered_df2.with_columns([
                pl.col("Compressed Size").apply(format_bytes_for_display).alias("Compressed Size Display"),
                pl.col("Uncompressed Size").apply(format_bytes_for_display).alias("Uncompressed Size Display")
            ])
            # Select and reorder columns for display if needed, or hide originals
            columns_to_display_df2 = ["Table", "Compressed Size Display", "Uncompressed Size Display", "Ratio", "Physical Compression"]
            # Ensure all selected columns exist
            columns_to_display_df2 = [col for col in columns_to_display_df2 if col in display_df2.columns]
            
            display_df2_pandas = display_df2.select(columns_to_display_df2).to_pandas()
            styled_df2 = display_df2_pandas.style.background_gradient(
                subset=['Physical Compression'],
                cmap='RdYlGn',
                vmin=0,
                vmax=100
            ).format({
                "Physical Compression": "{:.2f}%",
                "Ratio": "{:.2f}",
            })

        else: # Handle empty filtered_df2
            # Create an empty DF with expected display columns for styling compatibility
            empty_schema_for_styling = {
                "Table": [], 
                "Compressed Size Display": [], 
                "Uncompressed Size Display": [], 
                "Ratio": [], 
                "Physical Compression": []
            }
            display_df2_pandas = pl.DataFrame(empty_schema_for_styling, schema={
                "Table": pl.Utf8, 
                "Compressed Size Display": pl.Utf8, 
                "Uncompressed Size Display": pl.Utf8, 
                "Ratio": pl.Float64, 
                "Physical Compression": pl.Float64
            }).to_pandas()
            styled_df2 = display_df2_pandas.style # Apply empty style
            columns_to_display_df2 = list(display_df2_pandas.columns)


        st.dataframe(
            styled_df2, # Display only the selected/formatted columns
            use_container_width=True,
            column_config={
                # "Ratio": st.column_config.NumberColumn(format="%.2f"), # Covered by style.format
                # "Physical Compression": st.column_config.NumberColumn(format="%.2f%%"), # Covered by style.format
            }
        )

    # Add Bar Chart for Physical Compression
    if not filtered_df2.is_empty():
        st.divider()
        st.subheader("📈 Physical Compression Ratios by Table")
        # Use the original filtered_df2 which has numeric "Physical Compression"
        chart_data_physical = filtered_df2.select(["Table", "Physical Compression"]).to_pandas().set_index("Table")
        # Ensure the column name matches for st.bar_chart
        chart_data_physical = chart_data_physical.rename(columns={"Physical Compression": "Compression (%)"})

        if not chart_data_physical.empty:
            st.bar_chart(chart_data_physical["Compression (%)"], height=min(max(len(chart_data_physical) * 35, 200), 600)) # Dynamic height with min/max
        else:
            st.caption("No data to display in chart.")
    else:
        # Optionally, display a message if there's no data for the chart
        st.caption("No physical compression data available for chart.")


if __name__ == "__main__":
            column_config={
                "Ratio": st.column_config.NumberColumn(format="%.2f"),
                "Logical_Compression": st.column_config.NumberColumn(format="%.2f%%"),
                "Event_Count": st.column_config.NumberColumn(format="%d events"),
                "Row_Count": st.column_config.NumberColumn(format="%d rows"),
            }
        )

    with col2:
        st.subheader("Physical Compression")
        # Prepare df2 for display with formatted byte sizes
        if not filtered_df2.is_empty():
            display_df2 = filtered_df2.with_columns([
                pl.col("Compressed Size").apply(format_bytes_for_display).alias("Compressed Size Display"),
                pl.col("Uncompressed Size").apply(format_bytes_for_display).alias("Uncompressed Size Display")
            ])
            # Select and reorder columns for display if needed, or hide originals
            columns_to_display_df2 = ["Table", "Compressed Size Display", "Uncompressed Size Display", "Ratio", "Physical Compression"]
            # Ensure all selected columns exist
            columns_to_display_df2 = [col for col in columns_to_display_df2 if col in display_df2.columns]

        else: # Handle empty filtered_df2
            display_df2 = pl.DataFrame(schema={ # Create an empty DF with expected display columns
                "Table": pl.Utf8, 
                "Compressed Size Display": pl.Utf8, 
                "Uncompressed Size Display": pl.Utf8, 
                "Ratio": pl.Float64, 
                "Physical Compression": pl.Float64
            })
            columns_to_display_df2 = list(display_df2.columns)


        st.dataframe(
            display_df2.select(columns_to_display_df2), # Display only the selected/formatted columns
            use_container_width=True,
            column_config={
                "Ratio": st.column_config.NumberColumn(format="%.2f"),
                "Physical Compression": st.column_config.NumberColumn(format="%.2f%%"),
            }
        )


if __name__ == "__main__":
    run()
