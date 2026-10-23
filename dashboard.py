"""Themed Streamlit views of the live system.tables catalog."""

from html import escape
from pathlib import Path

import altair as alt
import polars as pl
import streamlit as st

from catalog import CatalogSnapshot, filter_catalog, summarize

STYLESHEET = Path(__file__).parent / "ui" / "dashboard.css"
PALETTE = ["#292822", "#A59A82", "#D2C5AB", "#716B5C", "#BFB8A9", "#5F5D52"]


def format_bytes(value: float | None) -> str:
    if value is None:
        return "—"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    index = 0
    while abs(value) >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return f"{value:,.1f} {units[index]}"


def heading(title: str):
    st.html(f'<div class="panel-heading"><h2>{escape(title)}</h2></div>')


def render_sidebar(snapshot: CatalogSnapshot):
    with st.sidebar:
        st.html('<div class="brand">ClickHouse</div>')
        view = st.radio(
            "Workspace",
            ["Overview", "Tables"],
            label_visibility="collapsed",
            key="workspace",
        )
        st.divider()
        databases = snapshot.tables["Database"].unique().sort().to_list()
        selected = st.multiselect(
            "Databases", databases, default=databases, key="databases"
        )
        engine_family = st.selectbox(
            "Engine family",
            ["All engines", "MergeTree family", "Other engines"],
            key="engine_family",
        )
    return view, selected, engine_family


def render_metrics(tables: pl.DataFrame):
    stats = summarize(tables)
    compression = stats["compression"]
    compression_text = f"{compression:,.1f}%" if compression is not None else "—"
    cards = [
        ("Tables", f"{stats['tables']:,}"),
        ("Compressed data", format_bytes(stats["compressed"])),
        ("Space saved", compression_text),
        ("Known rows", f"{stats['rows']:,}" if stats["rows"] is not None else "—"),
    ]
    markup = "".join(
        f'<div class="kpi"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div></div>'
        for label, value in cards
    )
    st.html(f'<div class="kpi-grid">{markup}</div>')


def chart_style(chart):
    return (
        chart.configure_view(stroke=None)
        .configure_axis(
            labelColor="#7C7466",
            titleColor="#7C7466",
            domain=False,
            gridColor="#EBE5D8",
            labelFontSize=11,
            labelFont="Arial",
            titleFont="Arial",
            titleFontSize=10,
            ticks=False,
            labelPadding=8,
            titlePadding=12,
        )
        .configure_legend(
            labelColor="#7C7466",
            labelFontSize=11,
            labelFont="Arial",
            titleFont="Arial",
            title=None,
            orient="top",
            symbolType="circle",
            symbolSize=60,
        )
    )


def render_footprint(tables: pl.DataFrame):
    heading("Storage")
    measure = st.segmented_control(
        "Chart measure",
        ["Storage", "Rows"],
        default="Storage",
        required=True,
        label_visibility="collapsed",
        key="chart_measure",
    )
    if measure == "Storage":
        data = (
            tables.filter(pl.col("Uncompressed bytes") > 0)
            .sort("Uncompressed bytes", descending=True)
            .head(6)
            .select(
                "Relation",
                (pl.col("Compressed bytes") / 1024**2).alias("Compressed"),
                (pl.col("Uncompressed bytes") / 1024**2).alias("Uncompressed"),
            )
            .unpivot(index="Relation", variable_name="Storage", value_name="MiB")
        )
        if data.is_empty():
            st.write("—")
            return
        chart = (
            alt.Chart(alt.Data(values=data.to_dicts()))
            .mark_bar(
                cornerRadiusEnd=4,
                height=9,
            )
            .encode(
                y=alt.Y(
                    "Relation:N",
                    sort=data["Relation"].unique(maintain_order=True).to_list(),
                    title=None,
                    axis=alt.Axis(labelLimit=170),
                ),
                yOffset=alt.YOffset("Storage:N", sort=["Uncompressed", "Compressed"]),
                x=alt.X("MiB:Q", title="MiB", axis=alt.Axis(tickCount=4, format="~s")),
                color=alt.Color(
                    "Storage:N",
                    scale=alt.Scale(
                        domain=["Uncompressed", "Compressed"],
                        range=["#DED4C1", "#292822"],
                    ),
                ),
                tooltip=[
                    "Relation:N",
                    "Storage:N",
                    alt.Tooltip("MiB:Q", format=",.3f"),
                ],
            )
            .properties(height=205)
        )
    else:
        data = tables.filter(pl.col("Rows") > 0).sort("Rows", descending=True).head(6)
        if data.is_empty():
            st.write("—")
            return
        chart = (
            alt.Chart(alt.Data(values=data.select("Relation", "Rows").to_dicts()))
            .mark_bar(
                color="#292822",
                cornerRadiusEnd=4,
                height=16,
            )
            .encode(
                y=alt.Y(
                    "Relation:N", sort="-x", title=None, axis=alt.Axis(labelLimit=170)
                ),
                x=alt.X(
                    "Rows:Q", title="Known stored rows", axis=alt.Axis(format="~s")
                ),
                tooltip=["Relation:N", alt.Tooltip("Rows:Q", format=",")],
            )
            .properties(height=205)
        )
    st.altair_chart(chart_style(chart), theme=None, width="stretch")


def render_allocation(tables: pl.DataFrame):
    heading("Databases")
    data = (
        tables.group_by("Database")
        .agg(pl.col("Compressed bytes").sum())
        .filter(pl.col("Compressed bytes") > 0)
        .sort("Compressed bytes", descending=True)
    )
    total = data["Compressed bytes"].sum()
    if not total:
        st.write("—")
        return
    names = data["Database"].to_list()
    colors = [PALETTE[index % len(PALETTE)] for index in range(len(names))]
    chart = (
        alt.Chart(alt.Data(values=data.to_dicts()))
        .mark_arc(
            innerRadius=64,
            outerRadius=88,
            cornerRadius=3,
            padAngle=0.025,
        )
        .encode(
            theta=alt.Theta("Compressed bytes:Q", stack=True),
            color=alt.Color(
                "Database:N", scale=alt.Scale(domain=names, range=colors), legend=None
            ),
            tooltip=[
                "Database:N",
                alt.Tooltip("Compressed bytes:Q", title="Compressed bytes", format=","),
            ],
        )
    )
    label = (
        alt.Chart(alt.Data(values=[{"label": format_bytes(total)}]))
        .mark_text(
            fontSize=21,
            font="Arial",
            fontWeight=600,
            color="#302D26",
            dy=-3,
        )
        .encode(text="label:N")
    )
    st.altair_chart(
        (chart + label)
        .properties(height=188, background="#FFFCF5")
        .configure_view(stroke=None),
        theme=None,
        width="stretch",
    )
    legend = "".join(
        '<div class="legend-row"><div class="legend-label">'
        f'<span class="legend-dot" style="background:{colors[index]}"></span>'
        f'{escape(row["Database"])}</div><span class="legend-value">'
        f"{row['Compressed bytes'] / total * 100:.1f}%</span></div>"
        for index, row in enumerate(data.head(4).to_dicts())
    )
    st.html(legend)


def render_explorer(tables: pl.DataFrame):
    with st.container(key="explorer"):
        heading("Tables")
        search_column, sort_column = st.columns([3, 1])
        with search_column:
            search = st.text_input(
                "Find a table",
                placeholder="Search tables or databases…",
                icon=":material/search:",
                label_visibility="collapsed",
                key="table_search",
            )
        with sort_column:
            sort = st.selectbox(
                "Sort by",
                ["Storage ↓", "Rows ↓", "Name A–Z"],
                label_visibility="collapsed",
            )
        filtered = filter_catalog(
            tables, tables["Database"].unique().to_list(), search=search
        )
        sort_column = {
            "Storage ↓": "Compressed bytes",
            "Rows ↓": "Rows",
            "Name A–Z": "Relation",
        }[sort]
        filtered = filtered.sort(
            sort_column, descending=sort != "Name A–Z", nulls_last=True
        )
        st.download_button(
            "Export CSV",
            filtered.write_csv(),
            "clickhouse-catalog.csv",
            "text/csv",
            icon=":material/download:",
            on_click="ignore",
        )
        st.dataframe(
            filtered,
            column_order=[
                "Table",
                "Database",
                "Engine",
                "Rows",
                "Compressed bytes",
                "Compression (%)",
                "Active parts",
            ],
            column_config={
                "Table": st.column_config.TextColumn("Table", width=210, pinned=True),
                "Database": st.column_config.TextColumn("Database", width=150),
                "Engine": st.column_config.TextColumn("Engine", width=135),
                "Rows": st.column_config.NumberColumn(
                    "Rows", format="localized", width=95
                ),
                "Compressed bytes": st.column_config.NumberColumn(
                    "Compressed", format="bytes", width=90
                ),
                "Compression (%)": st.column_config.NumberColumn(
                    "Saved", format="%.1f%%", width=75
                ),
                "Active parts": st.column_config.NumberColumn("Parts", width=60),
            },
            hide_index=True,
            width="stretch",
            height=338,
            row_height=40,
            key="catalog_table",
        )


def render_dashboard(snapshot: CatalogSnapshot):
    st.html(STYLESHEET)
    view, databases, engine_family = render_sidebar(snapshot)
    tables = filter_catalog(snapshot.tables, databases, engine_family)
    title = "Overview" if view == "Overview" else "Tables"
    hero, refresh = st.columns([5, 1], vertical_alignment="center")
    with hero:
        st.html(f'<div class="hero"><h1>{title}</h1></div>')
    with refresh:
        st.button("Refresh", icon=":material/refresh:", width="stretch")
    render_metrics(tables)
    if view == "Overview":
        footprint, allocation = st.columns([1.65, 1], gap="medium")
        with footprint, st.container(key="footprint"):
            render_footprint(tables)
        with allocation, st.container(key="allocation"):
            render_allocation(tables)
    render_explorer(tables)
