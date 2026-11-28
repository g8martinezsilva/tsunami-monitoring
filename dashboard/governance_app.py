"""Data Governance Dashboard for the published Gold indicators."""

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
from dash.exceptions import PreventUpdate

from dashboard import theme
from dashboard.gold import GoldCache

OBSERVATION_TOPICS = ("earthquakes", "tsunami_events", "tsunami_bulletins")
SOURCE_NAMES = {
    "noaa_hazel": "NOAA Historical Tsunami Database",
    "usgs_fdsn": "USGS Earthquake Catalog (FDSN)",
    "usgs_realtime": "USGS Real-time Feed",
    "ntwc": "NTWC — National Tsunami Warning Center",
    "ptwc": "PTWC — Pacific Tsunami Warning Center",
}
STATUS_LABELS = {"warning": "Warning", "review": "Review", "ok": "OK", "expected": "Expected", "n/a": "N/A"}
QUALITY_COLORS = {**theme.STATUS_COLORS, "warning": "#C2352F"}
QUALITY_CHECKS = (
    ("null_rate", "Null rate", "null_rate_pct", "field", "≤", "%"),
    ("outlier_rate", "IQR outlier rate", "outlier_rate_pct", "field", "≤", "%"),
    ("schema_compliance", "Schema compliance", "schema_compliance_pct", "silver_topic", "≥", "%"),
    ("matching", "Matching coverage", "coverage_pct", "relation_type", "≥", "%"),
    ("pipeline_state", "Watermark age", "watermark_age_hours", None, "≤", " h"),
    ("orchestration", "Run compliance", "run_compliance_pct", "dag_id", "≥", "%"),
)
BOX_VARIABLES = {
    "magnitude": ("Earthquake Magnitude", "Magnitude", "earthquakes"),
    "depth_km": ("Earthquake Depth", "Depth (km)", "earthquakes"),
    "max_water_height": ("Maximum Water Height", "Water height (m)", "tsunami_events"),
    "num_runups": ("Number of Runups", "Runups (count)", "tsunami_events"),
}

cache = GoldCache(include_storytelling=False)


def source_name(source):
    return SOURCE_NAMES.get(source, source)


def percentage(numerator, denominator):
    return 100 * numerator / denominator if pd.notna(denominator) and denominator > 0 else None


def rate(value):
    return theme.fmt(value, 2, "%")


def status_badge(status):
    if pd.isna(status):
        return "—"
    return html.Span(STATUS_LABELS[status], className="status-badge",
                     style={"--status-color": theme.STATUS_COLORS[status]})


def observations(pair):
    return pair.governance.loc[pair.governance["topic"].isin(OBSERVATION_TOPICS)]


def common_threshold(rows):
    limits = rows["threshold"].dropna().unique()
    return limits[0] if len(limits) == 1 else None


def missing_value_kpi(counts, missing):
    excluded = missing["source"].eq("usgs_fdsn") & missing["field"].eq("tsunami_bulletin_flag")
    missing = missing.loc[~excluded.fillna(False)]
    # Gold checks every observation for each field. Source identifies its observation topic
    # in this catalog. A repeated source across topics would make the denominator ambiguous.
    by_source = counts.drop_duplicates("source", keep=False).set_index("source")["unique_observations"]
    evaluated = missing["source"].map(by_source)
    total = evaluated.sum(min_count=1)
    if pd.notna(total) and total > 0 and evaluated.notna().all() and missing["null_count"].notna().all():
        return "Overall Null Rate", percentage(missing["null_count"].sum(), total), (
            f"{theme.fmt(missing['null_count'].sum())} missing of {theme.fmt(total)} field values."
        )
    return "Mean Field Null Rate", missing["null_rate_pct"].mean(), f"Unweighted mean across fields and sources."


def kpi_cards(pair):
    counts = observations(pair)
    total = counts["unique_observations"].sum(min_count=1)
    processed = counts["rows_processed"].sum(min_count=1)
    repeated = counts["repeated_rows"].sum(min_count=1)
    label, null_rate, null_detail = missing_value_kpi(counts, pair.topic("null_rate"))

    schema = pair.topic("schema_compliance")
    checked = schema["schema_rows"].sum(min_count=1)
    compliant = schema["schema_compliant"].sum(min_count=1)
    threshold = common_threshold(schema)
    schema_detail = f"{theme.fmt(compliant)} of {theme.fmt(checked)} observations" if pd.notna(checked) else (
        "Schema indicator unavailable"
    )
    if threshold is not None:
        schema_detail += f" · Target ≥ {threshold:g}%"
    cards = (
        theme.kpi_card("Unique Observations", theme.fmt(total), "Distinct observations by source"),
        theme.kpi_card(label, rate(null_rate), null_detail),
        theme.kpi_card("Duplicate Rate", rate(percentage(repeated, processed)),
                       f"{theme.fmt(repeated)} of {theme.fmt(processed)} rows. Repeat captures are consolidated."
                       if pd.notna(processed) else "Processing counts unavailable"),
        theme.kpi_card("Schema Compliance", rate(percentage(compliant, checked)), schema_detail),
    )
    return [dbc.Col(card, xs=12, sm=6, lg=3) for card in cards]


def field_rates(rows, title, column, axis, counts):
    rows = rows.dropna(subset=[column]).sort_values(column, ascending=False, kind="stable").copy()
    if rows.empty:
        return theme.empty_figure(title, "No field rates available for the selected sources.")

    rows["category"] = rows["field"] + " · " + rows["source"]
    rows["label"] = [
        field.replace("_", " ") + (f" · {STATUS_LABELS[status]}" if status in {"expected", "warning", "review"} else "")
        + "<br>" + source_name(source)
        for field, source, status in rows[["field", "source", "status"]].fillna("").itertuples(index=False, name=None)
    ]
    figure = go.Figure()
    for status, group in rows.groupby(rows["status"].fillna(""), sort=False):
        label = STATUS_LABELS.get(status, "Status unavailable")
        color = QUALITY_COLORS.get(status, theme.GREY)
        if column == "outlier_rate_pct" and status == "review":
            color = "#F4D03F"
        hover = [
            "<br>".join([f"{source_name(row['source'])} · {row['field']}",
                         *(f"{name}: {theme.fmt(row[key])}" for key, name in counts),
                         f"Status: {label}",
                         f"Threshold: {rate(row['threshold'])}"])
            for row in group.to_dict("records")
        ]
        figure.add_bar(
            name=label, x=group[column].astype(float), y=group["category"], orientation="h",
            marker={"color": color,
                    "pattern": {"shape": "/" if status == "expected" else ""}},
            width=0.56,
            text=group[column].map(rate), textposition="outside", cliponaxis=False,
            hovertext=hover, hovertemplate="%{hovertext}<br>%{x:.2f}%<extra></extra>",
        )
    threshold = common_threshold(rows)
    upper = rows[column].max()
    if threshold is not None:
        upper = max(upper, threshold)
        figure.add_vline(x=threshold, line_dash="dash", line_color=theme.GREY, line_width=1)
        figure.add_annotation(x=threshold, xref="x", y=1, yref="paper", yanchor="bottom", yshift=6,
                              xanchor="right" if threshold > upper / 2 else "left",
                              text=f"Target ≤ {threshold:g}%", showarrow=False, font={"size": 11})
    figure.update_yaxes(categoryorder="array", categoryarray=rows["category"].tolist(), autorange="reversed",
                        tickmode="array", tickvals=rows["category"].tolist(), ticktext=rows["label"].tolist(),
                        automargin=True, ticklabelstandoff=4, tickfont={"size": 11})
    figure.update_xaxes(ticksuffix="%", range=[0, max(upper * 1.08, 1)], automargin=True)
    theme.style(figure, title, x=axis, height=max(330, 146 + 32 * len(rows)))
    return figure.update_layout(barmode="overlay", margin={"l": 8, "r": 50, "t": 48, "b": 96},
                                legend={"x": 0.5, "xref": "container", "xanchor": "center",
                                        "y": 0, "yref": "container", "yanchor": "bottom",
                                        "font": {"size": 11}})


def daily_volume_chart(rows):
    title = "Processed Rows Over Time"
    rows = rows.dropna(subset=["period", "volume_rows"]).sort_values("period")
    if rows.empty:
        return theme.empty_figure(title, "No dated volume observations for the selected sources.")

    trend = go.Figure()
    for source, group in rows.groupby("source", sort=False):
        color = theme.SOURCE_COLORS.get(source, theme.GREY)
        trend.add_scatter(
            name=source_name(source), x=group["period"].astype(str), y=group["volume_rows"].astype(float),
            mode="lines+markers" if group["period"].nunique() > 1 else "markers",
            line={"color": color}, marker={"color": color, "size": 8},
            customdata=group["new_observations"].map(theme.fmt).tolist(),
            hovertemplate="Processed rows: %{y:,.0f}<br>New observations: %{customdata}<extra>%{fullData.name}</extra>",
        )

    theme.style(trend, title, x="Capture day (UTC)", y="Processed rows (log scale)", height=415)
    dates = rows["period"].astype(str).unique().tolist()
    trend.update_xaxes(type="date", tickformat="%Y-%m-%d", tickvals=dates if len(dates) <= 5 else None,
                       ticklabeloverflow="allow", automargin=True)
    trend.update_yaxes(type="log", dtick=1, automargin=True)
    return trend.update_layout(hovermode="x unified", margin={"l": 50, "r": 36, "t": 32, "b": 116},
                               legend={"yanchor": "top", "y": -0.23, "font": {"size": 11}})


def source_charts(pair, source):
    def rows(topic):
        frame = pair.topic(topic)
        return frame.loc[frame["source"].eq(source)] if source else frame

    missing = rows("null_rate")
    excluded = missing["source"].eq("usgs_fdsn") & missing["field"].eq("tsunami_bulletin_flag")
    missing = missing.loc[~excluded.fillna(False)]
    positive = missing.loc[missing["null_count"].gt(0)]
    missing_chart = (
        theme.empty_figure("Missing Values by Field and Source", "No missing values detected in the evaluated fields.")
        if positive.empty and missing["null_count"].notna().any()
        else field_rates(positive, "Missing Values by Field and Source", "null_rate_pct", "Missing values (%)",
                         (("null_count", "Missing values"),))
    )
    outliers = field_rates(rows("outlier_rate"), "Outlier Rates by Field and Source", "outlier_rate_pct",
                           "IQR outliers (%)", (("outlier_count", "Outliers"), ("outlier_values", "Evaluated values")))
    return missing_chart, daily_volume_chart(rows("volume")), outliers


def distribution_box_plot(pair, source, field):
    title = "Distribution and Outliers by Source"
    _, axis, topic = BOX_VARIABLES[field]
    rows = pair.topic("outlier_rate")
    rows = rows.loc[rows["field"].eq(field) & rows["silver_topic"].eq(topic)]
    if source:
        rows = rows.loc[rows["source"].eq(source)]
    rows = rows.dropna(subset=["q1", "median", "q3", "lower_whisker", "upper_whisker", "outlier_points"])
    if rows.empty:
        return theme.empty_figure(title, "Distribution statistics are unavailable for this selection "
                                  "in the current Gold publication.")

    figure = go.Figure()
    for row in rows.sort_values("source").itertuples(index=False):
        name = source_name(row.source)
        color = theme.SOURCE_COLORS[row.source]
        figure.add_box(x=[name], q1=[row.q1], median=[row.median], q3=[row.q3],
                       lowerfence=[row.lower_whisker], upperfence=[row.upper_whisker],
                       line_color=color, whiskerwidth=0.5, boxpoints=False, hoverinfo="skip")
        # Plotly uses fixed hover labels on boxes. Mark the median and outliers for the detailed tooltip.
        points = [row.median, *row.outlier_points]
        figure.add_scatter(
            x=[name] * len(points), y=points, mode="markers",
            marker={"color": color, "size": 7, "symbol": ["diamond-open"] + ["circle"] * len(row.outlier_points)},
            text=["Median"] + ["Outlier"] * len(row.outlier_points),
            hovertemplate=(
                f"Source: {name}<br>Evaluated observations: {theme.fmt(row.outlier_values)}<br>"
                f"Q1: {theme.fmt(row.q1, 2)}<br>Median: {theme.fmt(row.median, 2)}<br>"
                f"Q3: {theme.fmt(row.q3, 2)}<br>IQR: {theme.fmt(row.iqr, 2)}<br>"
                f"Outlier count: {theme.fmt(row.outlier_count)}<br>Outlier rate: {rate(row.outlier_rate_pct)}"
                "<br>%{text}: %{y}<extra></extra>"
            ),
        )
    theme.style(figure, title, y=axis, legend=False)
    figure.update_xaxes(automargin=True)
    figure.update_yaxes(automargin=True)
    return figure.update_layout(hovermode="closest", margin={"t": 16})


def table(columns, rows, empty="Indicator unavailable in this Gold publication."):
    if not rows:
        return html.P(empty, className="text-muted small mb-0")
    return dbc.Table(
        [html.Thead(html.Tr([html.Th(label, scope="col") for label in columns])),
         html.Tbody([html.Tr([html.Td(value) for value in row]) for row in rows])],
        responsive=True, hover=True, size="sm", className="mb-0 align-middle",
    )


def unique_observations_table(pair):
    counts = observations(pair).groupby("source")["unique_observations"].sum(min_count=1)
    counts = counts.sort_values(ascending=False)
    rows = [(source_name(source), theme.fmt(count)) for source, count in counts.items()]
    rows.append((html.Strong("Total"), html.Strong(theme.fmt(counts.sum(min_count=1)))))
    return table(("Source", "Unique Observations"), rows)


def quality_summary(pair):
    order = {status: i for i, status in enumerate(STATUS_LABELS)}
    checks = []
    for topic, label, value, detail, bound, unit in QUALITY_CHECKS:
        for row in pair.topic(topic).itertuples(index=False):
            target = f"{bound} {row.threshold:g}{unit}" if pd.notna(row.threshold) else "—"
            context = getattr(row, detail) if detail else None
            checks.append((row.status, label, source_name(row.source) if pd.notna(row.source) else "—",
                           context if pd.notna(context) else "—",
                           theme.fmt(getattr(row, value), 2, unit), target))
    for row in observations(pair).dropna(subset=["duplicate_rate_pct"]).itertuples(index=False):
        checks.append((None, "Duplicate rate", source_name(row.source), row.topic,
                       rate(row.duplicate_rate_pct), "—"))

    checks.sort(key=lambda row: order.get(row[0], len(order)))
    return table(("Indicator", "Source", "Field or Topic", "Current", "Target", "Status"),
                 [(*values, status_badge(status)) for status, *values in checks])


def utc_time(value):
    return value.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(value) else "—"


def panel(title, content, note=None):
    return dbc.Card(dbc.CardBody([
        html.H3(title, className="h5 mb-3"), content,
        html.P(note, className="small text-muted mb-0 mt-3") if note else None,
    ]), className="h-100")


def pipeline_health(pair):
    pipeline = pair.topic("pipeline_state")
    orchestration = pair.topic("orchestration")
    rejected = pair.topic("rejected_records")
    matching = pair.topic("matching")
    # The loader inserts a run and records success in one transaction. Uncommitted runs
    # are invisible to Gold. Subtract recorded failures from the load count in its window.
    successful = pipeline["load_runs_window"] - pipeline["failed_load_runs_window"]
    pipeline_table = table(
        ("Source", "Last Success (UTC)", "Watermark (UTC)"),
        [([html.Div(source_name(row.source)), status_badge(row.status)],
          [html.Div(utc_time(row.last_successful_run_at)),
           html.Small(f"{theme.fmt(successes)} OK / {theme.fmt(row.failed_load_runs_window)} failed",
                      className="text-muted")],
          [html.Div(utc_time(row.watermark_at)),
           html.Small(f"{theme.fmt(row.watermark_age_hours, 2)} h old", className="text-muted")
           if pd.notna(row.watermark_age_hours) else None])
         for row, successes in zip(pipeline.itertuples(index=False), successful)],
    )
    run_table = table(
        ("DAG", "OK", "Failed", "Expected", "Compliance"),
        [(row.dag_id, theme.fmt(row.successful_dag_runs), theme.fmt(row.failed_dag_runs),
          theme.fmt(row.expected_dag_runs), rate(row.run_compliance_pct)) for row in orchestration.itertuples(index=False)],
    )
    rejection_table = table(
        ("Source", "Rejected", "Unique Rejections"),
        [(source_name(row.source), theme.fmt(row.rejected_records), theme.fmt(row.unique_rejected_records))
         for row in rejected.itertuples(index=False)],
        "No rejection rows in this Gold publication.",
    )
    match_table = table(
        ("Source / Relation", "Eligible", "Accepted", "Pending", "Coverage"),
        [([html.Div(source_name(row.source)), html.Small(row.relation_type, className="text-muted")],
          theme.fmt(row.eligible), theme.fmt(row.accepted_links),
          theme.fmt(row.pending_links), rate(row.coverage_pct)) for row in matching.itertuples(index=False)],
    )
    return panel("Pipeline Health", html.Div([
        html.H4("Source Freshness and Loads", className="h6 mb-2"),
        pipeline_table,
        html.Small("Watermark age is measured at publication. Counts use the Gold reporting window.",
                   className="text-muted"),
        html.H4("Airflow Runs", className="h6 mt-3 mb-2"),
        run_table,
        html.Small("A newly initialized environment can have low run compliance with zero failed runs.",
                   className="text-muted"),
        html.H4("Matching Coverage", className="h6 mt-3 mb-2"),
        match_table,
        html.Small("Counts refer to eligible observations with accepted or pending-only links.",
                   className="text-muted"),
        html.H4("Rejected Records", className="h6 mt-3 mb-2"),
        rejection_table,
    ], className="pipeline-scroll"), "Source: Gold Governance")


def graph(figure):
    height = f"{figure.layout.height}px"
    if not figure.data:
        return html.Div(figure.layout.annotations[0].text, role="status",
                        className="d-flex align-items-center justify-content-center text-center text-muted p-3",
                        style={"minHeight": height})
    # Responsive Graph clears Plotly's layout height. Give its container the planned height.
    return dcc.Graph(figure=figure.update_layout(title_text=None), responsive=True,
                     style={"height": height, "minWidth": 0},
                     config={"displaylogo": False, "modeBarButtons": [["zoom2d", "resetScale2d"]]})


def chart_card(graph_id, figure, provenance, note=None, *, controls=None, footer=None):
    return dbc.Card(dbc.CardBody([
        html.H3(figure.layout.title.text, className="h5 mb-2"),
        controls,
        html.Div(graph(figure), id=graph_id),
        html.Div(note, className="small text-muted") if note else None,
        theme.source_note(f"Gold Governance · {provenance}"),
        footer,
    ]), className="h-100")


def source_options(pair):
    sources = sorted(pair.governance["source"].dropna().unique(), key=source_name)
    return [{"label": "All Sources", "value": ""}, *[
        {"label": source_name(source), "value": source} for source in sources
    ]]


def layout():
    pair = cache.get()
    missing, trend, outliers = source_charts(pair, "")
    return html.Main(dbc.Container([
        html.Header(theme.header("Data Governance Dashboard", "", pair), className="governance-heading"),
        dcc.Store(id="gold-stamp", data=pair.stamp),
        dbc.Row(kpi_cards(pair), id="kpis", className="g-2 mb-3"),
        dbc.Row([
            dbc.Col(html.Label("Chart source", htmlFor="source-filter", className="fw-bold"), width="auto"),
            dbc.Col(dcc.Dropdown(id="source-filter", options=source_options(pair), value="", clearable=False),
                    xs=12, sm=6, md=4, lg=3),
            dbc.Col(html.Small("Filters the charts.", className="text-muted"),
                    xs=12, md=True),
        ], className="g-2 align-items-center mb-3"),
        dbc.Row([
            dbc.Col(chart_card("missing-values", missing, "null_rate",
                               "Only fields with missing values are shown. Hatching marks expected gaps."), lg=5, xs=12),
            dbc.Col(chart_card("ingestion-trend", trend, "volume",
                               "Hover over a capture day to see processed rows and new observations.",
                               footer=html.Div([
                                   html.H4("Unique Observations by Source", className="h6 mb-1"),
                                   html.Div(unique_observations_table(pair), id="unique-observations-by-source"),
                                   html.Small("The table counts distinct observations across the Gold publication. "
                                              "The chart shows rows processed per capture day.",
                                              className="d-block text-muted mt-1"),
                               ], className="border-top mt-2 pt-2")), lg=7, xs=12),
        ], className="g-3 mb-3"),
        dbc.Row(dbc.Col(chart_card(
            "distribution-box-plot", distribution_box_plot(pair, "", "magnitude"), "outlier_rate",
            "Outliers represent statistically unusual observations and may correspond to real natural events.",
            controls=dbc.Row([
                dbc.Col(html.Label("Variable", htmlFor="box-variable", className="fw-bold"), width="auto"),
                dbc.Col(dcc.Dropdown(
                    id="box-variable", value="magnitude", clearable=False,
                    options=[{"label": label, "value": field} for field, (label, _, _) in BOX_VARIABLES.items()],
                ), xs=12, sm=6, lg=4),
            ], className="g-2 align-items-center mb-2"),
        )), className="g-3 mb-3"),
        dbc.Row([
            dbc.Col(chart_card("outlier-rates", outliers, "outlier_rate",
                               "The IQR rule identifies unusual values, including real natural events."),
                    xl=4, lg=6, xs=12),
            dbc.Col(panel("Data Quality Summary", html.Div(id="quality-summary", children=quality_summary(pair),
                                                           className="quality-scroll")), xl=5, lg=6, xs=12),
            dbc.Col(html.Section(pipeline_health(pair), id="pipeline-health", className="h-100"), xl=3, xs=12),
        ], className="g-3 mb-4"),
    ], fluid=True, className="governance px-3 px-lg-4"), className="bg-light min-vh-100")


app = Dash(__name__, external_stylesheets=[theme.STYLESHEET], title="Data Governance Dashboard")
app.index_string = app.index_string.replace("</head>", """<style>
    .governance { max-width: 1500px; }
    .governance-heading h2 { font-size: clamp(1.25rem, 2.2vw, 1.6rem); }
    .governance-heading > .row { padding-block: .65rem !important; margin-bottom: .7rem !important; }
    .governance-heading #gold-label { font-size: .875rem; }
    .governance .card { border-color: #dbe2e9; }
    .governance .card-body { min-width: 0; padding: .85rem; }
    #kpis .card-body { padding: .55rem .8rem; }
    #kpis h3 { font-size: 1.6rem; line-height: 1.15; }
    .governance .table { font-size: .875rem; }
    .governance .table td,
    .governance .table th { overflow-wrap: anywhere; }
    .governance .table th { font-weight: 600; vertical-align: middle; }
    .quality-scroll { max-height: 415px; overflow: auto; }
    .quality-scroll .table-responsive { overflow: visible; }
    .quality-scroll th { position: sticky; top: 0; background: white; z-index: 1; }
    .quality-scroll th:nth-child(n+4),
    .quality-scroll td:nth-child(n+4) { white-space: nowrap; }
    .pipeline-scroll { max-height: 484px; overflow-y: auto; }
    .pipeline-scroll > .table-responsive:first-of-type .table { table-layout: fixed; }
    .status-badge { display: inline-block; border-left: 4px solid var(--status-color);
                    border-radius: .25rem; padding: .15rem .4rem; background: #f3f5f7;
                    font-weight: 600; font-size: .75rem; white-space: nowrap; }
    #pipeline-health .table { font-size: .8rem; }
</style></head>""")
app.layout = layout


@app.callback(
    Output("missing-values", "children"), Output("ingestion-trend", "children"), Output("outlier-rates", "children"),
    Output("gold-label", "children"), Output("kpis", "children"), Output("source-filter", "options"),
    Output("quality-summary", "children"), Output("pipeline-health", "children"), Output("gold-stamp", "data"),
    Output("unique-observations-by-source", "children"),
    Input("refresh", "n_clicks"), Input("source-filter", "value"), State("gold-stamp", "data"),
    prevent_initial_call=True,
)
def update_dashboard(_clicks, source, shown_stamp):
    pair = cache.get()
    changed = pair.stamp != shown_stamp
    if ctx.triggered_id == "refresh" and not changed:
        raise PreventUpdate
    options = source_options(pair)
    source = source if any(option["value"] == source for option in options) else ""
    charts = tuple(graph(figure) for figure in source_charts(pair, source))
    if not changed:
        return charts + (no_update,) * 7
    return (*charts, pair.label, kpi_cards(pair), options, quality_summary(pair), pipeline_health(pair), pair.stamp,
            unique_observations_table(pair))


@app.callback(
    Output("distribution-box-plot", "children"),
    Input("gold-stamp", "data"), Input("source-filter", "value"), Input("box-variable", "value"),
    prevent_initial_call=True,
)
def update_distribution(_stamp, source, field):
    return graph(distribution_box_plot(cache.get(), source, field))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8050, load_dotenv=False)
