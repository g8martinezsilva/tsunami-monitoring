"""Data Governance Dashboard for the published Gold indicators."""

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
from dash.exceptions import PreventUpdate

from dashboard import theme
from dashboard.gold import GoldCache

OBSERVATION_TOPICS = ("earthquakes", "tsunami_events", "tsunami_bulletins")
STATUS_LABELS = {"warning": "Warning", "review": "Review", "ok": "OK", "expected": "Expected", "n/a": "N/A"}
QUALITY_CHECKS = (
    ("null_rate", "Null rate", "null_rate_pct", "field", "≤", "%"),
    ("outlier_rate", "IQR outlier rate", "outlier_rate_pct", "field", "≤", "%"),
    ("schema_compliance", "Schema compliance", "schema_compliance_pct", "silver_topic", "≥", "%"),
    ("matching", "Matching coverage", "coverage_pct", "relation_type", "≥", "%"),
    ("pipeline_state", "Watermark age", "watermark_age_hours", None, "≤", " h"),
    ("orchestration", "Run compliance", "run_compliance_pct", "dag_id", "≥", "%"),
)

cache = GoldCache(include_storytelling=False)


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
    # Gold checks every observation for each field. Source identifies its observation topic
    # in this catalog. A repeated source across topics would make the denominator ambiguous.
    by_source = counts.drop_duplicates("source", keep=False).set_index("source")["unique_observations"]
    evaluated = missing["source"].map(by_source)
    total = evaluated.sum(min_count=1)
    if pd.notna(total) and total > 0 and evaluated.notna().all() and missing["null_count"].notna().all():
        return "Overall Null Rate", percentage(missing["null_count"].sum(), total), (
            f"{theme.fmt(missing['null_count'].sum())} missing of {theme.fmt(total)} field values, including expected gaps"
        )
    return "Mean Field Null Rate", missing["null_rate_pct"].mean(), "Unweighted mean across fields and sources"


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
        field.replace("_", " ") + "<br>" + theme.source_name(source)
        + (f" · {STATUS_LABELS[status]}" if status in {"expected", "warning", "review"} else "")
        for field, source, status in rows[["field", "source", "status"]].fillna("").itertuples(index=False, name=None)
    ]
    figure = go.Figure()
    for source, group in rows.groupby("source", sort=False):
        hover = [
            "<br>".join([f"{theme.source_name(source)} · {row['field']}",
                         *(f"{label}: {theme.fmt(row[key])}" for key, label in counts),
                         f"Status: {STATUS_LABELS.get(row['status'], '—')}",
                         f"Threshold: {rate(row['threshold'])}"])
            for row in group.to_dict("records")
        ]
        color = theme.SOURCE_COLORS.get(source, theme.GREY)
        states = group["status"].fillna("")
        figure.add_bar(
            name=theme.source_name(source), x=group[column].astype(float), y=group["category"], orientation="h",
            marker={"color": [theme.STATUS_COLORS["expected"] if status == "expected" else color for status in states],
                    "line": {"color": [theme.STATUS_COLORS[status] if status in {"warning", "review"} else color
                                       for status in states],
                             "width": [2 if status in {"warning", "review"} else 0 for status in states]},
                    "pattern": {"shape": ["/" if status == "expected" else "" for status in states]}},
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
    theme.style(figure, title, x=axis, legend=False, height=max(295, 100 + 32 * len(rows)))
    return figure.update_layout(barmode="overlay", margin={"l": 8, "r": 50, "t": 48, "b": 50})


def daily_volume(rows):
    title = "Daily Ingestion Volume"
    rows = rows.dropna(subset=["period", "volume_rows"]).sort_values("period")
    if rows.empty:
        return theme.empty_figure(title, "No dated volume observations for the selected sources.")

    figure = go.Figure()
    for source, group in rows.groupby("source", sort=False):
        for column, label, pattern in (("volume_rows", "Processed rows", ""),
                                       ("new_observations", "New observations", "/")):
            values = group.dropna(subset=[column])
            if values.empty:
                continue
            figure.add_bar(
                name=theme.source_name(source), x=values["period"].astype(str), y=values[column].astype(float),
                legendgroup=source, showlegend=column == "volume_rows",
                marker={"color": theme.SOURCE_COLORS.get(source, theme.GREY), "pattern": {"shape": pattern}},
                hovertemplate=f"%{{x}}<br>{label}: %{{y:,.0f}}<extra>%{{fullData.name}}</extra>",
            )
    theme.style(figure, title, x="Capture day (UTC)", y="Observations (count)", height=415)
    dates = rows["period"].astype(str).unique().tolist()
    figure.update_xaxes(type="category", categoryorder="array", categoryarray=dates, automargin=True)
    figure.update_yaxes(rangemode="tozero", automargin=True)
    return figure.update_layout(barmode="group", hovermode="x", margin={"l": 50, "r": 8, "t": 32, "b": 116},
                                legend={"yanchor": "top", "y": -0.23, "font": {"size": 11}})


def source_charts(pair, source):
    def rows(topic):
        frame = pair.topic(topic)
        return frame.loc[frame["source"].eq(source)] if source else frame

    missing = rows("null_rate")
    positive = missing.loc[missing["null_count"].gt(0)]
    missing_chart = (
        theme.empty_figure("Missing Values by Field and Source", "No missing values for the selected sources.")
        if positive.empty and missing["null_count"].notna().any()
        else field_rates(positive, "Missing Values by Field and Source", "null_rate_pct", "Missing values (%)",
                         (("null_count", "Missing values"),))
    )
    outliers = field_rates(rows("outlier_rate"), "Outlier Rates by Field and Source", "outlier_rate_pct",
                           "IQR outliers (%)", (("outlier_count", "Outliers"), ("outlier_values", "Evaluated values")))
    return missing_chart, daily_volume(rows("volume")), outliers


def table(columns, rows, empty="Indicator unavailable in this Gold publication."):
    if not rows:
        return html.P(empty, className="text-muted small mb-0")
    return dbc.Table(
        [html.Thead(html.Tr([html.Th(label, scope="col") for label in columns])),
         html.Tbody([html.Tr([html.Td(value) for value in row]) for row in rows])],
        responsive=True, hover=True, size="sm", className="mb-0 align-middle",
    )


def quality_summary(pair):
    order = {status: i for i, status in enumerate(STATUS_LABELS)}
    checks = []
    for topic, label, value, detail, bound, unit in QUALITY_CHECKS:
        for row in pair.topic(topic).itertuples(index=False):
            target = f"{bound} {row.threshold:g}{unit}" if pd.notna(row.threshold) else "—"
            context = getattr(row, detail) if detail else None
            checks.append((row.status, label, theme.source_name(row.source) if pd.notna(row.source) else "—",
                           context if pd.notna(context) else "—",
                           theme.fmt(getattr(row, value), 2, unit), target))
    for row in observations(pair).dropna(subset=["duplicate_rate_pct"]).itertuples(index=False):
        checks.append((None, "Duplicate rate", theme.source_name(row.source), row.topic,
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
        ("Source", "Last Successful Load (UTC)", "Watermark (UTC)", "Loads (OK / Failed)", "Status"),
        [(theme.source_name(row.source), utc_time(row.last_successful_run_at),
          [html.Div(utc_time(row.watermark_at)),
           html.Small(f"{theme.fmt(row.watermark_age_hours, 2)} h old", className="text-muted")
           if pd.notna(row.watermark_age_hours) else None],
          f"{theme.fmt(successes)} / {theme.fmt(row.failed_load_runs_window)}",
          status_badge(row.status)) for row, successes in zip(pipeline.itertuples(index=False), successful)],
    )
    run_table = table(
        ("DAG", "Successful", "Failed", "Expected", "Compliance"),
        [(row.dag_id, theme.fmt(row.successful_dag_runs), theme.fmt(row.failed_dag_runs),
          theme.fmt(row.expected_dag_runs), rate(row.run_compliance_pct)) for row in orchestration.itertuples(index=False)],
    )
    rejection_table = table(
        ("Source", "Rejected Records", "Unique Rejections"),
        [(theme.source_name(row.source), theme.fmt(row.rejected_records), theme.fmt(row.unique_rejected_records))
         for row in rejected.itertuples(index=False)],
        "No rejection rows in this Gold publication.",
    )
    match_table = table(
        ("Source", "Relation", "Eligible", "Accepted", "Pending", "Coverage"),
        [(theme.source_name(row.source), row.relation_type, theme.fmt(row.eligible), theme.fmt(row.accepted_links),
          theme.fmt(row.pending_links), rate(row.coverage_pct)) for row in matching.itertuples(index=False)],
    )
    return panel("Pipeline Health", dbc.Row([
        dbc.Col([html.H4("Source Freshness and Loads", className="h6 mb-2"), pipeline_table,
                 html.Small("Watermark age is measured at publication. Counts use the Gold reporting window.",
                            className="text-muted")], lg=7, xs=12),
        dbc.Col([html.H4("Airflow Runs", className="h6 mb-2"), run_table,
                 html.Small("A newly initialized environment can have low run compliance with zero failed runs.",
                            className="text-muted")], lg=5, xs=12),
        dbc.Col([html.H4("Matching Coverage", className="h6 mb-2"), match_table,
                 html.Small("Counts refer to eligible observations with accepted or pending-only links.",
                            className="text-muted")], lg=7, xs=12),
        dbc.Col([html.H4("Rejected Records", className="h6 mb-2"), rejection_table], lg=5, xs=12),
    ], className="g-3"), "Source: Gold Governance")


def graph(figure):
    # Responsive Graph clears Plotly's layout height. Give its container the planned height.
    return dcc.Graph(figure=figure.update_layout(title_text=None), responsive=True,
                     style={"height": f"{figure.layout.height}px", "minWidth": 0},
                     config={"displaylogo": False, "modeBarButtons": [["zoom2d", "resetScale2d"]]})


def chart_card(graph_id, figure, provenance, note=None):
    return dbc.Card(dbc.CardBody([
        html.H3(figure.layout.title.text, className="h5 mb-2"),
        html.Div(graph(figure), id=graph_id),
        html.Div(note, className="small text-muted") if note else None,
        theme.source_note(f"Gold Governance · {provenance}"),
    ]), className="h-100")


def source_options(pair):
    sources = sorted(pair.governance["source"].dropna().unique(), key=theme.source_name)
    return [{"label": "All Sources", "value": ""}, *[
        {"label": theme.source_name(source), "value": source} for source in sources
    ]]


def layout():
    pair = cache.get()
    missing, volume, outliers = source_charts(pair, "")
    return html.Main(dbc.Container([
        html.Header(theme.header("Data Governance Dashboard", "", pair), className="governance-heading"),
        dcc.Store(id="gold-stamp", data=pair.stamp),
        dbc.Row(kpi_cards(pair), id="kpis", className="g-2 mb-3"),
        dbc.Row([
            dbc.Col(html.Label("Chart source", htmlFor="source-filter", className="fw-bold"), width="auto"),
            dbc.Col(dcc.Dropdown(id="source-filter", options=source_options(pair), value="", clearable=False),
                    xs=12, sm=6, md=4, lg=3),
            dbc.Col(html.Small("Filters missing values, ingestion volume and outlier charts.", className="text-muted"),
                    xs=12, md=True),
        ], className="g-2 align-items-center mb-3"),
        dbc.Row([
            dbc.Col(chart_card("missing-values", missing, "null_rate",
                               "Only fields with missing values are shown. Hatching marks expected gaps."), lg=6, xs=12),
            dbc.Col(chart_card("ingestion-volume", volume, "volume",
                               "Solid bars show processed rows. Hatched bars show new observations."), lg=6, xs=12),
            dbc.Col(chart_card("outlier-rates", outliers, "outlier_rate",
                               "The IQR rule identifies unusual values, including real natural events."), lg=6, xs=12),
            dbc.Col(panel("Data Quality Summary", html.Div(id="quality-summary", children=quality_summary(pair),
                                                           className="quality-scroll")), lg=6, xs=12),
        ], className="g-3"),
        html.Section(pipeline_health(pair), id="pipeline-health", className="mt-3 mb-4"),
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
    .status-badge { display: inline-block; border-left: 4px solid var(--status-color);
                    border-radius: .25rem; padding: .15rem .4rem; background: #f3f5f7;
                    font-weight: 600; font-size: .75rem; white-space: nowrap; }
    #pipeline-health .table { font-size: .8rem; }
</style></head>""")
app.layout = layout


@app.callback(
    Output("missing-values", "children"), Output("ingestion-volume", "children"), Output("outlier-rates", "children"),
    Output("gold-label", "children"), Output("kpis", "children"), Output("source-filter", "options"),
    Output("quality-summary", "children"), Output("pipeline-health", "children"), Output("gold-stamp", "data"),
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
        return charts + (no_update,) * 6
    return (*charts, pair.label, kpi_cards(pair), options, quality_summary(pair), pipeline_health(pair), pair.stamp)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8050, load_dotenv=False)
