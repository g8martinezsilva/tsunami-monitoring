"""Storytelling dashboard for the Pacific monitoring analyst (Workshop 4), at localhost:8051.

It reads only the storytelling Gold dataset and follows the design in the Workshop 4 report: the page
answers four questions in reading order (what is happening now, what the official centers said, how
unusual it is, the historical context). The time window (last 24 hours or 7 days) is measured back
from the Gold publication time, the "last update" shown in the header.

The Tsunami Relevance Level is a monitoring aid, never a tsunami prediction or warning.
"""

import os
from datetime import timedelta

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate

from dashboard import theme
from dashboard.gold import GoldCache

WINDOWS = {"24h": ("Last 24 hours", timedelta(hours=24)), "7d": ("Last 7 days", timedelta(days=7))}
ALL_AREA = "All Pacific query area"
CARDS = [("LOW", "Low", "Lower priority"), ("MODERATE", "Moderate", "Check soon"), ("HIGH", "High", "High priority"),
         ("OFFICIAL ALERT", "Official alert", "Follow the warning center"),
         (theme.NOT_RATED, "Not rated", "Below magnitude 6")]
LEVEL_RANK = {"OFFICIAL ALERT": 0, "HIGH": 1, "MODERATE": 2, "LOW": 3, theme.NOT_RATED: 4}
CENTER_NAMES = {"ntwc": "NTWC", "ptwc": "PTWC"}
TSUNAMI_GOV = "https://www.tsunami.gov/"

cache = GoldCache()


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def recent(pair, window):
    """Recent earthquakes in the window, with a level for every row (NOT RATED below M6)."""
    rows = pair.metric("recent_earthquakes").copy()
    rows["level"] = rows["relevance_level"].fillna(theme.NOT_RATED)
    start = pair.published_at - WINDOWS[window][1]
    return rows[(rows["event_time"] > start) & (rows["event_time"] <= pair.published_at)].reset_index(drop=True)


def area_comparison(pair):
    rows = pair.metric("historical_vs_current")
    whole = rows[rows["region_cell"] == ALL_AREA]
    return (rows[rows["region_cell"] != ALL_AREA], whole.iloc[0] if len(whole) else None)


def plural(count, word, plural_word=None):
    """'none', '1 earthquake', '3 earthquakes'."""
    if count == 0:
        return f"no {plural_word or word + 's'}"
    return f"{count} {word if count == 1 else plural_word or word + 's'}"


def main_message(pair, window):
    """Three sentences, conclusion first (rules in the Workshop 4 report, section Main Message)."""
    rows = recent(pair, window)
    n = len(rows)
    k = int(rows["level"].isin(["MODERATE", "HIGH", "OFFICIAL ALERT"]).sum())
    b = int(rows["bulletin_source"].notna().sum())
    label = WINDOWS[window][0].lower()
    first = (f"In the {label.replace('last ', '')} up to the last update, {plural(n, 'earthquake')} "
             f"(magnitude 4.5 or higher) {'was' if n == 1 else 'were'} recorded in the Pacific area")
    if n:
        first += (f": {'none' if k == 0 else k} {'deserves' if k == 1 else 'deserve'} a closer look and "
                  f"{'none' if b == 0 else b} {'has' if b == 1 else 'have'} a linked official bulletin.")
    else:
        first += "."
    _, whole = area_comparison(pair)
    if whole is None or pd.isna(whole["expected_count"]):
        second = "The comparison of the past 30 days with 2010–2025 is not available in this data update."
    else:
        busier = pd.notna(whole["p_value"]) and whole["p_value"] < 0.05
        second = (f"Over the past {int(whole['window_days'])} days, strong earthquakes (magnitude 6 or higher) were "
                  f"{'more frequent than usual' if busier else 'not unusually frequent'}: {int(whole['count'])}, "
                  f"compared with about {whole['expected_count']:.1f} expected from the 2010–2025 rate.")
    third = ("The relevance level only helps you decide what to check first. It is not a tsunami prediction: "
             "rely on the official warning centers (NTWC, PTWC) for warnings.")
    return first, second, third


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------

def level_cards(pair, window):
    counts = recent(pair, window)["level"].value_counts()
    cards = []
    for level, name, hint in CARDS:
        cards.append(dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([html.Div(name.upper(), className="fw-bold"), html.Small(hint, className="text-muted")]),
            html.Div(f"{int(counts.get(level, 0))}", className="level-count"),
        ], className="d-flex justify-content-between align-items-center py-2"),
            className="h-100 level-card", style={"borderLeft": f"8px solid {theme.LEVEL_COLORS[level]}"}), xs=6, lg=True))
    return cards


def recent_map(pair, window):
    title = "Where did earthquakes happen recently?"
    rows = recent(pair, window)
    if rows.empty:
        return theme.empty_figure(title, f"No earthquakes recorded in the {WINDOWS[window][0].lower()} before the last update.")
    figure = go.Figure()
    for level in [theme.NOT_RATED, "LOW", "MODERATE", "HIGH", "OFFICIAL ALERT"]:
        group = rows[rows["level"] == level]
        if group.empty:
            continue
        hover = [f"<b>{r.place}</b><br>{r.event_time:%Y-%m-%d %H:%M} UTC<br>Magnitude {r.magnitude:.1f} · "
                 f"depth {r.depth_km:.0f} km<br>Level: {level.title()}" for r in group.itertuples()]
        figure.add_scattergeo(
            lon=group["longitude"], lat=group["latitude"], name=level.title(), text=hover, hoverinfo="text",
            marker={"size": 7 + 4 * (group["magnitude"] - 4.5).clip(lower=0) ** 1.5,
                    "color": theme.LEVEL_COLORS[level], "line": {"width": 0.8, "color": "white"},
                    "opacity": 0.8 if level == theme.NOT_RATED else 0.95})
    # Pacific-centred: 180 degrees in the middle keeps the basin in one piece
    figure.update_geos(projection_type="natural earth", projection_rotation_lon=180, showcountries=True,
                       countrycolor="#d0d6dc", showland=True, landcolor="#f2f2ee", showocean=True,
                       oceancolor="#e8f1f8", coastlinecolor="#9aa5b1", showframe=False, fitbounds="locations")
    theme.style(figure, title, height=430)
    return figure.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0},
                                legend={"orientation": "h", "y": -0.02, "x": 0, "title": None})


def daily_activity(pair):
    """Earthquakes per day over the last 30 days (the time series of the page)."""
    title = "Is activity changing from day to day?"
    rows = pair.metric("recent_earthquakes").copy()
    end = pd.Timestamp(pair.published_at)
    rows = rows[(rows["event_time"] > end - timedelta(days=30)) & (rows["event_time"] <= end)]
    if rows.empty:
        return theme.empty_figure(title, "No earthquakes in the last 30 days of this data update.")
    days = pd.date_range((end - timedelta(days=29)).normalize(), end.normalize(), freq="D")
    rows["day"] = rows["event_time"].dt.tz_convert("UTC").dt.normalize()
    all_counts = rows.groupby("day").size().reindex(days, fill_value=0)
    strong = rows[rows["magnitude"] >= 6].groupby("day").size().reindex(days, fill_value=0)
    figure = go.Figure()
    figure.add_bar(x=days, y=all_counts, name="Magnitude 4.5+", marker_color=theme.SOURCE_COLORS["usgs_realtime"],
                   hovertemplate="%{x|%Y-%m-%d}: %{y} earthquakes<extra></extra>")
    figure.add_scatter(x=days, y=strong, name="Magnitude 6+", mode="lines+markers",
                       line={"color": theme.BLACK, "width": 2}, marker={"size": 5},
                       hovertemplate="%{x|%Y-%m-%d}: %{y} strong earthquakes<extra></extra>")
    theme.style(figure, title, x="Day (UTC)", y="Earthquakes (count)", height=250)
    return figure.update_layout(margin={"l": 50, "r": 10, "t": 10, "b": 40}, bargap=0.15)


def bulletin_rows(pair, window):
    rows = recent(pair, window)
    if rows.empty:
        return []
    rows["has_bulletin"] = rows["bulletin_source"].notna()
    rows["rank"] = rows["level"].map(LEVEL_RANK)
    rows = rows.sort_values(["has_bulletin", "rank", "magnitude"], ascending=[False, True, False])
    out = []
    for r in rows.itertuples():
        bulletin = "None linked"
        if pd.notna(r.bulletin_source):
            sent = f" · {r.bulletin_sent_at:%m-%d %H:%M}" if pd.notna(r.bulletin_sent_at) else ""
            bulletin = f"{CENTER_NAMES.get(r.bulletin_source, r.bulletin_source)} · {r.bulletin_severity or '—'}{sent}"
        out.append({"Level": r.level.title(), "Time (UTC)": f"{r.event_time:%m-%d %H:%M}", "Place": r.place,
                    "Magnitude": round(float(r.magnitude), 1),
                    "Depth (km)": None if pd.isna(r.depth_km) else round(float(r.depth_km)),
                    "Past tsunamis within 300 km": "—" if pd.isna(r.history_tsunamis) else int(r.history_tsunamis),
                    "Official bulletin": bulletin, "level_key": r.level})
    return out


def bulletin_table(pair, window):
    columns = ["Level", "Time (UTC)", "Place", "Magnitude", "Depth (km)", "Past tsunamis within 300 km",
               "Official bulletin"]
    return dash_table.DataTable(
        id="bulletin-table", data=bulletin_rows(pair, window), columns=[{"name": c, "id": c} for c in columns],
        page_size=7, sort_action="native", style_as_list_view=True,
        style_cell={"fontFamily": "inherit", "fontSize": 13, "padding": "4px 6px", "textAlign": "left",
                    "whiteSpace": "normal", "height": "auto", "maxWidth": 260},
        style_header={"fontWeight": 600, "backgroundColor": "white"},
        style_data_conditional=[{"if": {"filter_query": f'{{level_key}} = "{level}"', "column_id": "Level"},
                                 "borderLeft": f"6px solid {color}"} for level, color in theme.LEVEL_COLORS.items()])


def busier_than_usual(pair):
    title = "Is the last month busier than usual?"
    regions, whole = area_comparison(pair)
    if whole is None:
        return theme.empty_figure(title, "The comparison is not available in this data update."), ""
    top = regions[regions["count"] > 0].sort_values(["p_value", "count"], ascending=[True, False]).head(5)
    subtitle = (f"Whole Pacific area, past {int(whole['window_days'])} days: {int(whole['count'])} strong earthquakes "
                f"(magnitude 6+) · about {whole['expected_count']:.1f} expected")
    if top.empty:
        return theme.empty_figure(title, "No region had a strong earthquake in the past 30 days."), subtitle
    labels = top["region_cell"].tolist()
    figure = go.Figure()
    for label, observed, expected in zip(labels, top["count"], top["expected_count"]):
        figure.add_scatter(x=[expected, observed], y=[label, label], mode="lines", showlegend=False,
                           line={"color": "#c8d0d8", "width": 3}, hoverinfo="skip")
    figure.add_scatter(x=top["expected_count"], y=labels, mode="markers", name="Typical (2010–2025 rate)",
                       marker={"size": 13, "color": "white", "line": {"color": theme.GREY, "width": 2}},
                       hovertemplate="%{y}<br>Typical: %{x:.2f}<extra></extra>")
    figure.add_scatter(x=top["count"], y=labels, mode="markers", name="Past 30 days",
                       marker={"size": 13, "color": theme.BLUE},
                       customdata=top["p_value"], hovertemplate="%{y}<br>Past 30 days: %{x}<br>"
                       "Chance of at least this many at the usual rate: %{customdata:.1%}<extra></extra>")
    theme.style(figure, title, x="Strong earthquakes (magnitude 6+) in 30 days (count)", height=300)
    figure.update_yaxes(autorange="reversed", automargin=True)
    return figure.update_layout(margin={"l": 10, "r": 10, "t": 30, "b": 45},
                                legend={"orientation": "h", "y": 1.02, "yanchor": "bottom", "x": 0}), subtitle


def common_regions(pair):
    title = "Where are strong quakes most common?"
    rows = pair.metric("regional_baseline").sort_values("count", ascending=False).head(6)
    if rows.empty:
        return theme.empty_figure(title, "The regional baseline is not available in this data update.")
    tsunamis = rows["historical_tsunamis"].fillna(0).astype(int)
    figure = go.Figure(go.Bar(
        x=rows["count"], y=rows["region_cell"], orientation="h", marker_color=theme.SOURCE_COLORS["usgs_fdsn"],
        text=[f"{c} · {t} ts." for c, t in zip(rows["count"], tsunamis)], textposition="outside", cliponaxis=False,
        customdata=rows[["max_magnitude", "max_water_height_m"]].fillna(0),
        hovertemplate="%{y}<br>%{x} earthquakes (magnitude 6+)<br>Largest magnitude %{customdata[0]:.1f}"
                      "<br>Highest tsunami %{customdata[1]:.1f} m<extra></extra>"))
    theme.style(figure, title, x="Earthquakes, magnitude 6+ (count) · ts. = tsunamis", legend=False, height=300)
    figure.update_yaxes(autorange="reversed", automargin=True)
    return figure.update_layout(margin={"l": 10, "r": 40, "t": 10, "b": 45})


def magnitudes(pair):
    title = "How strong are the earthquakes?"
    rows = pair.metric("earthquake_magnitude_distribution")
    if rows.empty:
        return theme.empty_figure(title, "Magnitude distributions are not available in this data update.")
    from plotly.subplots import make_subplots
    figure = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.18,
                           subplot_titles=("Recent feed: worldwide, magnitude 4.5+",
                                           "Pacific catalog 2010–2025, magnitude 6+"))
    for i, source in enumerate(["usgs_realtime", "usgs_fdsn"], start=1):
        bins = rows[rows["source"] == source].groupby("bin_start")["count"].sum()
        figure.add_bar(x=bins.index + 0.25, y=bins.values, width=0.45, row=i, col=1, showlegend=False,
                       marker_color=theme.SOURCE_COLORS[source],
                       hovertemplate="Magnitude %{x:.2f} ± 0.25: %{y} earthquakes<extra></extra>")
        figure.update_yaxes(title_text="Count", row=i, col=1)
    figure.update_xaxes(title_text="Magnitude", dtick=0.5, row=2, col=1)
    figure.update_annotations(font_size=12, x=0, xanchor="left")
    theme.style(figure, title, legend=False, height=300)
    return figure.update_layout(margin={"l": 50, "r": 10, "t": 25, "b": 45})


def tsunamis_per_year(pair):
    title = "How many tsunamis were reported?"
    rows = pair.metric("historical_tsunamis_by_year")
    rows = rows[rows["year"].between(2010, 2025)].groupby("year")["count"].sum().sort_index()
    if rows.empty:
        return theme.empty_figure(title, "Yearly tsunami counts are not available in this data update.")
    figure = go.Figure(go.Scatter(x=rows.index.astype(int), y=rows.values, mode="lines+markers",
                                  line={"color": theme.SOURCE_COLORS["noaa_hazel"], "width": 2},
                                  hovertemplate="%{x}: %{y} tsunami reports<extra></extra>"))
    theme.style(figure, title, x="Year", y="Reports (count)", legend=False, height=300)
    figure.update_yaxes(rangemode="tozero")
    return figure.update_layout(margin={"l": 50, "r": 10, "t": 10, "b": 45})


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

def graph(figure, graph_id=None):
    extra = {"id": graph_id} if graph_id else {}
    return dcc.Graph(figure=figure.update_layout(title_text=None), config={"displaylogo": False},
                     style={"height": f"{figure.layout.height}px"}, **extra)


def card(step, title, body, source, subtitle=None):
    head = [html.Span(str(step), className="step"), html.Span(title, className="h5 mb-0 align-middle")]
    content = [html.Div(head, className="mb-1")]
    if subtitle is not None:
        content.append(html.Div(subtitle, className="small text-muted mb-1"))
    content += (body if isinstance(body, list) else [body]) + [theme.source_note(source)]
    return dbc.Card(dbc.CardBody(content), className="h-100 shadow-sm")


def message_card(pair, window):
    first, second, third = main_message(pair, window)
    return [html.Span("1", className="step"), html.Span("What you should know", className="h5 align-middle"),
            html.P(first, className="mb-1 mt-2"), html.P(second, className="mb-1"),
            html.P(third, className="mb-0 fw-bold")]


def lower_row(pair):
    busy, subtitle = busier_than_usual(pair)
    return [
        dbc.Col(card(3, "Is the last month busier than usual?", graph(busy),
                     "USGS real-time feed and earthquake catalog. Not a forecast.", subtitle), lg=3, md=6, xs=12),
        dbc.Col(card(4, "Where are strong quakes most common?", graph(common_regions(pair)),
                     "USGS catalog; NOAA/NCEI tsunami database", "2010–2025 · magnitude 6+ · top 6 regions"),
                lg=3, md=6, xs=12),
        dbc.Col(card(4, "How strong are the earthquakes?", graph(magnitudes(pair)),
                     "USGS real-time feed and earthquake catalog"), lg=3, md=6, xs=12),
        dbc.Col(card(4, "How many tsunamis were reported?", graph(tsunamis_per_year(pair)),
                     "NOAA/NCEI Global Historical Tsunami Database", "Pacific tsunami reports per year, 2010–2025"),
                lg=3, md=6, xs=12),
    ]


def layout():
    pair = cache.get()
    window = "7d"
    return html.Main(dbc.Container([
        theme.header("What is happening in the Pacific?",
                     "Recent earthquakes, official tsunami bulletins and the historical record", pair),
        dcc.Store(id="gold-stamp", data=pair.stamp),
        dbc.Card(dbc.CardBody(message_card(pair, window), id="message"), className="message-card mb-3"),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([
                html.Div("Show earthquakes from", className="small text-muted mb-1"),
                dbc.RadioItems(id="window", value=window, inline=True, inputClassName="btn-check",
                               labelClassName="btn btn-outline-dark btn-sm",
                               labelCheckedClassName="active",
                               options=[{"label": label, "value": key} for key, (label, _) in WINDOWS.items()]),
            ]), className="h-100"), xs=12, lg=2),
            dbc.Col(dbc.Row(level_cards(pair, window), id="level-cards", className="g-2"), xs=12, lg=10),
        ], className="g-2 mb-3"),
        dbc.Row([
            dbc.Col(card(1, "Where did earthquakes happen recently?",
                         [html.Div(graph(recent_map(pair, window)), id="map"),
                          html.Div(graph(daily_activity(pair)), id="daily")],
                         "USGS real-time earthquake feed; relevance level from the project's Gold layer",
                         "Colour = relevance level · bigger circle = stronger · below: earthquakes per day, "
                         "last 30 days"), lg=6, xs=12),
            dbc.Col(card(2, "What did the official warning centers say?", [
                html.Div(bulletin_table(pair, window), id="bulletins"),
                html.P(["“None linked” does not mean there is no warning: always check ",
                        html.A("tsunami.gov", href=TSUNAMI_GOV, target="_blank"), "."],
                       className="small fw-bold mt-2 mb-1"),
                html.P(theme.RELEVANCE_NOTE, className="small text-muted mb-0"),
            ], "USGS; NTWC and PTWC (tsunami.gov); NOAA/NCEI",
                "Earthquakes with a bulletin first, then by level and strength"), lg=6, xs=12),
        ], className="g-3 mb-3"),
        dbc.Row(lower_row(pair), id="lower-row", className="g-3 mb-4"),
    ], fluid=True, className="story px-3 px-lg-4"), className="bg-light min-vh-100")


app = Dash(__name__, external_stylesheets=[theme.STYLESHEET], title="What is happening in the Pacific?")
app.index_string = app.index_string.replace("</head>", """<style>
    .story { max-width: 1900px; }
    .story .card { border-color: #dbe2e9; }
    .story .card-body { min-width: 0; }
    .step { display: inline-flex; width: 1.6rem; height: 1.6rem; border-radius: 50%; background: #1f2a36; color: white;
            align-items: center; justify-content: center; font-weight: 700; font-size: .85rem; margin-right: .5rem; }
    .message-card { border: 2px solid #1f2a36 !important; }
    .level-count { font-size: 2rem; font-weight: 700; }
    .level-card { min-height: 72px; }
    .btn-check + .btn-outline-dark { margin-right: .3rem; }
</style></head>""")
app.layout = layout


@app.callback(
    Output("message", "children"), Output("level-cards", "children"), Output("map", "children"),
    Output("bulletins", "children"), Output("daily", "children"), Output("lower-row", "children"),
    Output("gold-label", "children"), Output("gold-stamp", "data"),
    Input("window", "value"), Input("refresh", "n_clicks"), State("gold-stamp", "data"),
    prevent_initial_call=True,
)
def update(window, _clicks, shown_stamp):
    pair = cache.get()
    changed = pair.stamp != shown_stamp
    if window is None:
        raise PreventUpdate
    return (message_card(pair, window), level_cards(pair, window), graph(recent_map(pair, window)),
            bulletin_table(pair, window),
            graph(daily_activity(pair)) if changed else no_update, lower_row(pair) if changed else no_update,
            pair.label if changed else no_update, pair.stamp)


if __name__ == "__main__":
    # 127.0.0.1 locally; the Docker image sets DASH_HOST=0.0.0.0 so the published port is reachable
    app.run(host=os.environ.get("DASH_HOST", "127.0.0.1"), port=8051, load_dotenv=False)
