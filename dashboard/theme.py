"""Shared look of both dashboards (Workshop 4): palette, header, KPI cards and chart defaults.

Colours come from the Okabe-Ito palette, which stays distinguishable for the common forms of colour
blindness (the two bulletin centers extend it). The same colour always means the same thing in both
applications, and the four relevance-level colours are reserved: no source uses them. The assignment
follows the design in the Workshop 4 report (Visualization Design Decisions, colour table).
"""

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html

PROJECT = "Tsunami Monitoring System"
TEAM = "Team 8 — Lina Moreno, Jaider Ávila, Gabriela Martínez"
TOPIC = "Pacific earthquakes, historical tsunamis and official tsunami bulletins"

# Okabe-Ito colours
BLACK, ORANGE, SKY, GREEN, YELLOW, BLUE, VERMILLION, PURPLE, GREY = (
    "#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7", "#8C8C8C")

DARK_GOLD, VIOLET = "#8C6D1F", "#5E3C99"  # extensions for the two warning centers

SOURCE_COLORS = {"usgs_fdsn": BLUE, "usgs_realtime": PURPLE, "noaa_hazel": GREEN, "ntwc": DARK_GOLD, "ptwc": VIOLET}
SOURCE_NAMES = {"usgs_fdsn": "USGS catalog (FDSN)", "usgs_realtime": "USGS real-time feed",
                "noaa_hazel": "NOAA tsunami database", "ntwc": "NTWC (Alaska)", "ptwc": "PTWC (Hawaii)"}
NOT_RATED = "NOT RATED"  # earthquakes below M6 receive no relevance level
LEVEL_COLORS = {"LOW": SKY, "MODERATE": ORANGE, "HIGH": VERMILLION, "OFFICIAL ALERT": BLACK, NOT_RATED: GREY}
LEVEL_ORDER = ["LOW", "MODERATE", "HIGH", "OFFICIAL ALERT"]
# Status colours appear only in status marks, always next to the status name
STATUS_COLORS = {"ok": GREEN, "warning": YELLOW, "review": PURPLE, "expected": GREY, "n/a": GREY}
STATUS_BADGES = {"ok": "success", "warning": "warning", "review": "info", "expected": "secondary", "n/a": "light"}

RELEVANCE_NOTE = ("The Tsunami Relevance Level is a monitoring aid computed from magnitude, depth and nearby "
                  "historical tsunamis. It is not a tsunami prediction or warning: official warnings come only "
                  "from the tsunami warning centers.")

STYLESHEET = dbc.themes.FLATLY


def source_name(source):
    return SOURCE_NAMES.get(source, source)


def header(title, subtitle, pair):
    """Page header: project, dashboard, topic, team and the time of the Gold pair on screen."""
    return dbc.Row([
        dbc.Col([
            html.H2(f"{PROJECT} — {title}", className="mb-0"),
            html.Div(subtitle, className="text-muted"),
            html.Small(f"{TOPIC} · {TEAM}", className="text-muted"),
        ], md=9),
        dbc.Col([
            html.Div("Data last updated", className="small text-muted text-md-end"),
            html.Div(pair.label, id="gold-label", className="fw-bold text-md-end"),
            dbc.Button("Refresh data", id="refresh", size="sm", color="primary", outline=True,
                       className="mt-1 float-md-end"),
        ], md=3),
    ], className="py-3 mb-3 border-bottom align-items-center")


def kpi_card(label, value, detail=None, status=None, card_id=None):
    """Metric card: large value, short label and an optional status badge."""
    body = [html.Div(label, className="small text-muted"), html.H3(value, className="mb-0")]
    if detail:
        body.append(html.Div(detail, className="small text-muted"))
    if status:
        body.append(dbc.Badge(status, color=STATUS_BADGES.get(status, "light"), className="mt-1"))
    extra = {"id": card_id} if card_id else {}  # Dash rejects id=None
    return dbc.Card(dbc.CardBody(body), className="h-100 shadow-sm", **extra)


def source_note(text):
    return html.Div(f"Source: {text}", className="small text-muted mt-1")


def style(figure, title, x=None, y=None, legend=True, height=380):
    """Common chart layout: descriptive title, labelled axes, readable legend."""
    figure.update_layout(
        title={"text": title, "x": 0.01, "font": {"size": 16}}, template="plotly_white", height=height,
        margin={"l": 60, "r": 20, "t": 60, "b": 50}, showlegend=legend,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.0, "x": 0},
        font={"family": "Lato, Helvetica, Arial, sans-serif", "size": 13})
    if x is not None:
        figure.update_xaxes(title_text=x)
    if y is not None:
        figure.update_yaxes(title_text=y)
    return figure


def empty_figure(title, message):
    """Placeholder chart that says why there is nothing to show (e.g. an older Gold pair)."""
    figure = go.Figure()
    figure.add_annotation(text=message, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper",
                          font={"size": 14, "color": GREY})
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False)
    return style(figure, title, legend=False, height=300)


def fmt(value, digits=0, suffix=""):
    """Number for a card: thousands separator, a dash when missing."""
    try:
        if value is None or value != value:
            return "—"
        return f"{value:,.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return "—"
