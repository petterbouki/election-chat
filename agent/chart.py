"""
chart.py — Génération de graphiques Plotly à partir des résultats SQL.
"""

import logging
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

log = logging.getLogger(__name__)

# Palette de couleurs cohérente
COLORS = ["#1D9E75", "#534AB7", "#D85A30", "#378ADD", "#BA7517", "#D4537E", "#639922", "#888780"]


def _best_x_y(df: pd.DataFrame, hint_x: Optional[str] = None, hint_y: Optional[str] = None):
    """Devine les colonnes X et Y les plus pertinentes."""
    str_cols = [c for c in df.columns if df[c].dtype == object]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    x = hint_x or (str_cols[0] if str_cols else df.columns[0])
    y = hint_y or (num_cols[0] if num_cols else df.columns[-1])
    return x, y


def make_bar_chart(df: pd.DataFrame, title: str = "", x: Optional[str] = None, y: Optional[str] = None) -> go.Figure:
    """Graphique en barres horizontales."""
    x_col, y_col = _best_x_y(df, x, y)
    df_sorted = df.sort_values(y_col, ascending=True).tail(20)

    fig = px.bar(
        df_sorted,
        x=y_col,
        y=x_col,
        orientation="h",
        title=title or f"{y_col} par {x_col}",
        color=y_col,
        color_continuous_scale=["#E1F5EE", "#1D9E75"],
        text=y_col,
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(
        height=max(300, len(df_sorted) * 30 + 100),
        showlegend=False,
        plot_bgcolor="white",
        xaxis_title=y_col,
        yaxis_title="",
        coloraxis_showscale=False,
        margin=dict(l=200, r=60, t=60, b=40),
    )
    return fig


def make_pie_chart(df: pd.DataFrame, title: str = "", names: Optional[str] = None, values: Optional[str] = None) -> go.Figure:
    """Camembert."""
    x_col, y_col = _best_x_y(df, names, values)
    fig = px.pie(
        df,
        names=x_col,
        values=y_col,
        title=title or f"Répartition de {y_col}",
        color_discrete_sequence=COLORS,
        hole=0.35,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(height=450, showlegend=True)
    return fig


def make_histogram(df: pd.DataFrame, title: str = "", column: Optional[str] = None) -> go.Figure:
    """Histogramme d'une colonne numérique."""
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    col = column or (num_cols[0] if num_cols else df.columns[0])

    fig = px.histogram(
        df,
        x=col,
        title=title or f"Distribution de {col}",
        nbins=20,
        color_discrete_sequence=["#1D9E75"],
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#1E1E1C",
        plot_bgcolor="#1E1E1C",
        font=dict(color="#F1EFE8"),
        title=dict(font=dict(color="#1D9E75")),
    )
    return fig


def make_line_chart(df: pd.DataFrame, title: str = "", x: Optional[str] = None, y: Optional[str] = None) -> go.Figure:
    """Graphique en ligne."""
    x_col, y_col = _best_x_y(df, x, y)
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        title=title or f"{y_col} en fonction de {x_col}",
        markers=True,
        color_discrete_sequence=["#1D9E75"],
    )
    fig.update_layout(height=350, plot_bgcolor="white")
    return fig


def make_chart(
    df: pd.DataFrame,
    chart_type: str = "bar",
    title: str = "",
    x: Optional[str] = None,
    y: Optional[str] = None,
) -> Optional[go.Figure]:
    """
    Factory principale : crée le bon graphique selon chart_type.
    chart_type : 'bar', 'pie', 'histogram', 'line'
    """
    if df is None or df.empty:
        log.warning("DataFrame vide, pas de graphique généré")
        return None

    chart_type = chart_type.lower()
    try:
        if chart_type in ("bar", "barres", "barplot"):
            return make_bar_chart(df, title, x, y)
        elif chart_type in ("pie", "camembert", "donut"):
            return make_pie_chart(df, title, x, y)
        elif chart_type in ("histogram", "histogramme", "hist"):
            return make_histogram(df, title, x)
        elif chart_type in ("line", "ligne", "courbe"):
            return make_line_chart(df, title, x, y)
        else:
            # Fallback : bar
            return make_bar_chart(df, title, x, y)
    except Exception as e:
        log.error(f"Erreur génération graphique ({chart_type}): {e}")
        return None


# ─── Suggestion automatique de graphique ─────────────────────────────────────

def suggest_chart_type(df: pd.DataFrame) -> str:
    """Suggère le type de graphique le plus adapté aux données."""
    if df is None or df.empty:
        return "bar"

    str_cols = [c for c in df.columns if df[c].dtype == object]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

    if len(str_cols) == 1 and len(num_cols) == 1:
        n_rows = len(df)
        if n_rows <= 8:
            return "pie"
        return "bar"

    if len(num_cols) >= 1 and len(str_cols) == 0:
        return "histogram"

    return "bar"
