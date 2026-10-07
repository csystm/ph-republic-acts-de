"""Temporal EDA on the curated RA corpus.

Exploits the now 99.5%-populated `approval_date` column to surface
legislative-activity patterns:

- enactments per year
- month-of-year distribution (session-driven seasonality)
- day-of-week distribution

Reads directly from the curated parquet; writes PNGs and Plotly HTML
embeds under `outputs/analytics/plots/`. No clustering, no refits.
"""
from __future__ import annotations

import io as _io

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from src.analytics.tfidf_cluster import REPO_ROOT, load_corpus
from src.utils.io import atomic_write_bytes, atomic_write_text, get_logger

log = get_logger(__name__)

OUT_DIR = REPO_ROOT / "outputs" / "analytics"
PLOTS_DIR = OUT_DIR / "plots"

MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]
DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _save_png(fig: plt.Figure, dest) -> None:
    fig.tight_layout()
    buf = _io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    atomic_write_bytes(buf.getvalue(), dest)
    log.info("Wrote %s", dest.relative_to(REPO_ROOT))


def _save_plotly(fig: go.Figure, dest) -> None:
    html = pio.to_html(
        fig, include_plotlyjs="cdn", full_html=True,
        config={"responsive": True, "displaylogo": False},
    )
    atomic_write_text(html, dest)
    log.info("Wrote %s", dest.relative_to(REPO_ROOT))


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse `approval_date` (object dtype) into a proper datetime column."""
    before_null = int(df["approval_date"].isna().sum())
    parsed = pd.to_datetime(df["approval_date"], errors="coerce")
    after_null = int(parsed.isna().sum())
    log.info(
        "Parsed approval_date: %d rows, %d null before, %d null after parsing",
        len(df), before_null, after_null,
    )
    out = df.copy()
    out["approval_date_dt"] = parsed
    return out


# ----------------------------------------------------------------------
# Charts
# ----------------------------------------------------------------------
def plot_by_year(df: pd.DataFrame) -> None:
    counts = (
        df["approval_date_dt"].dt.year.value_counts().sort_index()
    )
    years = counts.index.astype(int).tolist()
    n = counts.values.tolist()

    # --- matplotlib PNG ---
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.bar(years, n, color="#4c72b0", width=0.8)
    ax.set_xlabel("Year of approval")
    ax.set_ylabel("Number of RAs enacted")
    ax.set_title(f"Republic Acts enacted per year ({min(years)}–{max(years)})")
    ax.grid(axis="y", alpha=0.3)
    _save_png(fig, PLOTS_DIR / "temporal_by_year.png")

    # --- Plotly HTML ---
    fig2 = go.Figure(go.Bar(
        x=years, y=n, marker_color="#4c72b0",
        hovertemplate="Year %{x}<br>%{y} RAs<extra></extra>",
    ))
    fig2.update_layout(
        title="Republic Acts enacted per year",
        xaxis_title="Year of approval",
        yaxis_title="Number of RAs",
        template="plotly_white",
        height=440,
    )
    _save_plotly(fig2, PLOTS_DIR / "temporal_by_year.html")


def plot_by_month(df: pd.DataFrame) -> None:
    m = df["approval_date_dt"].dt.month.dropna().astype(int)
    counts = m.value_counts().reindex(range(1, 13), fill_value=0).sort_index()
    n = counts.values.tolist()

    # --- matplotlib PNG ---
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(MONTH_LABELS, n, color="#55a868")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of RAs")
    ax.set_title("RAs by month of approval (all years combined)")
    ax.grid(axis="y", alpha=0.3)
    _save_png(fig, PLOTS_DIR / "temporal_by_month.png")

    # --- Plotly HTML ---
    fig2 = go.Figure(go.Bar(
        x=MONTH_LABELS, y=n, marker_color="#55a868",
        hovertemplate="%{x}<br>%{y} RAs<extra></extra>",
    ))
    fig2.update_layout(
        title="RAs by month of approval",
        xaxis_title="Month",
        yaxis_title="Number of RAs",
        template="plotly_white",
        height=440,
    )
    _save_plotly(fig2, PLOTS_DIR / "temporal_by_month.html")


def plot_by_dow(df: pd.DataFrame) -> None:
    d = df["approval_date_dt"].dt.dayofweek.dropna().astype(int)
    counts = d.value_counts().reindex(range(7), fill_value=0).sort_index()
    n = counts.values.tolist()

    # --- matplotlib PNG ---
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(DOW_LABELS, n, color="#c44e52")
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Number of RAs")
    ax.set_title("RAs by day of week of approval")
    ax.grid(axis="y", alpha=0.3)
    _save_png(fig, PLOTS_DIR / "temporal_by_dow.png")

    # --- Plotly HTML ---
    fig2 = go.Figure(go.Bar(
        x=DOW_LABELS, y=n, marker_color="#c44e52",
        hovertemplate="%{x}<br>%{y} RAs<extra></extra>",
    ))
    fig2.update_layout(
        title="RAs by day of week of approval",
        xaxis_title="Day of week",
        yaxis_title="Number of RAs",
        template="plotly_white",
        height=440,
    )
    _save_plotly(fig2, PLOTS_DIR / "temporal_by_dow.html")


def main() -> int:
    log.info("=== Temporal EDA ===")
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_corpus()
    df = parse_dates(df)

    log.info("Plotting enactments per year...")
    plot_by_year(df)

    log.info("Plotting month-of-year distribution...")
    plot_by_month(df)

    log.info("Plotting day-of-week distribution...")
    plot_by_dow(df)

    log.info("=== Temporal EDA complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())