"""Regenerate the findings charts from the serving database.

Requires the local stack to be running (Postgres published on localhost:5432):

    pip install psycopg2-binary matplotlib
    python analysis/make_charts.py

Outputs to docs/images/findings_*.png - commit the regenerated files.
"""

from __future__ import annotations

import os
import pathlib

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import psycopg2  # noqa: E402

DSN = os.environ.get("SERVING_DSN", "postgresql://energy:energy@localhost:5432/serving")
OUT = pathlib.Path(__file__).resolve().parents[1] / "docs" / "images"
BG = "#111217"
ACCENT = "#7ee787"
ACCENT2 = "#79c0ff"
PEAK = "#ffa657"


def fetch(query: str) -> list[tuple]:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def style(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_title(title, color="white", fontsize=13, pad=12)
    ax.set_xlabel(xlabel, color="#c9d1d9")
    ax.set_ylabel(ylabel, color="#c9d1d9")
    ax.grid(alpha=0.15)
    ax.tick_params(colors="#8b949e")
    for spine in ax.spines.values():
        spine.set_color("#30363d")


def save(fig, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    fig.savefig(path, facecolor=BG, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {path}")


def main() -> None:
    plt.style.use("dark_background")

    hours = fetch(
        "SELECT local_hour, avg(price_eur_mwh)::float "
        "FROM serving.price_hourly GROUP BY 1 ORDER BY 1"
    )
    peak = fetch(
        "SELECT avg(price_eur_mwh) FILTER (WHERE local_hour BETWEEN 18 AND 20)::float, "
        "       avg(price_eur_mwh) FILTER (WHERE local_hour BETWEEN 11 AND 14)::float "
        "FROM serving.price_hourly"
    )[0]
    rhythm = fetch(
        "SELECT local_hour, "
        "  avg(price_eur_mwh) FILTER (WHERE NOT is_weekend)::float, "
        "  avg(price_eur_mwh) FILTER (WHERE is_weekend)::float "
        "FROM serving.price_hourly GROUP BY 1 ORDER BY 1"
    )
    monthly = fetch(
        "SELECT to_char(delivery_ts AT TIME ZONE 'Europe/Berlin', 'YYYY-MM'), "
        "  count(*) FILTER (WHERE is_negative), avg(price_eur_mwh)::float "
        "FROM serving.price_hourly GROUP BY 1 ORDER BY 1"
    )

    # 1) Duck curve
    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=150, facecolor=BG)
    xs = [r[0] for r in hours]
    ys = [r[1] for r in hours]
    ax.plot(xs, ys, color=ACCENT, linewidth=2.4)
    ax.fill_between(xs, ys, min(ys) - 5, color=ACCENT, alpha=0.12)
    ax.axvline(13, color=ACCENT2, linestyle=":", alpha=0.7)
    ax.axvline(19, color=PEAK, linestyle=":", alpha=0.7)
    ax.annotate(
        f"midday trough ~€{peak[1]:.0f}",
        xy=(13, peak[1]),
        xytext=(13.5, peak[1] + 55),
        color=ACCENT2,
        arrowprops={"arrowstyle": "->", "color": ACCENT2, "alpha": 0.7},
    )
    ax.annotate(
        f"evening peak ~€{peak[0]:.0f}",
        xy=(19, peak[0] - 15),
        xytext=(12.8, 215),
        color=PEAK,
        arrowprops={"arrowstyle": "->", "color": PEAK, "alpha": 0.7},
    )
    style(
        ax,
        "Average German day-ahead price by hour (DE-LU, Jun-Sep 2026)",
        "Hour of day (Europe/Berlin)",
        "€/MWh",
    )
    save(fig, "findings_duck_curve.png")

    # 2) Week rhythm: weekend vs weekday
    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=150, facecolor=BG)
    ax.plot(
        [r[0] for r in rhythm],
        [r[1] for r in rhythm],
        color=ACCENT2,
        linewidth=2.2,
        label="weekday",
    )
    ax.plot(
        [r[0] for r in rhythm], [r[2] for r in rhythm], color=ACCENT, linewidth=2.2, label="weekend"
    )
    ax.legend(facecolor=BG, edgecolor="#30363d")
    style(
        ax,
        "The week rhythm: weekend prices collapse around midday",
        "Hour of day (Europe/Berlin)",
        "€/MWh",
    )
    save(fig, "findings_week_rhythm.png")

    # 3) Monthly negative hours + average price
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2), dpi=150, facecolor=BG)
    months = [r[0] for r in monthly]
    ax1.bar(months, [r[1] for r in monthly], color=ACCENT)
    style(ax1, "Negative-price hours per month", "", "hours below €0")
    ax2.plot(months, [r[2] for r in monthly], marker="o", color=PEAK, linewidth=2.2)
    style(ax2, "Average price per month", "", "€/MWh")
    fig.tight_layout()
    save(fig, "findings_monthly.png")


if __name__ == "__main__":
    main()
