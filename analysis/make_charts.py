"""Regenerate the findings charts from the serving database.

Requires the local stack to be running (Postgres published on localhost:5432):

    pip install psycopg2-binary matplotlib
    python analysis/make_charts.py

Outputs to docs/images/findings_*.png - commit the regenerated files.

Every number drawn or written on a chart comes from the same serving tables as
analysis/bi_queries.sql, so the charts and findings.md cannot disagree. Colours
are the validated dark-mode categorical pair (blue, orange) with recessive
hairline chrome; text never wears a series colour.
"""

from __future__ import annotations

import calendar
import datetime as dt
import os
import pathlib

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import psycopg2  # noqa: E402

DSN = os.environ.get("SERVING_DSN", "postgresql://energy:energy@localhost:5432/serving")
OUT = pathlib.Path(__file__).resolve().parents[1] / "docs" / "images"

BG = "#111217"  # chart surface
INK = "#ffffff"  # primary text
SECONDARY = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
BASELINE = "#383835"
SERIES_1 = "#3987e5"  # blue
SERIES_2 = "#d95926"  # orange
WASH = 0.10  # area fills are a wash, never a block

MIDDAY = (11, 14)
EVENING = (18, 20)


def fetch(query: str) -> list[tuple]:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def new_figure(width: float = 10, height: float = 4.8):
    fig, ax = plt.subplots(figsize=(width, height), dpi=150, facecolor=BG)
    return fig, ax


def style(ax, title: str, subtitle: str, ylabel: str = "€/MWh") -> None:
    ax.set_facecolor(BG)
    ax.set_title(title, color=INK, fontsize=13, loc="left", pad=26, fontweight="bold")
    ax.text(0, 1.035, subtitle, transform=ax.transAxes, color=SECONDARY, fontsize=9.5)
    ax.set_ylabel(ylabel, color=SECONDARY, fontsize=9.5)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=MUTED, labelsize=9, length=0, pad=6)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)


def hour_axis(ax) -> None:
    ax.set_xlim(-0.5, 23.5)
    ax.set_xticks(range(0, 24, 3), [f"{h:02d}:00" for h in range(0, 24, 3)])
    ax.set_xlabel("hour of day (Europe/Berlin)", color=SECONDARY, fontsize=9.5)


def window_band(ax, window: tuple[int, int], label: str) -> None:
    """Shade an inclusive hour window and name it at the top of the plot."""
    start, end = window
    ax.axvspan(start - 0.5, end + 0.5, color=SECONDARY, alpha=0.07, linewidth=0)
    ax.text(
        (start + end) / 2,
        0.97,
        label,
        transform=ax.get_xaxis_transform(),
        ha="center",
        va="top",
        color=SECONDARY,
        fontsize=9,
    )


def end_dot(ax, x: float, y: float, color: str) -> None:
    ax.scatter([x], [y], s=46, color=color, edgecolors=BG, linewidths=2, zorder=5)


def save(fig, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    fig.savefig(path, facecolor=BG, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"wrote {path}")


def day_text(day: dt.date, year: bool = False) -> str:
    # %-d is not portable (Windows), so the day number is formatted separately.
    return f"{day.day} {day:%b %Y}" if year else f"{day.day} {day:%b}"


def window_text(first: dt.date, last: dt.date, hours: int) -> str:
    span = f"{day_text(first)} – {day_text(last, year=True)}"
    return f"DE-LU day-ahead, {span}, {hours:,} hours · SMARD.de"


def main() -> None:
    first, last, total = fetch(
        "SELECT min(delivery_ts AT TIME ZONE 'Europe/Berlin')::date, "
        "       max(delivery_ts AT TIME ZONE 'Europe/Berlin')::date, count(*) "
        "FROM serving.price_hourly"
    )[0]
    subtitle = window_text(first, last, total)

    hours = fetch(
        "SELECT local_hour, avg(price_eur_mwh)::float "
        "FROM serving.price_hourly GROUP BY 1 ORDER BY 1"
    )
    evening, midday = fetch(
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
    weekend_midday = fetch(
        "SELECT avg(price_eur_mwh)::float FROM serving.price_hourly "
        "WHERE is_weekend AND local_hour BETWEEN 11 AND 14"
    )[0][0]
    monthly = fetch(
        "SELECT to_char(delivery_ts AT TIME ZONE 'Europe/Berlin', 'YYYY-MM'), "
        "  count(*) FILTER (WHERE is_negative), avg(price_eur_mwh)::float "
        "FROM serving.price_hourly GROUP BY 1 ORDER BY 1"
    )

    # 1) Duck curve: the headline windows are averages, so they are drawn as bands
    xs, ys = [r[0] for r in hours], [r[1] for r in hours]
    fig, ax = new_figure()
    window_band(ax, MIDDAY, f"midday 11–14h\navg €{midday:.0f}")
    window_band(ax, EVENING, f"evening 18–20h\navg €{evening:.0f}")
    ax.fill_between(xs, ys, 0, color=SERIES_1, alpha=WASH, linewidth=0)
    ax.plot(xs, ys, color=SERIES_1, linewidth=2, solid_capstyle="round")
    low = min(range(len(ys)), key=ys.__getitem__)
    high = max(range(len(ys)), key=ys.__getitem__)
    for i, va, dy in ((low, "top", -12), (high, "bottom", 10)):
        end_dot(ax, xs[i], ys[i], SERIES_1)
        ax.annotate(
            f"€{ys[i]:.0f} at {xs[i]:02d}:00",
            (xs[i], ys[i]),
            xytext=(0, dy),
            textcoords="offset points",
            ha="center",
            va=va,
            color=INK,
            fontsize=9,
        )
    ax.set_ylim(0, max(ys) * 1.3)
    hour_axis(ax)
    style(ax, "The duck curve, priced: cheap at midday, expensive in the evening", subtitle)
    save(fig, "findings_duck_curve.png")

    # 2) Week rhythm: weekday against weekend, with the cheapest window called out
    fig, ax = new_figure()
    window_band(ax, MIDDAY, f"weekend midday\navg €{weekend_midday:.2f}")
    hrs = [r[0] for r in rhythm]
    series = (
        ("weekday", [r[1] for r in rhythm], SERIES_1),
        ("weekend", [r[2] for r in rhythm], SERIES_2),
    )
    for label, values, color in series:
        ax.plot(hrs, values, color=color, linewidth=2, label=label, solid_capstyle="round")
        end_dot(ax, hrs[-1], values[-1], color)
    ends = sorted(series, key=lambda s: s[1][-1], reverse=True)
    for (label, values, _), dy in zip(ends, (7, -7), strict=True):
        ax.annotate(
            label,
            (hrs[-1], values[-1]),
            xytext=(10, dy),
            textcoords="offset points",
            va="center",
            color=SECONDARY,
            fontsize=9,
        )
    ax.axhline(0, color=BASELINE, linewidth=1)
    ax.legend(loc="upper left", frameon=False, labelcolor=SECONDARY, fontsize=9, handlelength=1.6)
    ax.set_ylim(min(0, *(v for _, s, _ in series for v in s)) - 5, None)
    hour_axis(ax)
    ax.set_xlim(-0.5, 25.5)
    style(ax, "The week rhythm: weekend prices collapse around midday", subtitle)
    save(fig, "findings_week_rhythm.png")

    # 3) Monthly: two single-measure panels, never a shared dual axis
    labels = []
    for i, (month, _, _) in enumerate(monthly):
        name = dt.date.fromisoformat(f"{month}-01").strftime("%b %Y")
        month_end = calendar.monthrange(last.year, last.month)[1]
        partial = (i == 0 and first.day > 1) or (i == len(monthly) - 1 and last.day < month_end)
        labels.append(f"{name}*" if partial else name)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.4), dpi=150, facecolor=BG)
    negative = [r[1] for r in monthly]
    bars = ax1.bar(labels, negative, color=SERIES_1, width=0.5)
    ax1.bar_label(bars, labels=[f"{n} h" for n in negative], color=INK, fontsize=9, padding=4)
    ax1.set_ylim(0, max(negative) * 1.2 if negative else 1)
    style(ax1, "Hours priced below zero", "per month", "")
    prices = [r[2] for r in monthly]
    ax2.plot(labels, prices, color=SERIES_1, linewidth=2, solid_capstyle="round")
    for x, y in zip(labels, prices, strict=True):
        end_dot(ax2, x, y, SERIES_1)
        ax2.annotate(
            f"€{y:.0f}",
            (x, y),
            xytext=(0, 9),
            textcoords="offset points",
            ha="center",
            color=INK,
            fontsize=9,
        )
    ax2.set_ylim(0, max(prices) * 1.25)
    ax2.margins(x=0.12)
    style(ax2, "Average price", "per month, €/MWh", "")
    fig.text(
        0.01,
        -0.02,
        f"* partial month: the window runs {day_text(first)} – {day_text(last, year=True)}",
        color=MUTED,
        fontsize=8.5,
    )
    fig.tight_layout()
    save(fig, "findings_monthly.png")


if __name__ == "__main__":
    main()
