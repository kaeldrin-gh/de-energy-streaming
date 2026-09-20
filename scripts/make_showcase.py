"""Build the GitHub Pages showcase for de-energy-streaming.

Renders the live market pulse (SMARD prices + classified news headlines) and the
repository's findings charts and screenshots into a single static page. The
pulse is best-effort: when SMARD or the classifier are unavailable, the page
still ships the charts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import re
import shutil
from pathlib import Path

from producer.config import Settings
from producer.news import classify, default_feeds, fetch_feeds
from producer.smard import SmardClient
from producer.summary import render_news, render_summary

REPO_ROOT = Path(__file__).resolve().parents[1]

CHARTS = [
    ("findings_duck_curve.png", "Average price by hour - the duck curve, priced"),
    ("findings_week_rhythm.png", "Weekend vs weekday price profile"),
    ("findings_monthly.png", "Negative-price hours and average price by month"),
]

SCREENSHOTS = [
    ("grafana-dashboard.png", "Grafana dashboard (serving layer)"),
    ("airflow-dag-grid.png", "Airflow DAG grid (hourly Spark orchestration)"),
    ("spark-master.png", "Spark master UI (streaming app + batch jobs)"),
]

REPO_URL = "https://github.com/kaeldrin-gh/de-energy-streaming"


def inline_html(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    return re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)


def markdown_to_html(markdown: str) -> str:
    """Convert the small Markdown subset the market pulse emits."""
    output: list[str] = []
    in_table = False
    for line in markdown.splitlines():
        if line.startswith("|"):
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            if all(set(cell) <= set("-: ") for cell in cells):
                continue
            if not in_table:
                output.append("<table>")
                in_table = True
                row_tag = "th"
            else:
                row_tag = "td"
            row = "".join(f"<{row_tag}>{inline_html(cell)}</{row_tag}>" for cell in cells)
            output.append(f"<tr>{row}</tr>")
            continue
        if in_table:
            output.append("</table>")
            in_table = False
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("## "):
            output.append(f"<h2>{inline_html(stripped[3:])}</h2>")
        elif stripped.startswith("### "):
            output.append(f"<h3>{inline_html(stripped[4:])}</h3>")
        else:
            output.append(f"<p>{inline_html(stripped)}</p>")
    if in_table:
        output.append("</table>")
    return "\n".join(output)


def live_pulse() -> str:
    """The price pulse as HTML; a short note when SMARD is unavailable."""
    try:
        settings = Settings()
        client = SmardClient(settings.smard_base_url, settings.smard_filter, settings.smard_region)
        points = client.fetch_latest(weeks=3)
        return markdown_to_html(render_summary(points, news=None))
    except Exception:  # noqa: BLE001 - the page must render without the pulse
        return (
            "<p>Market pulse unavailable at build time. "
            f"See the workflow runs on <a href='{REPO_URL}/actions'>GitHub Actions</a>.</p>"
        )


def live_news() -> str:
    """The classified headlines block as HTML; a note when unavailable."""
    try:
        headlines = fetch_feeds(default_feeds())
        if not headlines:
            return "<p>No headlines available at build time.</p>"
        return markdown_to_html(render_news(classify(headlines), heading=None))
    except Exception:  # noqa: BLE001 - the page must render without the news
        return "<p>Headlines unavailable at build time.</p>"


def build_page(out_dir: Path) -> Path:
    images = REPO_ROOT / "docs" / "images"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, _ in CHARTS + SCREENSHOTS:
        source = images / name
        if source.exists():
            shutil.copy2(source, out_dir / name)

    def figures(items: list[tuple[str, str]]) -> str:
        blocks = []
        for name, caption in items:
            if (out_dir / name).exists():
                blocks.append(
                    f"<figure><img src='{name}' alt='{html.escape(caption)}'>"
                    f"<figcaption>{html.escape(caption)}</figcaption></figure>"
                )
        return "\n".join(blocks)

    generated = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
    page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>de-energy-streaming - showcase</title>
<style>
body {{ background: #0d1117; color: #c9d1d9; font-family: -apple-system, "Segoe UI", sans-serif;
       max-width: 980px; margin: 2rem auto; padding: 0 1rem; line-height: 1.5; }}
h1 {{ color: #fff; margin-bottom: 0.2rem; }}
h2 {{ color: #fff; margin-top: 2.2rem; border-bottom: 1px solid #30363d; padding-bottom: 6px; }}
a {{ color: #79c0ff; }}
.sub {{ color: #8b949e; }}
table {{ border-collapse: collapse; margin-top: 10px; }}
td, th {{ border: 1px solid #30363d; padding: 4px 12px; font-size: 0.92rem; }}
th {{ background: #161b22; }}
tr:nth-child(even) {{ background: #11151c; }}
code {{ background: #161b22; padding: 1px 5px; border-radius: 4px; }}
figure {{ margin: 1.2rem 0; }}
img {{ max-width: 100%; border: 1px solid #30363d; border-radius: 6px; }}
figcaption {{ color: #8b949e; font-size: 0.88rem; margin-top: 6px; }}
</style></head><body>
<h1>de-energy-streaming</h1>
<p class="sub">German day-ahead power prices through Kafka, Spark Structured Streaming and
Apache Iceberg, orchestrated with Airflow and served with PostgreSQL and Grafana.
Rebuilt daily; generated {generated}.</p>
<p><a href="{REPO_URL}">Repository</a> ·
<a href="{REPO_URL}#readme">README</a> ·
<a href="{REPO_URL}/blob/main/docs/operations.md">Operations runbook</a> ·
<a href="{REPO_URL}/blob/main/analysis/findings.md">Findings</a></p>

<h2>Market pulse (live)</h2>
{live_pulse()}

<h2>News context (live)</h2>
<p class="sub">Public energy-news headlines classified into topics by
<a href="https://classifier.dev">classifier.dev</a>; unrelated headlines are filtered out.</p>
{live_news()}

<h2>What the data says</h2>
{figures(CHARTS)}

<h2>Screenshots</h2>
{figures(SCREENSHOTS)}
</body></html>"""

    path = out_dir / "index.html"
    path.write_text(page, encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("site"), help="output directory")
    args = parser.parse_args()
    path = build_page(args.out)
    print(f"showcase written to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
