"""The provisioned Grafana dashboard loads, uses only provisioned data sources, and has no
overlapping panels (Grafana silently shifts overlapping panels, which breaks the layout)."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROVISIONING = ROOT / "docker" / "grafana" / "provisioning"
DASHBOARD = json.loads((PROVISIONING / "dashboards" / "de-energy.json").read_text("utf-8"))


def test_every_panel_uses_a_provisioned_datasource():
    # CI's unit-test job has no YAML parser; the uid lines are simple enough for a regex.
    datasources = (PROVISIONING / "datasources" / "datasources.yml").read_text("utf-8")
    uids = set(re.findall(r"^\s+uid:\s*(\S+)", datasources, flags=re.MULTILINE))
    assert uids == {"energy-postgres", "prometheus"}

    for panel in DASHBOARD["panels"]:
        assert panel["datasource"]["uid"] in uids, panel["title"]
        assert panel["targets"], f"{panel['title']} has no query"


def test_panels_fit_the_24_column_grid_without_overlapping():
    cells: dict[tuple[int, int], str] = {}
    for panel in DASHBOARD["panels"]:
        pos = panel["gridPos"]
        assert pos["x"] + pos["w"] <= 24, panel["title"]
        for x in range(pos["x"], pos["x"] + pos["w"]):
            for y in range(pos["y"], pos["y"] + pos["h"]):
                assert (x, y) not in cells, f"{panel['title']} overlaps {cells[(x, y)]}"
                cells[(x, y)] = panel["title"]


def test_panel_ids_are_unique_and_the_range_includes_published_day_ahead_hours():
    ids = [panel["id"] for panel in DASHBOARD["panels"]]

    assert len(ids) == len(set(ids))
    assert DASHBOARD["time"]["to"] == "now+1d"
