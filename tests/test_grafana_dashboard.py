"""grafana/home-assistant.json, the copy customers download, held to its shape.

The dashboard is executed against a real API in the product repository, where
its twin lives; that suite cannot see this file, and this one cannot reach an
API. What it can do is hold the published copy to every property that makes
the dashboard work and that a well-meaning edit here would quietly break: the
URLs stay relative and keep their date macros and decimation, the parser stays
the backend one, the tag list stays off the panels, the transformation maps
the columns its query actually reads, and nothing account-specific ships.

Not a test of this integration's Python - it lives in this suite because this
suite is the one CI runs for the repository root.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

DASHBOARD = Path(__file__).resolve().parent.parent / "grafana" / "home-assistant.json"


def load() -> dict:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def queries(dashboard: dict) -> list[tuple[str, dict]]:
    found = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            found.append((f"panel {panel['title']!r}", target))
    for variable in dashboard["templating"]["list"]:
        query = variable.get("query")
        if isinstance(query, dict) and "infinityQuery" in query:
            found.append((f"variable {variable['name']!r}", query["infinityQuery"]))
    return found


def test_the_published_dashboard_exists_and_has_its_queries():
    assert len(queries(load())) == 4


def test_every_url_is_relative_so_the_data_source_supplies_the_host():
    text = DASHBOARD.read_text(encoding="utf-8")
    assert "api.taghistorian.com" not in text
    for where, query in queries(load()):
        assert query["url"].startswith("/api/"), where


def test_chart_queries_use_the_iso_date_macros_and_decimation():
    charts = [q for _, q in queries(load()) if "maxPoints=" in q["url"]]
    assert len(charts) == 2
    for query in charts:
        assert "${__timeFrom:date}" in query["url"]
        assert "${__timeTo:date}" in query["url"]
        assert "decimation=lttb" in query["url"]


def test_every_query_runs_on_the_grafana_server():
    for where, query in queries(load()):
        assert query["parser"] == "backend", where


def test_the_tag_list_is_never_on_a_panel():
    for where, query in queries(load()):
        if where.startswith("panel"):
            assert not re.match(r"^/api/tags(\?|$)", query["url"]), where


def test_the_transformation_maps_exactly_what_its_query_reads():
    chart = next(p for p in load()["panels"] if p["type"] == "timeseries")
    options = chart["transformations"][0]["options"]
    config = next(t for t in chart["targets"] if t["refId"] == options["configRefId"])
    produced = {c["text"] for c in config["columns"]} | {c["text"] for c in config["computed_columns"]}
    mapped = {m["fieldName"] for m in options["mappings"]}
    assert produced == mapped
    handlers = {m["fieldName"]: m["handlerKey"] for m in options["mappings"]}
    assert handlers["units"] == "unit"
    assert handlers["name"] == "displayName"


def test_nothing_account_specific_ships():
    dashboard = load()
    customer = next(v for v in dashboard["templating"]["list"] if v["name"] == "customerId")
    assert customer["query"] == ""
    assert "Token " not in DASHBOARD.read_text(encoding="utf-8")
