#!/usr/bin/env python3
"""Shared helpers for the SigNoz v6 dashboard generators in this directory."""
import json
import uuid

panels = {}
items = []
row = {"y": 0}


def gb(name):
    return {"name": name, "signal": "", "fieldContext": "attribute", "fieldDataType": "string"}


def metric(name, kind, flt="", group=None, legend="", letter="A"):
    counter = kind == "rate"
    return {
        "name": letter, "stepInterval": 60, "signal": "metrics", "source": "",
        "aggregations": [{
            "metricName": name, "temporality": "",
            "timeAggregation": "rate" if counter else "avg",
            "spaceAggregation": "sum" if counter else "avg",
            "reduceTo": "avg",
        }],
        "disabled": False, "filter": {"expression": flt},
        "groupBy": [gb(g) for g in (group or [])], "order": [],
        "having": {"expression": ""}, "legend": legend,
    }


def trace(expr, flt, group, legend, letter="A"):
    return {
        "name": letter, "stepInterval": 60, "signal": "traces", "source": "",
        "aggregations": [{"expression": expr}],
        "disabled": False, "filter": {"expression": flt},
        "groupBy": [gb(g) for g in group], "order": [],
        "having": {"expression": ""}, "legend": legend,
    }


def panel(title, desc, queries, unit="none", table=False, col=0):
    pid = str(uuid.uuid4())
    if table:
        plugin = {"kind": "signoz/TablePanel", "spec": {
            "visualization": {"timePreference": "global_time"},
            "formatting": {"columnUnits": None, "decimalPrecision": "2"}, "thresholds": None}}
        qs = [{"kind": "scalar", "spec": {"plugin": {"kind": "signoz/CompositeQuery", "spec": {
            "queries": [{"type": "builder_query", "spec": q} for q in queries]}}}}]
    else:
        plugin = {"kind": "signoz/TimeSeriesPanel", "spec": {
            "visualization": {"timePreference": "global_time", "fillSpans": False},
            "formatting": {"unit": unit, "decimalPrecision": "2"},
            "chartAppearance": {"lineInterpolation": "spline", "showPoints": False,
                                "lineStyle": "solid", "fillMode": "none",
                                "spanGaps": {"fillOnlyBelow": False, "fillLessThan": ""}},
            "axes": {"softMin": None, "softMax": None, "isLogScale": False},
            "legend": {"position": "bottom", "mode": "list", "customColors": None},
            "thresholds": None}}
        if len(queries) == 1:
            qs = [{"kind": "time_series", "spec": {"name": queries[0]["name"], "plugin": {
                "kind": "signoz/BuilderQuery", "spec": queries[0]}}}]
        else:
            # A panel holds exactly one query entry; several builder queries go in a composite.
            qs = [{"kind": "time_series", "spec": {"plugin": {"kind": "signoz/CompositeQuery", "spec": {
                "queries": [{"type": "builder_query", "spec": q} for q in queries]}}}}]
    panels[pid] = {"kind": "Panel", "spec": {
        "display": {"name": title, "description": desc}, "plugin": plugin, "queries": qs, "links": []}}
    items.append({"x": col * 6, "y": row["y"], "width": 6, "height": 3,
                  "content": {"$ref": f"#/spec/panels/{pid}"}})
    if col == 1:
        row["y"] += 3




def hist(name, space, flt="", group=None, legend="", letter="A"):
    """Histogram metric: per-second rate of buckets, aggregated to a percentile (p50..p99)."""
    q = metric(name, "avg", flt, group, legend, letter)
    q["aggregations"][0].update({"timeAggregation": "rate", "spaceAggregation": space})
    return q


def write(path, name, description, tags):
    dash = {"schemaVersion": "v6", "generateName": True,
            "tags": [{"key": "tag", "value": t} for t in tags],
            "spec": {"display": {"name": name, "description": description},
                     "variables": [], "panels": panels,
                     "layouts": [{"kind": "Grid", "spec": {"items": items}}],
                     "duration": "", "refreshInterval": "", "links": []}}
    with open(path, "w") as f:
        json.dump(dash, f, indent=2)
        f.write("\n")
    print(len(panels), "panels")
