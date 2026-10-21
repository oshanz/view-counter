#!/usr/bin/env python3
"""Generates signoz/view-counter.json (SigNoz v6 dashboard) for DB, pool and system metrics."""
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


# Row 1: connection pool and DB calls (from the service).
panel("Pool connections", "pgx pool: connections by state, against the configured max.", [
    metric("pgxpool.acquired_connections", "avg", legend="acquired", letter="A"),
    metric("pgxpool.idle_connections", "avg", legend="idle", letter="B"),
    metric("pgxpool.total_connections", "avg", legend="total", letter="C"),
    metric("pgxpool.max_connections", "avg", legend="max", letter="D")], col=0)
panel("Pool contention", "Acquires that had to wait for a free connection, and cancelled acquires (per sec).", [
    metric("pgxpool.empty_acquire", "rate", legend="waited", letter="A"),
    metric("pgxpool.canceled_acquires", "rate", legend="cancelled", letter="B")], col=1)
panel("DB call p95 latency", "p95 duration of Postgres client spans, by operation (sampled traces).", [
    trace("p95(durationNano)", "db.system = 'postgresql'", ["name"], "{{name}}")], unit="ns", col=0)
panel("DB call rate", "Postgres client spans per second, by operation (sampled traces).", [
    trace("rate()", "db.system = 'postgresql'", ["name"], "{{name}}")], col=1)
panel("Slowest queries", "Postgres client spans by operation: count, p95 and max duration (sampled traces).", [
    trace("count()", "db.system = 'postgresql'", ["name"], "Calls", "A"),
    trace("p95(durationNano)", "db.system = 'postgresql'", ["name"], "p95 ns", "B"),
    trace("max(durationNano)", "db.system = 'postgresql'", ["name"], "max ns", "C")], table=True, col=0)
panel("Pool acquire wait", "Total time spent waiting for a pool connection (sec per sec).", [
    metric("pgxpool.empty_acquire_wait_time", "rate", legend="wait", letter="A")], unit="ns", col=1)

# Row 2: Postgres server.
panel("Server connections", "Open backends against max_connections.", [
    metric("postgresql.backends", "avg", group=["postgresql.database.name"], legend="{{postgresql.database.name}}", letter="A"),
    metric("postgresql.connection.max", "avg", legend="max", letter="B")], col=0)
panel("Locks: deadlocks and rollbacks", "Deadlocks and rolled-back transactions per second.", [
    metric("postgresql.deadlocks", "rate", legend="deadlocks", letter="A"),
    metric("postgresql.rollbacks", "rate", legend="rollbacks", letter="B")], col=1)
panel("Commits", "Committed transactions per second.", [
    metric("postgresql.commits", "rate", legend="commits")], col=0)
panel("Row operations", "Inserts, updates, deletes per second.", [
    metric("postgresql.operations", "rate", group=["operation"], legend="{{operation}}")], col=1)
panel("Disk blocks read", "Blocks read per second, by source (heap, index, ...).", [
    metric("postgresql.blocks_read", "rate", group=["source"], legend="{{source}}")], col=0)
panel("Table size", "On-disk size per table.", [
    metric("postgresql.table.size", "avg", group=["postgresql.table.name"], legend="{{postgresql.table.name}}")],
    unit="bytes", col=1)

# Row 3: system and containers.
panel("Host CPU", "Host CPU utilization by state.", [
    metric("system.cpu.utilization", "avg", flt="state != 'idle'", group=["state"], legend="{{state}}")],
    unit="percentunit", col=0)
panel("Host memory", "Host memory utilization by state.", [
    metric("system.memory.utilization", "avg", group=["state"], legend="{{state}}")],
    unit="percentunit", col=1)
panel("Container CPU", "CPU utilization per container.", [
    metric("container.cpu.utilization", "avg", group=["container.name"], legend="{{container.name}}")],
    unit="percent", col=0)
panel("Container memory", "Memory usage per container.", [
    metric("container.memory.usage.total", "avg", group=["container.name"], legend="{{container.name}}")],
    unit="bytes", col=1)
panel("Go heap and goroutines (service)", "Heap in use and goroutine count of the viewcounter process.", [
    metric("go.memory.used", "avg", legend="memory used (bytes)", letter="A"),
    metric("go.goroutine.count", "avg", legend="goroutines", letter="B")], col=0)

dash = {"schemaVersion": "v6", "generateName": True,
        "tags": [{"key": "tag", "value": "postgres"}, {"key": "tag", "value": "view-counter"}],
        "spec": {"display": {"name": "View counter: DB, pool and system",
                             "description": "Postgres queries, locks and connections, the pgx pool, host and container CPU and memory."},
                 "variables": [], "panels": panels,
                 "layouts": [{"kind": "Grid", "spec": {"items": items}}],
                 "duration": "", "refreshInterval": "", "links": []}}
with open("signoz/view-counter.json", "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
print(len(panels), "panels")
