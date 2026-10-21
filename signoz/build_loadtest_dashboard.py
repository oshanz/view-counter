#!/usr/bin/env python3
"""Generates signoz/loadtest.json (SigNoz v6 dashboard) for finding the write bottleneck under load."""
from dash import metric, hist, trace, panel, write

HIT = "http.route = '/v2/hit'"

# Row 1: what the load looks like from the HTTP side.
panel("Requests per second", "Server requests per second by route (all requests, not sampled).", [
    metric("http.server.request.duration.count", "rate", group=["http.route"], legend="{{http.route}}")], col=0)
panel("Request latency (/v2/hit)", "p50, p95 and p99 server-side duration of POST /v2/hit.", [
    hist("http.server.request.duration.bucket", "p50", HIT, legend="p50", letter="A"),
    hist("http.server.request.duration.bucket", "p95", HIT, legend="p95", letter="B"),
    hist("http.server.request.duration.bucket", "p99", HIT, legend="p99", letter="C")], unit="s", col=1)
panel("Responses by status", "Requests per second by status code.", [
    metric("http.server.request.duration.count", "rate", group=["http.response.status_code"],
           legend="{{http.response.status_code}}")], col=0)
panel("p95 latency by route", "p95 server-side duration per route.", [
    hist("http.server.request.duration.bucket", "p95", group=["http.route"], legend="{{http.route}}")],
    unit="s", col=1)

# Row 2: is the writer keeping up.
panel("Ingest backlog", "pending: published, not yet fetched. ack_pending: fetched, not yet committed.", [
    metric("ingest.pending", "avg", legend="pending", letter="A"),
    metric("ingest.ack_pending", "avg", legend="ack pending", letter="B")], col=0)
panel("Writer throughput", "Rows committed per second by Postgres (inserts on seen and page_views).", [
    metric("postgresql.operations", "rate", group=["operation"], legend="{{operation}}")], col=1)
panel("Writer transaction latency", "p95 duration of Postgres client spans from the writer (sampled traces).", [
    trace("p95(durationNano)", "db.system = 'postgresql'", ["name"], "{{name}}")], unit="ns", col=0)
panel("Pool contention", "Acquires that had to wait for a free connection (per sec).", [
    metric("pgxpool.empty_acquire", "rate", legend="waited", letter="A"),
    metric("pgxpool.canceled_acquires", "rate", legend="cancelled", letter="B")], col=1)

# Row 3: which process or resource is saturated.
panel("Container CPU", "CPU utilization per container (NATS and Postgres). k6 and the service are on the host.", [
    metric("container.cpu.utilization", "avg", group=["container.name"], legend="{{container.name}}")],
    unit="percent", col=0)
panel("Host CPU", "Host CPU by state. Near 100% busy means k6, service, NATS and Postgres compete.", [
    metric("system.cpu.utilization", "avg", flt="state != 'idle'", group=["state"], legend="{{state}}")],
    unit="percentunit", col=1)
panel("Go allocation rate", "Bytes allocated per second by the service. High values mean GC pressure.", [
    metric("go.memory.allocated", "rate", legend="allocated")], unit="bytes", col=0)
panel("Go heap, GC goal and goroutines", "Heap in use, next GC target and goroutine count.", [
    metric("go.memory.used", "avg", legend="heap used", letter="A"),
    metric("go.memory.gc.goal", "avg", legend="gc goal", letter="B"),
    metric("go.goroutine.count", "avg", legend="goroutines", letter="C")], col=1)
panel("Container network", "Bytes per second in and out of the NATS and Postgres containers.", [
    metric("container.network.io.usage.rx_bytes", "rate", group=["container.name"], legend="rx {{container.name}}", letter="A"),
    metric("container.network.io.usage.tx_bytes", "rate", group=["container.name"], legend="tx {{container.name}}", letter="B")],
    unit="bytes", col=0)
panel("Slowest spans", "Server spans by route: count, p95 and max (sampled traces).", [
    trace("count()", "kind_string = 'Server'", ["name"], "Calls", "A"),
    trace("p95(durationNano)", "kind_string = 'Server'", ["name"], "p95 ns", "B"),
    trace("max(durationNano)", "kind_string = 'Server'", ["name"], "max ns", "C")], table=True, col=1)

write("signoz/loadtest.json", "View counter: write load test",
      "HTTP rate and latency, ingest backlog, writer and Postgres throughput, CPU and Go runtime during a load test.",
      ["view-counter", "loadtest"])
