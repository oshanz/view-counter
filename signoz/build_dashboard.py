#!/usr/bin/env python3
"""Generates signoz/view-counter.json (SigNoz v6 dashboard) for DB, pool, system and NATS metrics."""
from dash import metric, trace, panel, write

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

# Row 4: NATS and JetStream (scraped from prometheus-nats-exporter).
panel("NATS stream messages", "Messages held in each JetStream stream.", [
    metric("jetstream_stream_total_messages", "avg", group=["stream_name"], legend="{{stream_name}}")], col=1)
panel("NATS message rate", "Messages received and sent by the server per second.", [
    metric("gnatsd_varz_in_msgs", "rate", legend="in", letter="A"),
    metric("gnatsd_varz_out_msgs", "rate", legend="out", letter="B")], col=0)
panel("NATS consumer backlog", "Messages not yet delivered, and delivered but not acked, per consumer.", [
    metric("jetstream_consumer_num_pending", "avg", group=["consumer_name"], legend="pending {{consumer_name}}", letter="A"),
    metric("jetstream_consumer_num_ack_pending", "avg", group=["consumer_name"], legend="ack pending {{consumer_name}}", letter="B")], col=1)
panel("NATS stream size", "Bytes stored per JetStream stream.", [
    metric("jetstream_stream_total_bytes", "avg", group=["stream_name"], legend="{{stream_name}}")],
    unit="bytes", col=0)
panel("NATS connections", "Open client connections.", [
    metric("gnatsd_varz_connections", "avg", legend="connections")], col=1)

write("signoz/view-counter.json", "View counter: DB, pool and system",
      "Postgres queries, locks and connections, the pgx pool, host and container CPU and memory, NATS and JetStream.",
      ["postgres", "nats", "view-counter"])
