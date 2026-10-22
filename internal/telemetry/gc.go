package telemetry

import (
	"context"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// observeGC reports stop-the-world GC pauses, which the runtime instrumentation
// leaves out.
func observeGC() error {
	m := otel.Meter("viewcounter/gc")
	total, err := m.Float64ObservableCounter("go.gc.pause.total",
		metric.WithUnit("s"),
		metric.WithDescription("Cumulative stop-the-world GC pause time."))
	if err != nil {
		return err
	}
	last, err := m.Float64ObservableGauge("go.gc.pause.last",
		metric.WithUnit("s"),
		metric.WithDescription("Most recent stop-the-world GC pause."))
	if err != nil {
		return err
	}
	cycles, err := m.Int64ObservableCounter("go.gc.cycles",
		metric.WithDescription("Completed GC cycles."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var s debug.GCStats
		s.Pause = make([]time.Duration, 1)
		debug.ReadGCStats(&s)
		o.ObserveFloat64(total, s.PauseTotal.Seconds())
		o.ObserveInt64(cycles, s.NumGC)
		if len(s.Pause) > 0 {
			o.ObserveFloat64(last, s.Pause[0].Seconds())
		}
		return nil
	}, total, last, cycles)
	return err
}
