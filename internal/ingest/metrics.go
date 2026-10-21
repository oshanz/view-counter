package ingest

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// ObserveLag registers gauges for the consumer lag on the global meter
// provider. Without a configured provider they are no-ops.
func (q *Queue) ObserveLag() error {
	m := otel.Meter("viewcounter/ingest")
	pending, err := m.Int64ObservableGauge("ingest.pending",
		metric.WithDescription("Hits published and not yet delivered to the writer."))
	if err != nil {
		return err
	}
	ackPending, err := m.Int64ObservableGauge("ingest.ack_pending",
		metric.WithDescription("Hits delivered to the writer and not yet committed."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		lag, err := q.Lag(ctx)
		if err != nil {
			return nil // skip this sample rather than fail the export
		}
		o.ObserveInt64(pending, int64(lag.Pending))
		o.ObserveInt64(ackPending, int64(lag.AckPending))
		return nil
	}, pending, ackPending)
	return err
}
