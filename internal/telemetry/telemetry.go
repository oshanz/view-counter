// Package telemetry wires OpenTelemetry traces and metrics to an OTLP
// collector such as SigNoz. It is off unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
package telemetry

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Enabled reports whether an OTLP endpoint is configured.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
}

// Setup installs global tracer and meter providers and returns a function that
// flushes and stops them. Sampling follows OTEL_TRACES_SAMPLER and
// OTEL_TRACES_SAMPLER_ARG (for example traceidratio and 0.01).
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(service)),
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))

	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	if err := runtime.Start(); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}
