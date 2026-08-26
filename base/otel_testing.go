//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// InstallTestTracerProvider installs a recording TracerProvider for the duration of the test, and
// returns the recorder holding completed spans.
func InstallTestTracerProvider(t testing.TB) *tracetest.SpanRecorder {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	previouslyEnabled := tracingEnabled.Load()
	otel.SetTracerProvider(tp)
	tracingEnabled.Store(true)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		tracingEnabled.Store(previouslyEnabled)
	})
	return recorder
}

// InstallTestTracerProviderWithSampler is InstallTestTracerProvider with an explicit sampler, for
// exercising sampling behaviour.
func InstallTestTracerProviderWithSampler(t testing.TB, sampler sdktrace.Sampler) *tracetest.SpanRecorder {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithSampler(sampler),
	)

	previous := otel.GetTracerProvider()
	previouslyEnabled := tracingEnabled.Load()
	otel.SetTracerProvider(tp)
	tracingEnabled.Store(true)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		tracingEnabled.Store(previouslyEnabled)
	})
	return recorder
}

// SetTracingEnabledForTest toggles the tracing-enabled flag for the duration of the test. Only
// needed when installing a TracerProvider directly rather than via the helpers above.
func SetTracingEnabledForTest(t testing.TB, enabled bool) {
	previous := tracingEnabled.Load()
	tracingEnabled.Store(enabled)
	t.Cleanup(func() { tracingEnabled.Store(previous) })
}
