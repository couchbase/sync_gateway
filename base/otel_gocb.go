//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// GocbTracer adapts the gocb SDK's tracing hook onto OpenTelemetry, so that the SDK's internal
// spans (dispatch, encode, server duration) hang off whichever SG span is on the caller's context.
// gocb reaches SG's parent span via the ParentSpan field on its per-operation options structs -
// see GocbParentSpan.
type GocbTracer struct{}

var (
	_ gocb.RequestTracer          = &GocbTracer{}
	_ gocb.OtelAwareRequestTracer = &GocbTracer{}
	_ gocb.RequestSpan            = &gocbSpan{}
	_ gocb.OtelAwareRequestSpan   = &gocbSpan{}
	_ gocb.RequestSpan            = suppressedSpan{}
)

func NewGocbTracer() *GocbTracer {
	return &GocbTracer{}
}

func (t *GocbTracer) RequestSpan(parentContext gocb.RequestSpanContext, operationName string) gocb.RequestSpan {
	// gocb hands back whatever we supplied as ParentSpan, or the Context() of a span it created
	// itself for a nested operation. An unrecognised parent means the KV op happened outside any SG
	// span, so emit nothing rather than an orphaned root span for every op in the process.
	parent, ok := parentContext.(*gocbSpan)
	if !ok {
		return suppressedSpan{}
	}

	ctx, span := StartSpan(parent.ctx, operationName)
	return &gocbSpan{ctx: ctx, span: span}
}

func (t *GocbTracer) Wrapped() trace.Tracer {
	return otelTracer()
}

func (t *GocbTracer) Provider() trace.TracerProvider {
	return otelTracerProvider()
}

type gocbSpan struct {
	ctx  context.Context
	span trace.Span
}

func (s *gocbSpan) End() {
	s.span.End()
}

func (s *gocbSpan) Context() gocb.RequestSpanContext {
	return s
}

func (s *gocbSpan) AddEvent(name string, timestamp time.Time) {
	s.span.AddEvent(name, trace.WithTimestamp(timestamp))
}

func (s *gocbSpan) SetAttribute(key string, value any) {
	if !s.span.IsRecording() {
		return
	}
	s.span.SetAttributes(gocbAttribute(key, value))
}

func (s *gocbSpan) Wrapped() trace.Span {
	return s.span
}

// GocbParentSpan wraps ctx so it can be handed to gocb as the ParentSpan of an operation. Returns
// nil when ctx carries no recording span, so gocb skips span creation entirely.
func GocbParentSpan(ctx context.Context) gocb.RequestSpan {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return nil
	}
	return &gocbSpan{ctx: ctx, span: span}
}

// gocbAttribute converts gocb's any-typed attribute values into typed OTel attributes.
func gocbAttribute(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case uint64:
		return attribute.Int64(key, int64(v))
	case float64:
		return attribute.Float64(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}

// suppressedSpan stands in for a KV operation that occurred outside any SG span. It deliberately is
// not a *gocbSpan: gocb hands a span back as the parent of the operation's child spans, and any
// parent RequestSpan recognises starts a new root trace - which would leave every unparented KV op
// emitting orphaned CMD_* and dispatch_to_server traces.
type suppressedSpan struct{}

func (suppressedSpan) End() {}

func (s suppressedSpan) Context() gocb.RequestSpanContext { return s }

func (suppressedSpan) AddEvent(string, time.Time) {}

func (suppressedSpan) SetAttribute(string, any) {}
