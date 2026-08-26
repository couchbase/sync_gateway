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
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// tracerName identifies this instrumentation library in emitted spans.
const tracerName = "github.com/couchbase/sync_gateway"

// OTel PoC environment variables. Tracing is off unless SG_OTEL_ENABLED is truthy, in which case
// no TracerProvider is registered and the OTel global no-op implementation is used throughout.
const (
	envOtelEnabled  = "SG_OTEL_ENABLED"
	envOtelEndpoint = "SG_OTEL_ENDPOINT"
	envOtelRatio    = "SG_OTEL_SAMPLE_RATIO"
	envOtelStdout   = "SG_OTEL_STDOUT"
	envOtelInbound  = "SG_OTEL_PROPAGATE_INBOUND"

	defaultOtelEndpoint = "localhost:4317"
)

// TracingEnabled reports whether a real TracerProvider was installed by InitTracing.
func TracingEnabled() bool {
	return tracingEnabled.Load()
}

var tracingEnabled atomic.Bool

// propagateInbound controls whether a traceparent header on an incoming request is honoured. Off by
// default: accepting it lets the caller choose this server's trace IDs and sampling decision, which
// is only appropriate when the caller is part of the same system.
var propagateInbound atomic.Bool

// noopSpan is returned by StartSpan while tracing is disabled, so the disabled path allocates
// nothing and does not touch the OTel global provider.
var noopSpan = noop.Span{}

// InitTracing installs a global TracerProvider when SG_OTEL_ENABLED is set, and returns a shutdown
// function that flushes any buffered spans. When disabled it is a no-op, leaving the OTel global
// no-op provider in place.
func InitTracing(ctx context.Context) (shutdown func(context.Context) error, err error) {
	noopShutdown := func(context.Context) error { return nil }

	if enabled, _ := strconv.ParseBool(os.Getenv(envOtelEnabled)); !enabled {
		return noopShutdown, nil
	}

	ratio := 1.0
	if v := os.Getenv(envOtelRatio); v != "" {
		ratio, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return noopShutdown, fmt.Errorf("invalid %s %q: %w", envOtelRatio, v, err)
		}
	}

	exporter, err := newSpanExporter(ctx)
	if err != nil {
		return noopShutdown, err
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("sync_gateway"),
		semconv.ServiceVersion(ProductVersion.String()),
		semconv.ServiceInstanceID(hostnameOrUnknown()),
	))
	if err != nil {
		return noopShutdown, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// W3C trace context, so SG can join a trace started elsewhere and pass it on to anything it calls
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	tracingEnabled.Store(true)

	if inbound, _ := strconv.ParseBool(os.Getenv(envOtelInbound)); inbound {
		propagateInbound.Store(true)
		InfofCtx(ctx, KeyAll, "OpenTelemetry inbound trace context propagation enabled")
	}

	InfofCtx(ctx, KeyAll, "OpenTelemetry tracing enabled (endpoint=%s sample_ratio=%v)", MD(otelEndpoint()), ratio)

	return func(ctx context.Context) error {
		tracingEnabled.Store(false)
		return tp.Shutdown(ctx)
	}, nil
}

func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	if stdoutOnly, _ := strconv.ParseBool(os.Getenv(envOtelStdout)); stdoutOnly {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	return otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otelEndpoint()),
		otlptracegrpc.WithInsecure(),
	)
}

func otelEndpoint() string {
	if endpoint := os.Getenv(envOtelEndpoint); endpoint != "" {
		return endpoint
	}
	return defaultOtelEndpoint
}

func hostnameOrUnknown() string {
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	return "unknown"
}

// StartSpan begins a span named name as a child of any span already on ctx. When tracing is
// disabled this resolves to the OTel no-op tracer, so the cost is a context lookup.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !tracingEnabled.Load() {
		return ctx, noopSpan
	}
	if len(attrs) == 0 {
		return otel.Tracer(tracerName).Start(ctx, name)
	}
	return otel.Tracer(tracerName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// EndSpan completes span, recording err against it if non-nil. Intended for use as
// `defer func() { base.EndSpan(span, err) }()` alongside a named error return.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// SpanFromContext returns the span on ctx, which is the OTel no-op span if there isn't one.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// otelTracer and otelTracerProvider expose the globals for the gocb adapter's OtelAware interface.
func otelTracer() trace.Tracer { return otel.Tracer(tracerName) }

func otelTracerProvider() trace.TracerProvider { return otel.GetTracerProvider() }

// TraceSpan runs fn inside a child span named name, recording any error it returns against the
// span. Convenient for wrapping a phase of a larger operation without restructuring it.
func TraceSpan(ctx context.Context, name string, fn func(context.Context) error) (err error) {
	ctx, span := StartSpan(ctx, name)
	defer func() { EndSpan(span, err) }()
	return fn(ctx)
}

// RecordSpanError marks span as failed without ending it.
func RecordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// DetachSpan returns a copy of ctx with no active span, keeping all other context values. Use it
// for work that outlives the operation being traced - background goroutines, long-lived feeds - so
// their spans start new traces instead of attaching to a trace that has already ended.
func DetachSpan(ctx context.Context) context.Context {
	return trace.ContextWithSpan(ctx, noopSpan)
}

// StartDetachedSpan begins a span at the root of a NEW trace, linked back to the span on
// parentCtx. Use it for work that outlives its caller - background jobs, long-lived feeds. The new
// trace stands alone, so it cannot grow unbounded inside the caller's trace, and the link records
// what started it. Jaeger shows the link under the span's References.
//
// The caller should record the returned span's TraceID on its own span so navigation works in both
// directions; Jaeger does not render links in reverse.
func StartDetachedSpan(parentCtx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx := DetachSpan(parentCtx)
	if !tracingEnabled.Load() {
		return ctx, noopSpan
	}
	opts := []trace.SpanStartOption{trace.WithAttributes(attrs...)}
	if link := trace.LinkFromContext(parentCtx); link.SpanContext.IsValid() {
		opts = append(opts, trace.WithLinks(link))
	}
	return otel.Tracer(tracerName).Start(ctx, name, opts...)
}

// StartKVSpan begins a span for a single KV operation, recording the document ID. The gocb SDK
// deliberately omits document keys from its own spans, so this is where they come from.
func StartKVSpan(ctx context.Context, op, key string) (context.Context, trace.Span) {
	if !tracingEnabled.Load() {
		return ctx, noopSpan
	}
	start := time.Now()
	ctx, span := StartSpan(ctx, "sgw.kv."+op, attribute.String("sgw.doc_id", key))
	// accumulate total KV time across the request, for its Server-Timing header
	kvCtx := ctx
	return ctx, timedSpan{Span: span, done: func() { RecordPhase(kvCtx, "kv", time.Since(start)) }}
}

// timedSpan records a phase duration when the span ends.
type timedSpan struct {
	trace.Span
	done func()
}

func (t timedSpan) End(opts ...trace.SpanEndOption) {
	t.done()
	t.Span.End(opts...)
}

// SetDocIDAttr records a document ID on the span already on ctx.
func SetDocIDAttr(ctx context.Context, docID string) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String("sgw.doc_id", docID))
	}
}

// SpanLinkFromContext captures a link to the span on ctx, for later use with StartSpanWithLink.
func SpanLinkFromContext(ctx context.Context) trace.Link {
	return trace.LinkFromContext(ctx)
}

// StartSpanWithLink begins a span with an additional link. Combined with a ctx whose span has been
// detached, this starts a new trace that still records what it belongs to.
func StartSpanWithLink(ctx context.Context, name string, link trace.Link, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !tracingEnabled.Load() {
		return ctx, noopSpan
	}
	opts := []trace.SpanStartOption{trace.WithAttributes(attrs...)}
	if link.SpanContext.IsValid() {
		opts = append(opts, trace.WithLinks(link))
	}
	return otel.Tracer(tracerName).Start(ctx, name, opts...)
}

// SpanContextFromContext returns the span context of the span on ctx, for propagating or reporting
// trace identifiers. Invalid when there is no recording span.
func SpanContextFromContext(ctx context.Context) trace.SpanContext {
	return trace.SpanContextFromContext(ctx)
}

// ExtractInboundTraceContext returns a context carrying the trace context from an incoming request's
// headers, so spans started from it join the caller's trace. Returns ctx unchanged unless
// SG_OTEL_PROPAGATE_INBOUND is set.
func ExtractInboundTraceContext(ctx context.Context, header http.Header) context.Context {
	if !tracingEnabled.Load() || !propagateInbound.Load() {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(header))
}

// InjectTraceContext writes the current trace context into outgoing request headers, so a
// downstream service can continue the trace.
func InjectTraceContext(ctx context.Context, header http.Header) {
	if !tracingEnabled.Load() {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
}

// phaseTimings accumulates per-phase durations for one request, for the Server-Timing header.
type phaseTimings struct {
	mu sync.Mutex
	d  map[string]time.Duration
}

type phaseTimingsKey struct{}

// WithPhaseTimings returns a context that collects per-phase durations. Work done under it can call
// RecordPhase; the originating request reads them back with PhaseTimings.
func WithPhaseTimings(ctx context.Context) context.Context {
	return context.WithValue(ctx, phaseTimingsKey{}, &phaseTimings{d: make(map[string]time.Duration, 4)})
}

// RecordPhase adds d to the named phase, if ctx is collecting them. Phases accumulate, so a phase
// entered several times reports its total.
func RecordPhase(ctx context.Context, name string, d time.Duration) {
	pt, _ := ctx.Value(phaseTimingsKey{}).(*phaseTimings)
	if pt == nil {
		return
	}
	pt.mu.Lock()
	pt.d[name] += d
	pt.mu.Unlock()
}

// PhaseTimings returns a copy of the durations collected on ctx.
func PhaseTimings(ctx context.Context) map[string]time.Duration {
	pt, _ := ctx.Value(phaseTimingsKey{}).(*phaseTimings)
	if pt == nil {
		return nil
	}
	pt.mu.Lock()
	defer pt.mu.Unlock()
	out := make(map[string]time.Duration, len(pt.d))
	maps.Copy(out, pt.d)
	return out
}

// ServerTimingHeader renders phases and the current trace context as a Server-Timing header value.
// Returns "" when there is nothing to report.
func ServerTimingHeader(ctx context.Context, total time.Duration) string {
	var parts []string
	if total > 0 {
		parts = append(parts, fmt.Sprintf("total;dur=%.1f", float64(total.Microseconds())/1000))
	}
	timings := PhaseTimings(ctx)
	for _, name := range slices.Sorted(maps.Keys(timings)) {
		parts = append(parts, fmt.Sprintf("%s;dur=%.1f", name, float64(timings[name].Microseconds())/1000))
	}
	// The documented way to expose trace context to a browser: Server-Timing is readable from
	// JavaScript via PerformanceServerTiming, arbitrary response headers are not.
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		parts = append(parts, fmt.Sprintf(`traceparent;desc="00-%s-%s-%s"`,
			sc.TraceID(), sc.SpanID(), sc.TraceFlags()))
	}
	return strings.Join(parts, ", ")
}

// SetSpanBoolAttr records a boolean attribute on the span already on ctx.
func SetSpanBoolAttr(ctx context.Context, key string, value bool) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.Bool(key, value))
	}
}
