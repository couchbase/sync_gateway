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
	"errors"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func spanNames(recorder *tracetest.SpanRecorder) []string {
	names := make([]string, 0, len(recorder.Ended()))
	for _, s := range recorder.Ended() {
		names = append(names, s.Name())
	}
	return names
}

func TestStartSpanNoopWhenDisabled(t *testing.T) {
	ctx, span := StartSpan(context.Background(), "sgw.test")
	defer span.End()
	assert.False(t, span.IsRecording(), "expected a non-recording span with no provider installed")
	assert.False(t, span.SpanContext().IsValid())
	assert.Nil(t, GocbParentSpan(ctx), "GocbParentSpan should be nil for a non-recording span")
}

func TestStartSpanRecordsAndNests(t *testing.T) {
	recorder := InstallTestTracerProvider(t)

	parentCtx, parent := StartSpan(context.Background(), "sgw.parent")
	require.True(t, parent.IsRecording())
	_, child := StartSpan(parentCtx, "sgw.child")
	child.End()
	parent.End()

	ended := recorder.Ended()
	require.Len(t, ended, 2)
	assert.Equal(t, []string{"sgw.child", "sgw.parent"}, spanNames(recorder))
	assert.Equal(t, ended[1].SpanContext().SpanID(), ended[0].Parent().SpanID(), "child should be parented to sgw.parent")
	assert.Equal(t, ended[1].SpanContext().TraceID(), ended[0].SpanContext().TraceID(), "both spans share a trace")
}

func TestEndSpanRecordsError(t *testing.T) {
	recorder := InstallTestTracerProvider(t)

	_, span := StartSpan(context.Background(), "sgw.failing")
	EndSpan(span, errors.New("boom"))

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "Error", ended[0].Status().Code.String())
	require.Len(t, ended[0].Events(), 1)
	assert.Equal(t, "exception", ended[0].Events()[0].Name)
}

func TestTraceSpanPropagatesError(t *testing.T) {
	recorder := InstallTestTracerProvider(t)

	sentinel := errors.New("phase failed")
	err := TraceSpan(context.Background(), "sgw.phase", func(ctx context.Context) error {
		assert.True(t, SpanFromContext(ctx).IsRecording(), "phase should run with the span on its context")
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)

	require.Len(t, recorder.Ended(), 1)
	assert.Equal(t, "sgw.phase", recorder.Ended()[0].Name())
}

// TestGocbTracerParentsUnderSGSpan covers the gocb RequestTracer adapter: SDK spans should nest
// under the SG span on the caller's context, and be dropped entirely when there isn't one.
func TestGocbTracerParentsUnderSGSpan(t *testing.T) {
	recorder := InstallTestTracerProvider(t)
	tracer := NewGocbTracer()

	sgCtx, sgSpan := StartSpan(context.Background(), "sgw.write")
	parent := GocbParentSpan(sgCtx)
	require.NotNil(t, parent)

	kvSpan := tracer.RequestSpan(parent.Context(), "mutate_in")
	kvSpan.SetAttribute("db.couchbase.retries", 3)
	kvSpan.End()
	sgSpan.End()

	ended := recorder.Ended()
	require.Len(t, ended, 2)
	assert.Equal(t, "mutate_in", ended[0].Name())
	assert.Equal(t, ended[1].SpanContext().SpanID(), ended[0].Parent().SpanID())

	// No SG span on the context: gocb spans are dropped rather than becoming orphan roots.
	orphan := tracer.RequestSpan(nil, "get")
	orphan.End()
	assert.Len(t, recorder.Ended(), 2, "unparented gocb ops should not emit spans")
}

// TestGocbTracerSuppressesChildrenOfOrphanOps covers the spans gocb starts beneath an operation it
// has already been told to drop. Suppression has to reach them too - the SDK parents CMD_* and
// dispatch_to_server to whatever the operation span handed back, so a stand-in the tracer accepts
// as a parent turns every unparented KV op into an orphan trace of its own.
func TestGocbTracerSuppressesChildrenOfOrphanOps(t *testing.T) {
	recorder := InstallTestTracerProvider(t)
	tracer := NewGocbTracer()

	op := tracer.RequestSpan(nil, "lookup_in")
	encoding := tracer.RequestSpan(op.Context(), "request_encoding")
	encoding.End()
	cmd := tracer.RequestSpan(op.Context(), "CMD_SUBDOCMULTILOOKUP")
	dispatch := tracer.RequestSpan(cmd.Context(), "dispatch_to_server")
	dispatch.SetAttribute("db.couchbase.server_duration", 42)
	dispatch.End()
	cmd.End()
	op.End()

	assert.Empty(t, spanNames(recorder), "no span below an unparented KV op should be emitted")
}

// TestSamplingIsAllOrNothingPerTrace confirms the ParentBased sampler used by InitTracing keeps a
// trace whole: a sampled root keeps all its children, and a dropped root drops all of them. This is
// what makes low sample ratios usable on the replication path - you get complete traces for a small
// fraction of messages, not fragments of every message.
func TestSamplingIsAllOrNothingPerTrace(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sampler       sdktrace.Sampler
		expectedSpans int
	}{
		{"AlwaysSample", sdktrace.ParentBased(sdktrace.AlwaysSample()), 4},
		{"NeverSample", sdktrace.ParentBased(sdktrace.NeverSample()), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := InstallTestTracerProviderWithSampler(t, tc.sampler)

			revCtx, rev := StartSpan(context.Background(), "blip.rev")
			for _, child := range []string{"sgw.sync_fn", "kv.lookup_in", "kv.mutate_in"} {
				_, span := StartSpan(revCtx, child)
				span.End()
			}
			rev.End()

			assert.Len(t, recorder.Ended(), tc.expectedSpans)
		})
	}
}
