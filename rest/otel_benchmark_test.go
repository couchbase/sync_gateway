//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// tracingModes are the sampling configurations compared by the benchmarks below. "off" leaves the
// OTel global no-op provider in place, which is how Sync Gateway runs today.
var tracingModes = []struct {
	name    string
	sampler sdktrace.Sampler // nil means leave tracing disabled
}{
	{name: "Off", sampler: nil},
	{name: "Sampled_1in1000", sampler: sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.001))},
	{name: "Full", sampler: sdktrace.ParentBased(sdktrace.AlwaysSample())},
}

// installExportingTracerProvider installs a provider that batches and serializes every span, then
// discards the bytes. Unlike the in-memory recorder used by the other modes this includes
// marshalling and the batch queue, so it approximates what a real collector pipeline costs in
// process. It does not include network cost or collector-side backpressure.
func installExportingTracerProvider(tb testing.TB) {
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(io.Discard))
	require.NoError(tb, err)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	base.SetTracingEnabledForTest(tb, true)
	tb.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
}

// BenchmarkBlipPushRevsWithExport is BenchmarkBlipPushRevs at full sampling with real span
// serialization, to separate span creation cost from export cost.
func BenchmarkBlipPushRevsWithExport(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelNone, base.KeyNone)
	installExportingTracerProvider(b)

	bt := NewBlipTesterFromSpec(b, BlipTesterSpec{
		GuestEnabled: true,
		syncFn:       `function(doc) { channel(doc.channels); }`,
	})
	defer bt.Close()

	body := []byte(`{"channels":["a"],"val":"benchmark payload"}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		bt.SendRev(fmt.Sprintf("doc-%d", i), "1-abc", body, blip.Properties{})
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "revs/sec")
}

// BenchmarkBlipPushRevs measures rev throughput over BLIP at each sampling rate, which is the
// number needed to judge whether replication-path tracing is affordable. Spans are collected in
// memory rather than exported, so this isolates in-process cost from collector and network cost.
func BenchmarkBlipPushRevs(b *testing.B) {
	for _, mode := range tracingModes {
		b.Run(mode.name, func(b *testing.B) {
			base.SetUpBenchmarkLogging(b, base.LevelNone, base.KeyNone)
			if mode.sampler != nil {
				base.InstallTestTracerProviderWithSampler(b, mode.sampler)
			}

			bt := NewBlipTesterFromSpec(b, BlipTesterSpec{
				GuestEnabled: true,
				syncFn:       `function(doc) { channel(doc.channels); }`,
			})
			defer bt.Close()

			body := []byte(`{"channels":["a"],"val":"benchmark payload"}`)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				bt.SendRev(fmt.Sprintf("doc-%d", i), "1-abc", body, blip.Properties{})
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "revs/sec")
		})
	}
}

// BenchmarkRestApiPutDoc is the same comparison over the REST write path, which shares the sync
// function and KV spans with the BLIP path but not the per-message root span.
func BenchmarkRestApiPutDoc(b *testing.B) {
	for _, mode := range tracingModes {
		b.Run(mode.name, func(b *testing.B) {
			base.SetUpBenchmarkLogging(b, base.LevelNone, base.KeyNone)
			if mode.sampler != nil {
				base.InstallTestTracerProviderWithSampler(b, mode.sampler)
			}

			rt := NewRestTester(b, &RestTesterConfig{
				SyncFn: `function(doc) { channel(doc.channels); }`,
			})
			defer rt.Close()
			_ = rt.Bucket()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/doc-%d", i), `{"channels":["a"]}`)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
		})
	}
}

// BenchmarkSpanOverhead measures the marginal in-process cost of one span, separating the CPU cost
// of instrumentation from the export cost that dominates at replication volumes.
func BenchmarkSpanOverhead(b *testing.B) {
	for _, mode := range tracingModes {
		b.Run(mode.name, func(b *testing.B) {
			if mode.sampler != nil {
				base.InstallTestTracerProviderWithSampler(b, mode.sampler)
			}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, span := base.StartSpan(ctx, "sgw.bench")
				span.End()
			}
		})
	}
}

// BenchmarkNestedSpanOverhead approximates the per-rev span cost: one BLIP root plus the sync
// function and KV children a single rev produces.
func BenchmarkNestedSpanOverhead(b *testing.B) {
	for _, mode := range tracingModes {
		b.Run(mode.name, func(b *testing.B) {
			if mode.sampler != nil {
				base.InstallTestTracerProviderWithSampler(b, mode.sampler)
			}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				revCtx, rev := base.StartSpan(ctx, "blip.rev")
				for _, child := range []string{"sgw.sync_fn", "kv.lookup_in", "kv.mutate_in"} {
					_, span := base.StartSpan(revCtx, child)
					span.End()
				}
				rev.End()
			}
		})
	}
}
