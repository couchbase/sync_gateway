// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package jsbench is a standalone module (isolated from the main sync_gateway module, similar to
// ../ruleguard) comparing otto (the JS engine sync_gateway used prior to CBG-5592) against goja
// (its replacement) for the sync function workload: compiling a function into a fresh runtime,
// and repeatedly calling an already-compiled function against a warm runtime. It's isolated so
// that otto -- fully removed from the main module as part of CBG-5592 -- doesn't need to come
// back as a dependency of sync_gateway just to keep this comparison around.
//
// Run with: go test -run xxx -bench=. -benchmem .
//
// Results (AMD Ryzen 9 3950X 16-Core, go1.26.5, linux/amd64):
//
//	BenchmarkCompile/Otto/Tiny-32         	    9468	    110580 ns/op	  155183 B/op	    1673 allocs/op
//	BenchmarkCompile/Goja/Tiny-32         	  118567	      9775 ns/op	   10080 B/op	     117 allocs/op
//	BenchmarkCompile/Otto/Small-32        	    9236	    116568 ns/op	  159130 B/op	    1771 allocs/op
//	BenchmarkCompile/Goja/Small-32        	   55801	     21080 ns/op	   17376 B/op	     232 allocs/op
//	BenchmarkCompile/Otto/Medium-32       	    7966	    134473 ns/op	  165603 B/op	    1925 allocs/op
//	BenchmarkCompile/Goja/Medium-32       	   30050	     39895 ns/op	   30352 B/op	     434 allocs/op
//	BenchmarkCompile/Otto/Large-32        	    1915	    642591 ns/op	  357206 B/op	    6621 allocs/op
//	BenchmarkCompile/Goja/Large-32        	    2002	    568543 ns/op	  417610 B/op	    5520 allocs/op
//	BenchmarkCall/Otto/Tiny-32            	  377562	      3008 ns/op	    2728 B/op	      43 allocs/op
//	BenchmarkCall/Goja/Tiny-32            	 1765952	       685.5 ns/op	     608 B/op	      10 allocs/op
//	BenchmarkCall/Otto/Small-32           	  269343	      4390 ns/op	    3480 B/op	      63 allocs/op
//	BenchmarkCall/Goja/Small-32           	 1253538	       954.8 ns/op	     624 B/op	      11 allocs/op
//	BenchmarkCall/Otto/Medium-32          	   21802	     55141 ns/op	   33314 B/op	     788 allocs/op
//	BenchmarkCall/Goja/Medium-32          	   57534	     21273 ns/op	   17528 B/op	     238 allocs/op
//	BenchmarkCall/Otto/Large-32           	    2004	    600626 ns/op	  347407 B/op	    9626 allocs/op
//	BenchmarkCall/Goja/Large-32           	    4269	    285612 ns/op	  305170 B/op	    3728 allocs/op
//
// Goja compiles ~2-11x faster than otto except at the "Large" size, where the two are close
// (goja's parser/compiler does more upfront work per byte of source, so it stops being a clear
// win once the source itself dominates). Goja calls a warm, already-compiled function ~2-4x
// faster than otto across all sizes, with meaningfully fewer allocations throughout.
package jsbench

import (
	"testing"
)

type sizeCase struct {
	name   string
	source string
	doc    map[string]interface{}
}

func sizeCases() []sizeCase {
	return []sizeCase{
		{"Tiny", tinySource, tinyDoc},
		{"Small", smallSource, smallDoc},
		{"Medium", mediumSource, mediumDoc},
		{"Large", largeSource, largeDoc},
	}
}

var emptyMeta = map[string]interface{}{}

// BenchmarkCompile measures the cost of compiling a sync function and creating a fresh runtime
// (the "cold start" / cache-miss path: otto.New()+Object() vs goja.New()+RunString()).
func BenchmarkCompile(b *testing.B) {
	for _, sz := range sizeCases() {
		sz := sz
		b.Run("Otto/"+sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				newOttoRunner(sz.source)
			}
		})
		b.Run("Goja/"+sz.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				newGojaRunner(sz.source)
			}
		})
	}
}

// BenchmarkCall measures the cost of repeatedly invoking an already-compiled sync function
// against the same runtime (the "warm" / pooled-runner path used in production).
func BenchmarkCall(b *testing.B) {
	for _, sz := range sizeCases() {
		sz := sz
		b.Run("Otto/"+sz.name, func(b *testing.B) {
			r := newOttoRunner(sz.source)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r.Call(sz.doc, nil, emptyMeta)
			}
		})
		b.Run("Goja/"+sz.name, func(b *testing.B) {
			r := newGojaRunner(sz.source)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r.Call(sz.doc, nil, emptyMeta)
			}
		})
	}
}
