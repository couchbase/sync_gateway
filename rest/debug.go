package rest

import (
	"bytes"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	_ "github.com/couchbase/gomemcached/debug"
	"github.com/samuel/go-metrics/metrics"

	"github.com/couchbase/sync_gateway/base"
)

const (
	kDebugURLPathPrefix    = "/_expvar"
	kMaxGoroutineSnapshots = 100000
)

var (
	poolhistos = map[string]metrics.Histogram{}
	opshistos  = map[string]metrics.Histogram{}
	histosMu   = sync.Mutex{}

	expPoolHistos *expvar.Map
	expOpsHistos  *expvar.Map

	grTracker *goroutineTracker
)

func init() {
	couchbase.ConnPoolCallback = recordConnPoolStat
	couchbase.ClientOpCallback = recordCBClientStat

	expCb := expvar.NewMap("cb")
	expPoolHistos = &expvar.Map{}
	expPoolHistos.Init()
	expCb.Set("pools", expPoolHistos)

	expOpsHistos = &expvar.Map{}
	expOpsHistos.Init()
	expCb.Set("ops", expOpsHistos)

	grTracker = &goroutineTracker{}
	expvar.Publish("goroutine_stats", grTracker)

}

type goroutineTracker struct {
	HighWaterMark uint64       // max number of goroutines seen thus far
	Snapshots     []uint64     // history of number of goroutines in system
	mutex         sync.RWMutex // mutex lock
}

func (g *goroutineTracker) String() string {

	g.mutex.RLock()
	defer g.mutex.RUnlock()

	buf := bytes.Buffer{}
	encoder := json.NewEncoder(&buf)
	err := encoder.Encode(g)
	if err != nil {
		return fmt.Sprintf("Error encoding json: %v", err)
	}
	return buf.String()

}

func (g *goroutineTracker) recordSnapshot() {

	g.mutex.Lock()
	defer g.mutex.Unlock()

	// get number of goroutines
	numGoroutines := uint64(runtime.NumGoroutine())

	// bump the high water mark
	if numGoroutines > g.HighWaterMark {
		g.HighWaterMark = numGoroutines

		varInt := expvar.Int{}
		varInt.Set(int64(numGoroutines))
		base.StatsExpvars.Set("goroutines_highWaterMark", &varInt)
	}

	// append to history
	g.Snapshots = append(g.Snapshots, numGoroutines)

	// drop the oldest one if we've gone over the max
	if len(g.Snapshots) > kMaxGoroutineSnapshots {
		g.Snapshots = g.Snapshots[1:]
	}

}

func connPoolHisto(name string) metrics.Histogram {
	histosMu.Lock()
	defer histosMu.Unlock()
	rv, ok := poolhistos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		poolhistos[name] = rv

		expPoolHistos.Set(name, &metrics.HistogramExport{
			Histogram:       rv,
			Percentiles:     []float64{0.25, 0.5, 0.75, 0.90, 0.99},
			PercentileNames: []string{"p25", "p50", "p75", "p90", "p99"}})
	}
	return rv
}

func recordConnPoolStat(host string, source string, start time.Time, err error) {
	duration := time.Since(start)
	histo := connPoolHisto(host)
	histo.Update(int64(duration))
}

func clientCBHisto(name string) metrics.Histogram {
	histosMu.Lock()
	defer histosMu.Unlock()
	rv, ok := opshistos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		opshistos[name] = rv

		expOpsHistos.Set(name, &metrics.HistogramExport{
			Histogram:       rv,
			Percentiles:     []float64{0.25, 0.5, 0.75, 0.90, 0.99},
			PercentileNames: []string{"p25", "p50", "p75", "p90", "p99"}})
	}
	return rv
}

func recordCBClientStat(opname, k string, start time.Time, err error) {
	duration := time.Since(start)
	histo := clientCBHisto(opname)
	histo.Update(int64(duration))
}

func (h *handler) handleExpvar() error {
	base.LogTo("HTTP", "Recording snapshot of current debug variables.")
	grTracker.recordSnapshot()
	h.rq.URL.Path = strings.Replace(h.rq.URL.Path, kDebugURLPathPrefix, "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(h.response, h.rq)
	return nil
}
