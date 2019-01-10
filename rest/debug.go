package rest

import (
	"expvar"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	_ "github.com/couchbase/gomemcached/debug"
	"github.com/couchbase/sync_gateway/base"
	"github.com/samuel/go-metrics/metrics"
)

const (
	kDebugURLPathPrefix = "/_expvar"
)

var (
	poolhistos = map[string]metrics.Histogram{}
	opshistos  = map[string]metrics.Histogram{}
	histosMu   = sync.Mutex{}

	expPoolHistos *expvar.Map
	expOpsHistos  *expvar.Map
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
	base.Infof(base.KeyHTTP, "Recording snapshot of current debug variables.")
	h.rq.URL.Path = strings.Replace(h.rq.URL.Path, kDebugURLPathPrefix, "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(h.response, h.rq)
	return nil
}
