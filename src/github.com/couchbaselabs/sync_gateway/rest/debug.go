package rest

import (
	"expvar"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/samuel/go-metrics/metrics"

	"github.com/couchbaselabs/sync_gateway/base"
)

const kDebugURLPathPrefix = "/_expvar"

var (
	histos   = map[string]metrics.Histogram{}
	histosMu = sync.Mutex{}

	expHistos *expvar.Map
)

func init() {
	couchbase.ConnPoolCallback = recordConnPoolStat

	expCb := expvar.NewMap("cb")
	expHistos = &expvar.Map{}
	expHistos.Init()
	expCb.Set("pools", expHistos)
}

func connPoolHisto(name string) metrics.Histogram {
	histosMu.Lock()
	defer histosMu.Unlock()
	rv, ok := histos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		histos[name] = rv

		expHistos.Set(name, &metrics.HistogramExport{rv,
			[]float64{0.25, 0.5, 0.75, 0.90, 0.99},
			[]string{"p25", "p50", "p75", "p90", "p99"}})
	}
	return rv
}

func recordConnPoolStat(host string, source string, start time.Time, err error) {
	duration := time.Since(start)
	histo := connPoolHisto(host)
	histo.Update(int64(duration))
}

func (h *handler) handleExpvar() error {
	base.LogTo("HTTP", "debuggin'")
	h.rq.URL.Path = strings.Replace(h.rq.URL.Path, kDebugURLPathPrefix, "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(h.response, h.rq)
	return nil
}
