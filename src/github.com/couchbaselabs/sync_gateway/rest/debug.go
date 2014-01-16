package rest

import (
	"encoding/json"
	"expvar"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
	"github.com/samuel/go-metrics/metrics"

	"github.com/couchbaselabs/sync_gateway/base"
)

const kDebugURLPathPrefix = "/_expvar"

var (
	poolhistos = map[string]metrics.Histogram{}
	opshistos  = map[string]metrics.Histogram{}
	histosMu   = sync.Mutex{}

	expPoolHistos *expvar.Map
	expOpsHistos  *expvar.Map
)

type mcops [257]uint64

func (m *mcops) String() string {
	j := map[string]uint64{}
	total := uint64(0)
	for i, v := range *m {
		if v > 0 {
			k := "error"
			if i < 256 {
				k = gomemcached.CommandCode(i).String()
			}
			j[k] = v
			total += v
		}
	}
	j["total"] = total
	b, err := json.Marshal(j)
	if err != nil {
		panic(err) // shouldn't be possible
	}
	return string(b)
}

func (m *mcops) countReq(req *gomemcached.MCRequest, n int, err error) {
	i := 256
	if req != nil {
		i = int(req.Opcode)
	}
	atomic.AddUint64(&m[i], uint64(n))
}

func (m *mcops) countRes(res *gomemcached.MCResponse, n int, err error) {
	i := 256
	if res != nil {
		i = int(res.Opcode)
	}
	atomic.AddUint64(&m[i], uint64(n))
}

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

	// Lower-level memcached stats

	mcSent := &mcops{}
	mcRecvd := &mcops{}
	tapRecvd := &mcops{}

	memcached.TransmitHook = mcSent.countReq
	memcached.ReceiveHook = mcRecvd.countRes
	memcached.TapRecvHook = tapRecvd.countReq

	mcStats := expvar.NewMap("mc")
	mcStats.Set("xmit", mcSent)
	mcStats.Set("recv", mcRecvd)
	mcStats.Set("tap", tapRecvd)
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
	base.LogTo("HTTP", "debuggin'")
	h.rq.URL.Path = strings.Replace(h.rq.URL.Path, kDebugURLPathPrefix, "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(h.response, h.rq)
	return nil
}
