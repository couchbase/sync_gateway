package rest

import (
	"bytes"
	"expvar"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/goutils/logging"
	"github.com/couchbase/sync_gateway/base"
)

// insertBatchDocs will insert the given number of documents to the given database
// This may need chunking for very large numbers of docCount and a remote CB server.
func insertBatchDocs(rt *RestTester, db string, docCount int) error {
	buf := new(bytes.Buffer)

	for i := 0; i < docCount; i++ {
		_, err := buf.Write([]byte(fmt.Sprintf(`{"_id": "%d", "val":true}`, i)))
		if err != nil {
			return err
		}

		// Split docs by comma
		if i < docCount-1 {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
		}
	}

	reqStart := time.Now()
	response := rt.SendRequest("POST", "/db/_bulk_docs", `{"docs": [`+buf.String()+`]}`)
	reqEnd := time.Since(reqStart)
	if response.Code != http.StatusCreated {
		return fmt.Errorf("Error %d inserting bulk docs: %s", response.Code, response.Body.String())
	}

	fmt.Fprintf(os.Stderr, "Created %d docs via bulk in %v\n", docCount, reqEnd)

	return nil
}
func BenchmarkHighRevCacheMiss(b *testing.B) {
	// Disable logging for benchmark
	base.SetLogLevel(2)
	logging.SetLogger(nil)

	var benchmarks = []struct {
		NumOfReads  int
		UseRevCache bool
		Name        string
	}{
		// {500, true, "500 reads - 100% resident"},
		// {500, false, "500 reads - no revcache"},

		{5000, true, "5k reads - 100% resident"},
		{5000, false, "5k reads - no revcache"},

		// {10000, true, "10k reads - 50% resident"},
		// {10000, false, "10k reads - no revcache"},

		{100000, true, "100k reads - 5% resident"},
		{100000, false, "100k reads - no revcache"},

		// {1000000, true, "1m reads - .5% resident"},
		// {1000000, false, "1m reads - no revcache"},

		// {10000000, true, "10m reads - .05% resident"},
		// {10000000, false, "10m reads - no revcache"},
	}

	rt := RestTester{}

	// Insert 1m docs upfront before benchmarking.
	err := insertBatchDocs(&rt, "db", 1000000)
	if err != nil {
		b.FailNow()
	}

	err = rt.WaitForPendingChanges()
	if err != nil {
		b.FailNow()
	}

	for _, bm := range benchmarks {
		b.Run(bm.Name, func(bench *testing.B) {

			rt.GetDatabase().UseRevCache = bm.UseRevCache

			// We only ever want Go to run the benchmark through once.
			// We control NumOfReads separately.
			bench.N = 1

			start := time.Now()
			bench.ResetTimer()

			// Run bm.NumOfReads GETs
			bench.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for i := 0; i < bm.NumOfReads; i++ {
						id := randDocID(bm.NumOfReads)
						rt.SendRequest("GET", "/db/"+id, "")
					}
				}
			})

			bench.StopTimer()
			end := time.Since(start)

			hits, _ := strconv.ParseFloat(base.StatsExpvars.Get("revisionCache_hits").String(), 64)
			miss, _ := strconv.ParseFloat(base.StatsExpvars.Get("revisionCache_misses").String(), 64)
			base.StatsExpvars.Set("revisionCache_hits", &expvar.Int{})
			base.StatsExpvars.Set("revisionCache_misses", &expvar.Int{})

			fmt.Printf("\n\n\nreq/sec: %.0f (%d in %v) revcache miss:%.2f%% (hits: %.0f misses: %.0f)\n\n\n",
				float64(bm.NumOfReads)/end.Seconds(), bm.NumOfReads, end.Round(time.Millisecond*10), (miss/float64(bm.NumOfReads))*100, hits, miss)

		})
	}

}

func randDocID(numOfDocs int) string {
	return fmt.Sprintf("%d", rand.Intn(numOfDocs))
}
