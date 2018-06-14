package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sgPublicAddr    = "http://127.0.0.1:4984"
	sgAdminAddr     = "http://127.0.0.1:4985"
	db              = "db"
	workers         = 16
	timeLogInterval = time.Millisecond * 100 // Interval to print timing information
)

var (
	// Counter to keep track of docs written
	totalDocsWritten uint64
	// Use custom HTTP client to tweak MaxIdleConnsPerHost to avoid ephemeral port exhaustion
	httpClient = http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}
)

func main() {

	wg := sync.WaitGroup{}
	wg.Add(workers)

	fmt.Fprintf(os.Stderr, "Starting Write/Longpoll loop with %d workers.\n", workers)
	fmt.Fprintf(os.Stderr, "Logging to stdout in CSV format\n\n\n")

	// Print CSV header
	fmt.Fprintf(os.Stdout, "milliseconds,docs_written,docs_written_per_second\n")

	startTime := time.Now()

	for i := int64(0); i < workers; i++ {
		workerID := strconv.FormatInt(i, 10)
		go writeAndWaitLoop(&wg, workerID)
	}

	// Periodically log timing information
	ticker := time.NewTicker(timeLogInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				logTiming(startTime)
			}
		}
	}()

	wg.Wait()

}

func writeAndWaitLoop(wg *sync.WaitGroup, workerID string) {

	defer wg.Done()

	for i := int64(0); i < math.MaxInt64; i++ {
		key := workerID + ":" + strconv.FormatInt(i, 10)
		seq, err := writeDoc(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%2s - Error writing doc: %v - err: %v\n", workerID, key, err)
			continue
		}

		err = waitForDoc(key, seq)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%2s - Error waiting for doc: %v - seq: %v - err: %v\n", workerID, key, seq, err)
			continue
		}

		atomic.AddUint64(&totalDocsWritten, 1)
	}

}

// Writes a doc with the given key
func writeDoc(key string) (seq string, err error) {

	resp, err := httpClient.Post(
		sgAdminAddr+"/"+db,
		"application/json",
		bytes.NewBufferString(`{"_id":"`+key+`", "a":1}`),
	)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	log.Printf("body: %s\n", body)

	// data := map[string]interface{}{}
	// err = json.Unmarshal(body, &data)
	// if err != nil {
	// 	return "", err
	// }

	// log.Printf("data: %+v\n", data)

	// FIXME: hardcoded seq
	return "1", nil

}

// does a longpoll _changes request and blocks until complete
func waitForDoc(key, seq string) (err error) {

	resp, err := httpClient.Get(sgAdminAddr + "/" + db + "/" + "_changes?feed=longpoll&since=" + seq)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	_, err = io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// Print timing information in CSV format
func logTiming(startTime time.Time) {
	totalRuntime := time.Since(startTime)
	docs := atomic.LoadUint64(&totalDocsWritten)
	fmt.Fprintf(os.Stdout, "%v,%v,%v\n", time.Since(startTime).Nanoseconds()/int64(time.Millisecond), docs, float64(docs)/totalRuntime.Seconds())
}
