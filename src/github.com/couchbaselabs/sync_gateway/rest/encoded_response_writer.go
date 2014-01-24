package rest

import (
	"compress/gzip"
	"net/http"
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
)

const kZipperCacheCapacity = 20

var zipperCache chan *gzip.Writer

func init() {
	zipperCache = make(chan *gzip.Writer, kZipperCacheCapacity)
}

// An implementation of http.ResponseWriter that wraps another instance and transparently applies
// GZip compression when appropriate.
type EncodedResponseWriter struct {
	http.ResponseWriter
	gz        *gzip.Writer
	status    int
	sniffDone bool
}

// Creates a new EncodedResponseWriter, or returns nil if the request doesn't allow encoded responses.
func NewEncodedResponseWriter(response http.ResponseWriter, rq *http.Request) *EncodedResponseWriter {
	if !strings.Contains(rq.Header.Get("Accept-Encoding"), "gzip") ||
		rq.Method == "HEAD" || rq.Method == "PUT" || rq.Method == "DELETE" {
		return nil
	}
	return &EncodedResponseWriter{ResponseWriter: response}
}

func (w *EncodedResponseWriter) WriteHeader(status int) {
	w.status = status
	w.sniff(nil) // Must do it now because headers can't be changed after WriteHeader call
	w.ResponseWriter.WriteHeader(status)
}

func (w *EncodedResponseWriter) Write(b []byte) (int, error) {
	w.sniff(b)
	if w.gz != nil {
		return w.gz.Write(b)
	} else {
		return w.ResponseWriter.Write(b)
	}
}

func (w *EncodedResponseWriter) disableCompression() {
	if w.sniffDone {
		base.Warn("EncodedResponseWriter: Too late to disableCompression!")
	}
	w.sniffDone = true
}

func (w *EncodedResponseWriter) sniff(bytes []byte) {
	if w.sniffDone {
		return
	}
	w.sniffDone = true
	// Check the content type, sniffing the initial data if necessary:
	respType := w.Header().Get("Content-Type")
	if respType == "" && bytes != nil {
		respType = http.DetectContentType(bytes)
		w.Header().Set("Content-Type", respType)
	}

	// Can/should we compress the response?
	if w.status >= 300 || w.Header().Get("Content-Encoding") != "" ||
		(!strings.HasPrefix(respType, "application/json") && !strings.HasPrefix(respType, "text/")) {
		return
	}

	// OK, we can compress the response:
	//base.LogTo("REST+", "GZip-compressing response")
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length") // length is unknown due to compression

	// Get a gzip writer from the cache, or create a new one if it's empty:
	select {
	case w.gz = <-zipperCache:
		w.gz.Reset(w.ResponseWriter)
	default:
		w.gz = gzip.NewWriter(w.ResponseWriter)
	}
}

// Flushes the GZip encoder buffer, and if possible flushes output to the network.
func (w *EncodedResponseWriter) Flush() {
	if w.gz != nil {
		w.gz.Flush()
	}
	switch r := w.ResponseWriter.(type) {
	case http.Flusher:
		r.Flush()
	}
}

// The writer should be closed when output is complete, to flush the GZip encoder buffer.
func (w *EncodedResponseWriter) Close() {
	if w.gz != nil {
		w.gz.Close()

		// Return the gzip writer to the cache, or discard it if the cache is full:
		select {
		case zipperCache <- w.gz:
		default:
		}

		w.gz = nil
	}
}
