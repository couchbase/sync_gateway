package rest

import (
	"compress/gzip"
	"net/http"
	"strings"
)

// An implementation of http.ResponseWriter that wraps another instance and transparently applies
// GZip compression when appropriate.
type EncodedResponseWriter struct {
	http.ResponseWriter
	gz        *gzip.Writer
	sniffDone bool
}

// Creates a new EncodedResponseWriter, or returns nil if the request doesn't allow encoded responses.
func NewEncodedResponseWriter(response http.ResponseWriter, rq *http.Request) *EncodedResponseWriter {
	if !strings.Contains(rq.Header.Get("Accept-Encoding"), "gzip") {
		return nil
	}
	return &EncodedResponseWriter{ResponseWriter: response}
}

func (w *EncodedResponseWriter) WriteHeader(status int) {
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

func (w *EncodedResponseWriter) sniff(bytes []byte) {
	if w.sniffDone {
		return
	}
	respType := w.Header().Get("Content-Type")
	if respType == "" && bytes != nil {
		respType = http.DetectContentType(bytes)
		w.Header().Set("Content-Type", respType)
	}
	if strings.HasPrefix(respType, "application/json") || strings.HasPrefix(respType, "text/") {
		if w.Header().Get("Content-Type") == "" {
			w.ResponseWriter.Header().Set("Content-Encoding", "gzip")
			w.gz = gzip.NewWriter(w.ResponseWriter)
		}
	}
	w.sniffDone = true
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
	}
}
