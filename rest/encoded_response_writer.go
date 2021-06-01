/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
)

// An implementation of http.ResponseWriter that wraps another instance and transparently applies
// GZip compression when appropriate.
type EncodedResponseWriter struct {
	http.ResponseWriter
	gz            *gzip.Writer
	status        int
	sniffDone     bool
	headerWritten bool
}

// Creates a new EncodedResponseWriter, or returns nil if the request doesn't allow encoded responses.
func NewEncodedResponseWriter(response http.ResponseWriter, rq *http.Request) *EncodedResponseWriter {
	isWebSocketRequest := strings.ToLower(rq.Header.Get("Upgrade")) == "websocket" &&
		strings.Contains(strings.ToLower(rq.Header.Get("Connection")), "upgrade")

	if isWebSocketRequest || !strings.Contains(rq.Header.Get("Accept-Encoding"), "gzip") ||
		rq.Method == "HEAD" || rq.Method == "PUT" || rq.Method == "DELETE" {
		return nil
	}

	// Workaround for https://github.com/couchbase/sync_gateway/issues/1419
	// if the user agent is empty or earlier than 1.2, then we never want to gzip
	// the entire response for requests to the _bulk_get endpoint, since the clients
	// are not equipped to handle that.
	if strings.Contains(rq.URL.Path, "_bulk_get") {
		userAgentVersion := NewUserAgentVersion(rq.Header.Get("User-Agent"))
		if userAgentVersion.IsBefore(1, 2) {
			return nil
		}
	}

	return &EncodedResponseWriter{ResponseWriter: response}
}

func (w *EncodedResponseWriter) WriteHeader(status int) {
	w.status = status
	w.sniff(nil) // Must do it now because headers can't be changed after WriteHeader call
	if w.headerWritten {
		return
	}
	w.ResponseWriter.WriteHeader(status)
	w.headerWritten = true

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
		(!strings.HasPrefix(respType, "application/json") && !strings.HasPrefix(respType, "text/") && !strings.HasPrefix(respType, "multipart/mixed")) {
		return
	}

	// OK, we can compress the response:
	//base.Debugf(base.KeyHTTP, "GZip-compressing response")
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length") // length is unknown due to compression

	w.gz = GetGZipWriter(w.ResponseWriter)
}

// Flushes the GZip encoder buffer, and if possible flushes output to the network.
func (w *EncodedResponseWriter) Flush() {
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	switch r := w.ResponseWriter.(type) {
	case http.Flusher:
		r.Flush()
	}
}

// The writer should be closed when output is complete, to flush the GZip encoder buffer.
func (w *EncodedResponseWriter) Close() {
	if w.gz != nil {
		ReturnGZipWriter(w.gz)
		w.gz = nil
	}
}

func (w *EncodedResponseWriter) CloseNotify() <-chan bool {
	var closeNotify <-chan bool
	cn, ok := w.ResponseWriter.(http.CloseNotifier)
	if ok {
		closeNotify = cn.CloseNotify()
	}
	return closeNotify
}

//////// GZIP WRITER CACHE:

var zipperCache sync.Pool

// Gets a gzip writer from the pool, or creates a new one if the pool is empty:
func GetGZipWriter(writer io.Writer) *gzip.Writer {
	if gz, ok := zipperCache.Get().(*gzip.Writer); ok {
		gz.Reset(writer)
		return gz
	} else {
		return gzip.NewWriter(writer)
	}
}

// Closes a gzip writer and returns it to the pool:
func ReturnGZipWriter(gz *gzip.Writer) {
	_ = gz.Close()
	zipperCache.Put(gz)
}
