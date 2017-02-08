package rest

import (
	"github.com/couchbase/sync_gateway/base"
	"io"

	"net/http"
)

// A TeeReader wrapper that can wrap an io.ReadCloser as opposed to just
// wrapping an io.Reader
type TeeReadCloser struct {
	r io.Reader
	w io.Writer
	c io.Closer
}

func NewTeeReadCloser(r io.ReadCloser, w io.Writer) io.ReadCloser {
	return &TeeReadCloser{
		r: io.TeeReader(r, w),
		w: w,
		c: r,
	}
}

func (t *TeeReadCloser) Read(b []byte) (int, error) {
	return t.r.Read(b)
}

func (t *TeeReadCloser) Close() error {
	return t.c.Close()
}

// A ResponseWriter that wraps another ResponseWriter, and dumps a copy of everything to
// the logging key
type LoggingTeeResponseWriter struct {
	http.ResponseWriter
	LogKey       string        // The log key to use, eg "HTTP+"
	SerialNumber uint64        // The request ID
	Request      *http.Request // The request
}

func NewLoggerTeeResponseWriter(wrappedResponseWriter http.ResponseWriter, logKey string, serialNum uint64, req *http.Request) http.ResponseWriter {
	return &LoggingTeeResponseWriter{
		ResponseWriter: wrappedResponseWriter,
		LogKey:         logKey,
		SerialNumber:   serialNum,
		Request:        req,
	}
}

func (l *LoggingTeeResponseWriter) Write(b []byte) (int, error) {
	base.LogTo(l.LogKey, " #%03d: %s %s %s", l.SerialNumber, l.Request.Method, base.SanitizeRequestURL(l.Request.URL), string(b))
	return l.ResponseWriter.Write(b)
}
