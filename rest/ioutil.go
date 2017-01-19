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
	LogKey string  // The log key to use, eg "HTTP++"
}

func NewLoggerTeeResponseWriter(wrappedResponseWriter http.ResponseWriter, logKey string) http.ResponseWriter {
	return &LoggingTeeResponseWriter{
		ResponseWriter: wrappedResponseWriter,
		LogKey: logKey,
	}
}

func (l *LoggingTeeResponseWriter) Write(b []byte) (int, error) {
	base.LogTo(l.LogKey, string(b))
	return l.ResponseWriter.Write(b)
}



