package rest

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

type CountableResponseWriter interface {
	http.ResponseWriter
	GetBytesWritten() int64
}

type CountedResponseWriter struct {
	writer       http.ResponseWriter
	bytesWritten int64
}

var _ CountedResponseWriter = CountedResponseWriter{}

func (w *CountedResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *CountedResponseWriter) Write(b []byte) (int, error) {
	n, err := w.writer.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

func (w *CountedResponseWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
}

func (w *CountedResponseWriter) GetBytesWritten() int64 {
	return w.bytesWritten
}

func (w *CountedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.writer.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter doesn't support http.Hijacker interface")
	}
	return h.Hijack()
}
