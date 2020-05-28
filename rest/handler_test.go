package rest

import (
	"net/http"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestParseHTTPRangeHeader(t *testing.T) {
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	type testcase struct {
		header        string
		contentLength uint64
		status        int
		start         uint64
		end           uint64
	}
	testcases := []testcase{
		// No Range: header at all:
		{"", 100, http.StatusOK, 0, 0},

		// Syntactically invalid Range headers are ignored:
		{"lolwut", 100, http.StatusOK, 0, 0},
		{"inches=-", 100, http.StatusOK, 0, 0},
		{"bytes=-", 100, http.StatusOK, 0, 0},
		{"bytes=50-bar", 100, http.StatusOK, 0, 0},
		{"bytes=50-49", 100, http.StatusOK, 0, 0},                  // invalid, not unsatisfiable
		{"bytes=99999999999999999999-1", 100, http.StatusOK, 0, 0}, // again, invalid

		// These requests return the entire document:
		{"bytes=0-", 100, http.StatusOK, 0, 0},
		{"bytes=0-99", 100, http.StatusOK, 0, 0},
		{"bytes=-100", 100, http.StatusOK, 0, 0},
		{"bytes=-99999999999999999999", 100, http.StatusOK, 0, 0},

		// Not satisfiable:
		{"bytes=100-", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=100-200", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=100-99999999999999999999", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=-0", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},

		{"bytes=10-", 100, http.StatusPartialContent, 10, 99},
		{"bytes=-10", 100, http.StatusPartialContent, 90, 99},
		{"bytes=0-0", 100, http.StatusPartialContent, 0, 0},
		{"bytes=50-60", 100, http.StatusPartialContent, 50, 60},
		{"bytes=99-", 100, http.StatusPartialContent, 99, 99},
		{"bytes=99-200", 100, http.StatusPartialContent, 99, 99},
		{"bytes=90-200", 100, http.StatusPartialContent, 90, 99},
		{"bytes=90-99999999999999999999", 100, http.StatusPartialContent, 90, 99},
		{"bytes=2-98", 100, http.StatusPartialContent, 2, 98},

		// Test with empty content:
		{"bytes=-1", 0, http.StatusOK, 0, 0},
		{"bytes=-10", 0, http.StatusOK, 0, 0},
		{"bytes=0-0", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=0-49", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=1-1", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=-0", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
	}

	for _, expected := range testcases {
		status, start, end := parseHTTPRangeHeader(expected.header, expected.contentLength)
		t.Logf("*** Range: %s  --> %d %d-%d", expected.header, status, start, end)
		goassert.Equals(t, status, expected.status)
		if status == http.StatusPartialContent {
			goassert.Equals(t, start, expected.start)
			goassert.Equals(t, end, expected.end)
		}
	}
}
