package rest

import (
	"net/http"
	"net/url"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestGetRestrictedIntQuery(t *testing.T) {

	defaultValue := uint64(42)
	minValue := uint64(20)
	maxValue := uint64(100)

	// make sure it returns default value when passed empty Values
	values := make(url.Values)
	restricted, _ := getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, defaultValue)

	// make sure it returns default value when passed Values that doesn't contain key
	values.Set("bar", "99")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, defaultValue)

	// make sure it returns appropriate value from Values
	values.Set("foo", "99")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, uint64(99))

	// make sure it is limited to max when value value is over max
	values.Set("foo", "200")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, maxValue)

	// make sure it is limited to min when value value is under min
	values.Set("foo", "1")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, minValue)

	// Return zero when allowZero=true
	values.Set("foo", "0")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		true,
	)
	goassert.Equals(t, restricted, uint64(0))

	// Return minValue when allowZero=false
	values.Set("foo", "0")
	restricted, _ = getRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	goassert.Equals(t, restricted, minValue)
}

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
