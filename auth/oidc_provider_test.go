package auth

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetChecksum(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		Header:     make(http.Header),
	}
	// Check SHA1 checksum of HTTP response with valid body
	checksum, err := getChecksum(response)
	require.NoError(t, err, "Error getting SHA1 checksum of response")
	assert.Equal(t, "a5e744d0164540d33b1d7ea616c28f2fa97e754a", checksum)

	// Check SHA1 checksum of HTTP response with empty body
	response.Body = ioutil.NopCloser(bytes.NewBufferString(""))
	checksum, err = getChecksum(response)
	require.NoError(t, err, "Error getting SHA1 checksum of response")
	assert.Equal(t, "da39a3ee5e6b4b0d3255bfef95601890afd80709", checksum)
}

func TestExpiresPass(t *testing.T) {
	tests := []struct {
		name    string
		date    string
		exp     string
		wantTTL time.Duration
		wantOK  bool
	}{{
		name:    "Expires and Date properly set",
		date:    "Thu, 01 Dec 1983 22:00:00 GMT",
		exp:     "Fri, 02 Dec 1983 01:00:00 GMT",
		wantTTL: 10800 * time.Second,
		wantOK:  true,
	}, {
		name:   "empty headers",
		date:   "",
		exp:    "",
		wantOK: false,
	}, {
		name:   "lack of Expires short-circuit Date parsing",
		date:   "foo",
		exp:    "",
		wantOK: false,
	}, {
		name:   "lack of Date short-circuit Expires parsing",
		date:   "",
		exp:    "foo",
		wantOK: false,
	}, {
		name:    "no Date",
		exp:     "Thu, 01 Dec 1983 22:00:00 GMT",
		wantTTL: 0,
		wantOK:  false,
	}, {
		name:    "no Expires",
		date:    "Thu, 01 Dec 1983 22:00:00 GMT",
		wantTTL: 0,
		wantOK:  false,
	}, {
		name:    "Expires less than Date",
		date:    "Fri, 02 Dec 1983 01:00:00 GMT",
		exp:     "Thu, 01 Dec 1983 22:00:00 GMT",
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := expires(tc.date, tc.exp)
			require.NoError(t, err, "error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestExpiresFail(t *testing.T) {
	tests := []struct {
		name    string
		date    string
		exp     string
		wantTTL time.Duration
		wantOK  bool
	}{{
		name:    "malformed Date header",
		date:    "foo",
		exp:     "Mon, 01 Jun 2020 19:49:09 GMT",
		wantTTL: 0,
		wantOK:  false,
	}, {
		name:    "malformed exp header",
		date:    "Mon, 01 Jun 2020 19:49:09 GMT",
		exp:     "bar",
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := expires(tc.date, tc.exp)
			require.Error(t, err, "No error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheablePass(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "valid Cache-Control",
		headers: http.Header{
			"Cache-Control": []string{"max-age=100"},
		},
		wantTTL: 100 * time.Second,
		wantOK:  true,
	}, {
		name: "valid Date and Expires",
		headers: http.Header{
			"Date":    []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires": []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 10800 * time.Second,
		wantOK:  true,
	}, {
		name: "Cache-Control supersedes Date and Expires",
		headers: http.Header{
			"Cache-Control": []string{"max-age=100"},
			"Date":          []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires":       []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 100 * time.Second,
		wantOK:  true,
	}, {
		name:    "no caching headers",
		headers: http.Header{},
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := cacheable(tc.headers)
			require.NoError(t, err, "Error getting expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}

func TestCacheableFail(t *testing.T) {
	tests := []struct {
		name    string
		header  http.Header
		wantTTL time.Duration
		wantOK  bool
	}{{
		name: "invalid Cache-Control short-circuits",
		header: http.Header{
			"Cache-Control": []string{"max-age"},
			"Date":          []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires":       []string{"Fri, 02 Dec 1983 01:00:00 GMT"},
		},
		wantTTL: 0,
		wantOK:  false,
	}, {
		name: "no Cache-Control invalid Expires",
		header: http.Header{
			"Date":    []string{"Thu, 01 Dec 1983 22:00:00 GMT"},
			"Expires": []string{"boo"},
		},
		wantTTL: 0,
		wantOK:  false,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ttl, ok, err := cacheable(tc.header)
			require.Error(t, err, "No error checking max age and expiry")
			assert.Equal(t, tc.wantTTL, ttl, "TTL mismatch")
			assert.Equal(t, tc.wantOK, ok, " incorrect ok value")
		})
	}
}
