//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func pattern(t *testing.T, jsn string) *ClientCertNamePattern {
	var pat *ClientCertNamePattern
	err := json.Unmarshal([]byte(jsn), &pat)
	if err != nil {
		t.Errorf("INVALID JSON: %v", err)
	}
	assert.True(t, err == nil)
	assert.NotNil(t, pat)
	return pat
}

func TestValidate(t *testing.T) {
	assert.NoError(t, pattern(t, `{"path":"san.dns", "prefix":"www.", "delimiters":". "}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"san.dns", "prefix":"www."}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"san.dns", "delimiters":". "}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"subject.cn", "regex":"^([-\\w]+)@couchbase\\.com$"}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"san.email", "regex":"^([-\\w]+)@couchbase\\.com$"}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"san.uri", "delimiters":". "}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"san.email", "delimiters":". "}`).Validate())
	assert.NoError(t, pattern(t, `{"path":"subject.cn"}`).Validate())

	assert.Error(t, pattern(t, `{"path":"mxptlk", "prefix":"www."}`).Validate())
	assert.Error(t, pattern(t, `{"path":"subject.cn", "regex":"^([-\\w"}`).Validate())
	assert.Error(t, pattern(t, `{"path":"subject.cn", "prefix":"www.", "regex":"^([-\\w]+)@couchbase\\.com$"}`).Validate())
	assert.Error(t, pattern(t, `{"path":"subject.cn", "delimiters":". ", "regex":"^([-\\w]+)@couchbase\\.com$"}`).Validate())
	assert.Error(t, pattern(t, `{"path":"subject.cn", "prefix":"www.", "delimiters":". ", "regex":"^([-\\w]+)@couchbase\\.com$"}`).Validate())
}

func TestPrefixMatch(t *testing.T) {
	pat := pattern(t, `{"path":"san.dns", "prefix":"www."}`)
	assert.Equal(t, *pat.match("www.foo.couchbase.com"), "foo.couchbase.com")
	assert.Equal(t, *pat.match("www.foo"), "foo")
	assert.Equal(t, *pat.match("www.foo "), "foo ")

	assert.Nil(t, pat.match("mmm.foo.couchbase.com"))
	assert.Nil(t, pat.match("www."))
	assert.Nil(t, pat.match(""))
}

func TestPrefixDelimiterMatch(t *testing.T) {
	pat := pattern(t, `{"path":"san.dns", "prefix":"www.", "delimiters":". "}`)
	assert.Equal(t, *pat.match("www.foo.couchbase.com"), "foo")
	assert.Equal(t, *pat.match("www.foo couchbase.com"), "foo")
	assert.Equal(t, *pat.match("www.foo."), "foo")
	assert.Equal(t, *pat.match("www.foo "), "foo")
	assert.Equal(t, *pat.match("www.foo"), "foo")

	assert.Nil(t, pat.match("mmm.foo.couchbase.com"))
	assert.Nil(t, pat.match("foo.couchbase.com"))
	assert.Nil(t, pat.match("www."))
	assert.Nil(t, pat.match("www.."))
	assert.Nil(t, pat.match("www "))
	assert.Nil(t, pat.match("www ."))
	assert.Nil(t, pat.match(""))
}

func TestDelimiterMatch(t *testing.T) {
	pat := pattern(t, `{"path":"san.dns", "delimiters":". "}`)
	assert.Equal(t, *pat.match("foo.couchbase.com"), "foo")
	assert.Equal(t, *pat.match("foo couchbase.com"), "foo")
	assert.Equal(t, *pat.match("foo."), "foo")
	assert.Equal(t, *pat.match("foo "), "foo")
	assert.Equal(t, *pat.match("foo"), "foo")

	assert.Nil(t, pat.match(".couchbase.com"))
	assert.Nil(t, pat.match(" foo.couchbase.com"))
	assert.Nil(t, pat.match(""))
}

func TestRegexMatch(t *testing.T) {
	// Note that this regex must match the entire string (since it uses `^...$`)
	pat := pattern(t, `{"path":"san.email", "regex":"^([-\\w]+)@couchbase\\.com$"}`)
	assert.Equal(t, *pat.match("foo@couchbase.com"), "foo")
	assert.Equal(t, *pat.match("foo-bar@couchbase.com"), "foo-bar")
	assert.Equal(t, *pat.match("f@couchbase.com"), "f")
	assert.Equal(t, *pat.match("aLengthierNameThanFoo@couchbase.com"), "aLengthierNameThanFoo")

	assert.Nil(t, pat.match("@couchbase.com"))
	assert.Nil(t, pat.match("føø@couchbase.com"))
	assert.Nil(t, pat.match("foo@@couchbase.com"))
	assert.Nil(t, pat.match("foo@couchbase.com "))
	assert.Nil(t, pat.match(" foo@couchbase.com"))
	assert.Nil(t, pat.match(" foo@couchbase.com "))

	// Now the regex is not anchored to the ends of the string:
	pat = pattern(t, `{"path":"san.email", "regex":"([-\\w]+)@couchbase\\.com"}`)
	assert.Equal(t, *pat.match("foo@couchbase.com"), "foo")
	assert.Equal(t, *pat.match("foo-bar@couchbase.com"), "foo-bar")
	assert.Equal(t, *pat.match("f@couchbase.com"), "f")
	assert.Equal(t, *pat.match("aLengthierNameThanFoo@couchbase.com"), "aLengthierNameThanFoo")

	assert.Equal(t, *pat.match("foo@couchbase.com "), "foo")
	assert.Equal(t, *pat.match(" foo@couchbase.com"), "foo")
	assert.Equal(t, *pat.match(" foo@couchbase.com "), "foo")
	assert.Equal(t, *pat.match("Nathan Foo <foo@couchbase.com>"), "foo")
	assert.Equal(t, *pat.match("snäfoo@couchbase.community.xxx"), "foo")

	assert.Nil(t, pat.match("x @couchbase.com"))
	assert.Nil(t, pat.match("føø@couchbase.com"))
	assert.Nil(t, pat.match("foo@@couchbase.com"))
}

func TestCertPatternMatch(t *testing.T) {
	// This is not a real certificate, but it has all the fields that the code we're testing uses
	url1, _ := url.Parse("https://baz.couchbase.com/wow.html")
	url2, _ := url.Parse("wss://www.zab.couchbase.com/wow.html")
	cert := x509.Certificate{
		Subject:        pkix.Name{CommonName: "Martha McGill"},
		EmailAddresses: []string{"foo@couchbase.com"},
		DNSNames:       []string{"bar.couchbase.com", "www.example.com"},
		URIs:           []*url.URL{url1, url2},
	}

	pat := pattern(t, `{"path":"san.dns", "prefix":"www.", "delimiters":". "}`)
	assert.Equal(t, *pat.Match(cert), "example")
	pat = pattern(t, `{"path":"san.uri", "prefix":"https://", "delimiters":"."}`)
	assert.Equal(t, *pat.Match(cert), "baz")
	pat = pattern(t, `{"path":"san.uri", "regex":"^wss?://(?:www\\.)?(\\w+).couchbase\\.com"}`)
	assert.Equal(t, *pat.Match(cert), "zab")
	pat = pattern(t, `{"path":"subject.cn", "delimiters":" "}`)
	assert.Equal(t, *pat.Match(cert), "Martha")
	pat = pattern(t, `{"path":"subject.cn"}`)
	assert.Equal(t, *pat.Match(cert), "Martha McGill")
	pat = pattern(t, `{"path":"san.email", "regex":"([-\\w]+)@couchbase\\.com"}`)
	assert.Equal(t, *pat.Match(cert), "foo")

	pat = pattern(t, `{"path":"san.uri", "prefix":"www.", "delimiters":". "}`)
	assert.Nil(t, pat.Match(cert))
}

func TestCertPatternMatchNilAttributes(t *testing.T) {
	// Make sure nil fields in the cert don't cause panics
	cert := x509.Certificate{}

	pat := pattern(t, `{"path":"san.dns", "prefix":"www.", "delimiters":". "}`)
	assert.Nil(t, pat.Match(cert))
	pat = pattern(t, `{"path":"san.uri", "prefix":"https://", "delimiters":"."}`)
	assert.Nil(t, pat.Match(cert))
	pat = pattern(t, `{"path":"subject.cn", "delimiters":" "}`)
	assert.Nil(t, pat.Match(cert))
	pat = pattern(t, `{"path":"san.email", "regex":"([-\\w]+)@couchbase\\.com"}`)
	assert.Nil(t, pat.Match(cert))
}
