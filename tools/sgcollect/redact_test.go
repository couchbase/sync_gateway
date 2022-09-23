package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactCopy(t *testing.T) {
	opts := &SGCollectOptions{
		LogRedactionSalt: "SALT",
	}
	redacted := func(in string) string {
		digest := sha1.Sum([]byte(string(opts.LogRedactionSalt) + in)) //nolint:gosec
		return hex.EncodeToString(digest[:])
	}
	cases := []struct {
		Name            string
		Input, Expected string
	}{
		{
			Name:     "no redaction",
			Input:    "foo bar",
			Expected: "foo bar",
		},
		{
			Name:     "simple",
			Input:    "foo <ud>bar</ud> baz",
			Expected: fmt.Sprintf("foo <ud>%s</ud> baz", redacted("bar")),
		},
		{
			Name:     "nested",
			Input:    "foo <ud>bar<ud>baz</ud>qux</ud> baz",
			Expected: fmt.Sprintf("foo <ud>%s</ud> baz", redacted("bar<ud>baz</ud>qux")),
		},
		{
			Name:     "multiple",
			Input:    "foo <ud>bar</ud> baz <ud>qux</ud>",
			Expected: fmt.Sprintf("foo <ud>%s</ud> baz <ud>%s</ud>", redacted("bar"), redacted("qux")),
		},
		{
			Name:     "only",
			Input:    "<ud>foo</ud>",
			Expected: fmt.Sprintf("<ud>%s</ud>", redacted("foo")),
		},
		{
			Name:     "at start",
			Input:    "<ud>foo</ud> bar",
			Expected: fmt.Sprintf("<ud>%s</ud> bar", redacted("foo")),
		},
		{
			Name:     "at end",
			Input:    "foo <ud>bar</ud>",
			Expected: fmt.Sprintf("foo <ud>%s</ud>", redacted("bar")),
		},
		{
			Name:     "corrupt",
			Input:    "foo <ud",
			Expected: "foo <ud",
		},
		{
			Name:     "mismatched",
			Input:    "foo <ud>bar",
			Expected: "foo <ud>bar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := RedactCopier(opts)(&buf, strings.NewReader(tc.Input))
			require.NoError(t, err)
			require.Equal(t, tc.Expected, buf.String())
			require.Equal(t, int64(buf.Len()), n)
		})
	}
}

func FuzzRedactCopy(f *testing.F) {
	opts := &SGCollectOptions{
		LogRedactionSalt: "SALT",
	}
	f.Add("foo bar")
	f.Add("foo <ud>bar</ud> baz")
	f.Fuzz(func(t *testing.T, in string) {
		var buf bytes.Buffer
		n, err := RedactCopier(opts)(&buf, strings.NewReader(in))
		require.NoError(t, err)
		require.Equal(t, int64(buf.Len()), n)
	})
}

// Verifies that RedactCopier doesn't change its input if it has nothing to do.
func FuzzRedactCopyIdempotent(f *testing.F) {
	opts := &SGCollectOptions{
		LogRedactionSalt: "SALT",
	}
	f.Add("foo bar")
	f.Fuzz(func(t *testing.T, in string) {
		if strings.Contains(in, "<ud>") && strings.Contains(in, "</ud>") {
			t.SkipNow()
		}
		var buf bytes.Buffer
		n, err := RedactCopier(opts)(&buf, strings.NewReader(in))
		require.NoError(t, err)
		require.Equal(t, int64(buf.Len()), n)
		require.Equal(t, buf.String(), in)
	})
}

func FuzzRedactCopyMiddle(f *testing.F) {
	opts := &SGCollectOptions{
		LogRedactionSalt: "SALT",
	}
	redacted := func(in string) string {
		digest := sha1.Sum([]byte(string(opts.LogRedactionSalt) + in)) //nolint:gosec
		return hex.EncodeToString(digest[:])
	}
	f.Add("foo", "bar", "baz")
	f.Fuzz(func(t *testing.T, s1, s2, s3 string) {
		var buf bytes.Buffer
		n, err := RedactCopier(opts)(&buf, strings.NewReader(fmt.Sprintf("%s<ud>%s</ud>%s", s1, s2, s3)))
		require.NoError(t, err)
		require.Equal(t, int64(buf.Len()), n)
		require.Equal(t, buf.String(), fmt.Sprintf("%s<ud>%s</ud>%s", s1, redacted(s2), s3))
	})
}
