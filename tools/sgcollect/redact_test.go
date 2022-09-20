package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactCopy(t *testing.T) {
	opts := &SGCollectOptions{
		LogRedactionSalt: "foo",
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
			Expected: "foo bar baz",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := Copier(opts)(&buf, strings.NewReader(tc.Input))
			require.NoError(t, err)
			require.Equal(t, tc.Expected, buf.String())
		})
	}
}
