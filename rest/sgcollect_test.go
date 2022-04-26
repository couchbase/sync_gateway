/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSgcollectFilename(t *testing.T) {
	filename := sgcollectFilename()

	// Check it doesn't have forbidden chars
	assert.False(t, strings.ContainsAny(filename, "\\/:*?\"<>|"))

	pattern := `^sgcollectinfo\-\d{4}\-\d{2}\-\d{2}t\d{6}\-sga?@(\d{1,3}\.){4}zip$`
	matched, err := regexp.Match(pattern, []byte(filename))
	assert.NoError(t, err, "unexpected regexp error")
	assert.True(t, matched, fmt.Sprintf("Filename: %s did not match pattern: %s", filename, pattern))
}

func TestSgcollectOptionsValidateValid(t *testing.T) {
	tests := []struct {
		name    string
		options *sgCollectOptions
	}{
		{
			name:    "defaults",
			options: &sgCollectOptions{},
		},
		{
			name:    "upload with customer name",
			options: &sgCollectOptions{Upload: true, Customer: "alice"},
		},
		{
			name:    "custom upload with customer name",
			options: &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
		},
		{
			name:    "directory that exists",
			options: &sgCollectOptions{OutputDirectory: "."},
		},
		{
			name:    "valid redact level",
			options: &sgCollectOptions{RedactLevel: "partial"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Nil(t, test.options.Validate())
		})
	}
}

func TestSgcollectOptionsValidateInvalid(t *testing.T) {
	binaryPath, err := os.Executable()
	assert.NoError(t, err, "unexpected error getting executable path")

	tests := []struct {
		name        string
		options     *sgCollectOptions
		errContains string
	}{
		{
			name:        "directory doesn't exist",
			options:     &sgCollectOptions{OutputDirectory: "/path/to/output/dir"},
			errContains: "no such file or directory",
		},
		{
			name:        "path not a directory",
			options:     &sgCollectOptions{OutputDirectory: binaryPath},
			errContains: "not a directory",
		},
		{
			name:        "invalid redact level",
			options:     &sgCollectOptions{RedactLevel: "asdf"},
			errContains: "'redact_level' must be",
		},
		{
			name:        "no customer",
			options:     &sgCollectOptions{Upload: true},
			errContains: "'customer' must be set",
		},
		{
			name:        "no customer with ticket",
			options:     &sgCollectOptions{Upload: true, Ticket: "12345"},
			errContains: "'customer' must be set",
		},
		{
			name:        "customer no upload",
			options:     &sgCollectOptions{Upload: false, Customer: "alice"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "ticket no upload",
			options:     &sgCollectOptions{Upload: false, Ticket: "12345"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "customer upload host no upload",
			options:     &sgCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "non-digit ticket number",
			options:     &sgCollectOptions{Upload: true, Customer: "alice", Ticket: "abc"},
			errContains: "'ticket' must be",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			errs := test.options.Validate()
			require.NotNil(t, errs)
			multiError, ok := errs.(*base.MultiError)
			require.True(t, ok)

			// make sure we get at least one error for the given invalid options.
			require.True(t, multiError.Len() > 0)

			// check each error matches the expected string.
			for _, err := range multiError.Errors {
				assert.Contains(ts, err.Error(), test.errContains)
			}
		})
	}

}

func TestSgcollectOptionsArgs(t *testing.T) {
	binPath, err := os.Executable()
	assert.NoError(t, err, "unexpected error getting executable path")

	tests := []struct {
		options      *sgCollectOptions
		expectedArgs []string
	}{
		{
			options:      &sgCollectOptions{},
			expectedArgs: []string{},
		},
		{
			options:      &sgCollectOptions{Upload: true},
			expectedArgs: []string{"--upload-host", defaultSGUploadHost},
		},
		{
			options:      &sgCollectOptions{Upload: true, Ticket: "123456"},
			expectedArgs: []string{"--upload-host", defaultSGUploadHost, "--ticket", "123456"},
		},
		{
			options:      &sgCollectOptions{Upload: true, RedactLevel: "partial"},
			expectedArgs: []string{"--upload-host", defaultSGUploadHost, "--log-redaction-level", "partial"},
		},
		{
			options:      &sgCollectOptions{Upload: true, RedactLevel: "partial", RedactSalt: "asdf"},
			expectedArgs: []string{"--upload-host", defaultSGUploadHost, "--log-redaction-level", "partial", "--log-redaction-salt", "asdf"},
		},
		{
			// Check that the default upload host is set
			options:      &sgCollectOptions{Upload: true, Customer: "alice"},
			expectedArgs: []string{"--upload-host", defaultSGUploadHost, "--customer", "alice"},
		},
		{
			options:      &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			expectedArgs: []string{"--upload-host", "example.org/custom-s3-bucket-url", "--customer", "alice"},
		},
		{
			options:      &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "https://example.org/custom-s3-bucket-url", UploadProxy: "http://proxy.example.org:8080"},
			expectedArgs: []string{"--upload-host", "https://example.org/custom-s3-bucket-url", "--upload-proxy", "http://proxy.example.org:8080", "--customer", "alice"},
		},
		{
			// Upload false, so don't pass upload host through
			options:      &sgCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			expectedArgs: []string{"--customer", "alice"},
		},
		{
			// Directory exists
			options:      &sgCollectOptions{OutputDirectory: "."},
			expectedArgs: []string{},
		},
		{
			// Directory doesn't exist
			options:      &sgCollectOptions{OutputDirectory: "/path/to/output/dir"},
			expectedArgs: []string{},
		},
		{
			// Path not a directory
			options:      &sgCollectOptions{OutputDirectory: binPath},
			expectedArgs: []string{},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(ts *testing.T) {
			// We'll run validate to populate some default fields,
			// but ignore errors raised by it for this test.
			_ = test.options.Validate()

			args := test.options.Args()
			assert.Equal(ts, test.expectedArgs, args)
		})
	}
}
