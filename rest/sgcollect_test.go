package rest

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func TestSgcollectFilename(t *testing.T) {
	filename := sgcollectFilename()

	// Check it doesn't have forbidden chars
	goassert.False(t, strings.ContainsAny(filename, "\\/:*?\"<>|"))

	pattern := `^sgcollectinfo\-\d{4}\-\d{2}\-\d{2}t\d{6}\-sga?@(\d{1,3}\.){4}zip$`
	matched, err := regexp.Match(pattern, []byte(filename))
	assert.NoError(t, err, "unexpected regexp error")
	assert.True(t, matched, fmt.Sprintf("Filename: %s did not match pattern: %s", filename, pattern))
}

func TestSgcollectOptionsValidate(t *testing.T) {

	binPath, err := os.Executable()
	assert.NoError(t, err, "unexpected error getting executable path")

	tests := []struct {
		options     *sgCollectOptions
		errContains string
	}{
		{
			options:     &sgCollectOptions{},
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: true, Customer: "alice"},
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: true, Customer: "alice", Ticket: "abc"},
			errContains: "ticket number must be",
		},
		{
			options:     &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: true},
			errContains: "customer must be set",
		},
		{
			options:     &sgCollectOptions{Upload: true, Ticket: "12345"},
			errContains: "customer must be set",
		},
		{
			options:     &sgCollectOptions{Upload: false, Customer: "alice"},
			errContains: "upload must be set to true",
		},
		{
			options:     &sgCollectOptions{Upload: false, Ticket: "12345"},
			errContains: "upload must be set to true",
		},
		{
			options:     &sgCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			errContains: "upload must be set to true",
		},
		{
			// Directory exists
			options:     &sgCollectOptions{OutputDirectory: "."},
			errContains: "",
		},
		{
			// Directory doesn't exist
			options:     &sgCollectOptions{OutputDirectory: "/path/to/output/dir"},
			errContains: "no such file or directory",
		},
		{
			// Path not a directory
			options:     &sgCollectOptions{OutputDirectory: binPath},
			errContains: "not a directory",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(ts *testing.T) {
			err := test.options.Validate()

			errStr := ""
			if err != nil {
				errStr = err.Error()
			}

			goassert.Equals(ts, err != nil, test.errContains != "")
			goassert.StringContains(ts, errStr, test.errContains)

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
			// Validation will fail, as no Customer is passed, default value not populated.
			options:      &sgCollectOptions{Upload: true},
			expectedArgs: []string{"--upload-host", ""},
		},
		{
			// Validation will fail, as no Customer is passed, default value not populated.
			options:      &sgCollectOptions{Upload: true, Ticket: "123456"},
			expectedArgs: []string{"--upload-host", "", "--ticket", "123456"},
		},
		{
			options:      &sgCollectOptions{Upload: true, RedactLevel: "partial"},
			expectedArgs: []string{"--upload-host", "", "--log-redaction-level", "partial"},
		},
		{
			options:      &sgCollectOptions{Upload: true, RedactLevel: "partial", RedactSalt: "asdf"},
			expectedArgs: []string{"--upload-host", "", "--log-redaction-level", "partial", "--log-redaction-salt", "asdf"},
		},
		{
			// Check that the default upload host is set
			options:      &sgCollectOptions{Upload: true, Customer: "alice"},
			expectedArgs: []string{"--upload-host", "https://s3.amazonaws.com/cb-customers", "--customer", "alice"},
		},
		{
			options:      &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			expectedArgs: []string{"--upload-host", "example.org/custom-s3-bucket-url", "--customer", "alice"},
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
			goassert.DeepEquals(ts, args, test.expectedArgs)
		})
	}
}
