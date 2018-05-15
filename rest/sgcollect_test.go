package rest

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestSgcollectFilename(t *testing.T) {
	filename := sgcollectFilename()

	// Check it doesn't have forbidden chars
	assert.False(t, strings.ContainsAny(filename, "\\/:*?\"<>|"))

	pattern := `^sgcollectinfo\-\d{4}\-\d{2}\-\d{2}t\d{6}\-sga?@(\d{1,3}\.){4}zip$`
	matched, err := regexp.Match(pattern, []byte(filename))
	assertNoError(t, err, "unexpected regexp error")
	assertTrue(t, matched, fmt.Sprintf("Filename: %s did not match pattern: %s", filename, pattern))
}

func TestSgcollectOptionsValidate(t *testing.T) {

	binPath, err := os.Executable()
	assertNoError(t, err, "unexpected error getting executable path")

	tests := []struct {
		options     *sgCollectOptions
		hasError    bool
		errContains string
	}{
		{
			options:     &sgCollectOptions{},
			hasError:    false,
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: true},
			hasError:    true,
			errContains: "customer must be set",
		},
		{
			options:     &sgCollectOptions{Upload: true, Customer: "alice"},
			hasError:    false,
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			hasError:    false,
			errContains: "",
		},
		{
			options:     &sgCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			hasError:    true,
			errContains: "upload must be set to true",
		},
		{
			// Directory exists
			options:     &sgCollectOptions{OutputDirectory: "."},
			hasError:    false,
			errContains: "",
		},
		{
			// Directory doesn't exist
			options:     &sgCollectOptions{OutputDirectory: "/path/to/output/dir"},
			hasError:    true,
			errContains: "no such file or directory",
		},
		{
			// Path not a directory
			options:     &sgCollectOptions{OutputDirectory: binPath},
			hasError:    true,
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

			assert.Equals(ts, err != nil, test.hasError)
			assert.StringContains(ts, errStr, test.errContains)

		})
	}

}

func TestSgcollectOptionsArgs(t *testing.T) {
	binPath, err := os.Executable()
	assertNoError(t, err, "unexpected error getting executable path")

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
			assert.DeepEquals(ts, args, test.expectedArgs)
		})
	}
}
