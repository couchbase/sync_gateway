/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sgcollecttest

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSgcollectFilename(t *testing.T) {
	filename := rest.SGCollectFilename()

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
		options *rest.SGCollectOptions
	}{
		{
			name:    "defaults",
			options: &rest.SGCollectOptions{},
		},
		{
			name:    "upload with customer name",
			options: &rest.SGCollectOptions{Upload: true, Customer: "alice"},
		},
		{
			name:    "custom upload with customer name",
			options: &rest.SGCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
		},
		{
			name:    "directory that exists",
			options: &rest.SGCollectOptions{OutputDirectory: "."},
		},
		{
			name:    "valid redact level",
			options: &rest.SGCollectOptions{RedactLevel: "partial"},
		},
		{
			name:    "valid keep_zip option",
			options: &rest.SGCollectOptions{KeepZip: true},
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
		options     *rest.SGCollectOptions
		errContains string
	}{
		{
			name:        "directory doesn't exist",
			options:     &rest.SGCollectOptions{OutputDirectory: "/path/to/output/dir"},
			errContains: "no such file or directory",
		},
		{
			name:        "path not a directory",
			options:     &rest.SGCollectOptions{OutputDirectory: binaryPath},
			errContains: "not a directory",
		},
		{
			name:        "invalid redact level",
			options:     &rest.SGCollectOptions{RedactLevel: "asdf"},
			errContains: "'redact_level' must be",
		},
		{
			name:        "no customer",
			options:     &rest.SGCollectOptions{Upload: true},
			errContains: "'customer' must be set",
		},
		{
			name:        "no customer with ticket",
			options:     &rest.SGCollectOptions{Upload: true, Ticket: "12345"},
			errContains: "'customer' must be set",
		},
		{
			name:        "customer no upload",
			options:     &rest.SGCollectOptions{Upload: false, Customer: "alice"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "ticket no upload",
			options:     &rest.SGCollectOptions{Upload: false, Ticket: "12345"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "customer upload host no upload",
			options:     &rest.SGCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			errContains: "'upload' must be set to true",
		},
		{
			name:        "non-digit ticket number",
			options:     &rest.SGCollectOptions{Upload: true, Customer: "alice", Ticket: "abc"},
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
		options      *rest.SGCollectOptions
		expectedArgs []string
	}{
		{
			options:      &rest.SGCollectOptions{},
			expectedArgs: []string{},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true},
			expectedArgs: []string{"--upload-host", rest.DefaultSGCollectUploadHost},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true, Ticket: "123456", KeepZip: true},
			expectedArgs: []string{"--upload-host", rest.DefaultSGCollectUploadHost, "--ticket", "123456", "--keep-zip"},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true, RedactLevel: "partial"},
			expectedArgs: []string{"--upload-host", rest.DefaultSGCollectUploadHost, "--log-redaction-level", "partial"},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true, RedactLevel: "partial", RedactSalt: "asdf"},
			expectedArgs: []string{"--upload-host", rest.DefaultSGCollectUploadHost, "--log-redaction-level", "partial", "--log-redaction-salt", "asdf"},
		},
		{
			// Check that the default upload host is set
			options:      &rest.SGCollectOptions{Upload: true, Customer: "alice"},
			expectedArgs: []string{"--upload-host", rest.DefaultSGCollectUploadHost, "--customer", "alice"},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url"},
			expectedArgs: []string{"--upload-host", "example.org/custom-s3-bucket-url", "--customer", "alice"},
		},
		{
			options:      &rest.SGCollectOptions{Upload: true, Customer: "alice", UploadHost: "https://example.org/custom-s3-bucket-url", UploadProxy: "http://proxy.example.org:8080"},
			expectedArgs: []string{"--upload-host", "https://example.org/custom-s3-bucket-url", "--upload-proxy", "http://proxy.example.org:8080", "--customer", "alice"},
		},
		{
			// Upload false, so don't pass upload host through. same for keep zip
			options:      &rest.SGCollectOptions{Upload: false, Customer: "alice", UploadHost: "example.org/custom-s3-bucket-url", KeepZip: false},
			expectedArgs: []string{"--customer", "alice"},
		},
		{
			// Directory exists
			options:      &rest.SGCollectOptions{OutputDirectory: "."},
			expectedArgs: []string{},
		},
		{
			// Directory doesn't exist
			options:      &rest.SGCollectOptions{OutputDirectory: "/path/to/output/dir"},
			expectedArgs: []string{},
		},
		{
			// Path not a directory
			options:      &rest.SGCollectOptions{OutputDirectory: binPath},
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

func TestSGCollectIntegration(t *testing.T) {
	base.TestRunSGCollectIntegrationTests(t)
	base.LongRunningTest(t) // this test is very long, and somewhat fragile, since it involves relying on the sgcollect_info tool to run successfully, which requires system python
	cwd, err := os.Getwd()
	require.NoError(t, err)
	config := rest.BootstrapStartupConfigForTest(t)
	sc, closeFn := rest.StartServerWithConfig(t, &config)
	defer closeFn()

	outputs := map[string]*strings.Builder{
		"stdout": &strings.Builder{},
		"stderr": &strings.Builder{},
	}

	sc.SGCollect.Stdout = outputs["stdout"]
	sc.SGCollect.Stderr = outputs["stderr"]
	python := "python3"
	if runtime.GOOS == "windows" {
		python = "python"
	}
	sc.SGCollect.SGCollectPath = []string{python, filepath.Join(cwd, "../../tools/sgcollect_info")}
	sc.SGCollect.SGCollectPathErr = nil
	validAuth := map[string]string{
		"Authorization": rest.GetBasicAuthHeader(t, base.TestClusterUsername(), base.TestClusterPassword()),
	}
	options := rest.SGCollectOptions{
		OutputDirectory: t.TempDir(),
	}
	resp := rest.BootstrapAdminRequestWithHeaders(t, sc, http.MethodPost, "/_sgcollect_info", string(base.MustJSONMarshal(t, options)), validAuth)
	resp.RequireStatus(http.StatusOK)

	var statusResponse struct {
		Status string
	}

	defer func() {
		if statusResponse.Status == "stopped" {
			return
		}
		resp := rest.BootstrapAdminRequestWithHeaders(t, sc, http.MethodDelete, "/_sgcollect_info", "", validAuth)
		resp.AssertStatus(http.StatusOK)
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rest.BootstrapAdminRequestWithHeaders(t, sc, http.MethodGet, "/_sgcollect_info", "", validAuth)
		resp.AssertStatus(http.StatusOK)
		resp.Unmarshal(&statusResponse)
		assert.Equal(c, "stopped", statusResponse.Status)
	}, 7*time.Minute, 2*time.Second, "sgcollect_info did not stop running in time")

	for name, stream := range outputs {
		output := stream.String()
		assert.NotContains(t, output, "Exception", "found in %s", name)
		assert.NotContains(t, output, "WARNING", "found in %s", name)
		assert.NotContains(t, output, "Error", "found in %s", name)
		assert.NotContains(t, output, "Errno", "found in %s", name)
		assert.NotContains(t, output, "Fail", "found in %s", name)
	}
}
