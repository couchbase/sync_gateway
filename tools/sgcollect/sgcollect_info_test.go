package main

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSGCollectTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	restTesterConfig := rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{}}}
	restTester := rest.NewRestTester(t, &restTesterConfig)
	require.NoError(t, restTester.SetAdminParty(false))
	defer restTester.Close()

	mockSyncGateway := httptest.NewServer(restTester.TestAdminHandler())
	defer mockSyncGateway.Close()
	mockSyncGatewayURL, _ := url.Parse(mockSyncGateway.URL)

	tasks := MakeAllTasks(mockSyncGatewayURL, &SGCollectOptions{}, ServerConfig{
		Databases: map[string]any{
			"db": restTester.DatabaseConfig.DbConfig,
		},
	})
	tr := NewTaskTester(t, SGCollectOptions{})
	for _, task := range tasks {
		output, res := tr.RunTask(task)
		tex := TaskEx(task)
		if tex.mayFailTest {
			if res.Error != nil {
				t.Logf("Failed to run %s [%s] - marked as may fail, so not failing test", task.Name(), task.Header())
				t.Logf("Error: %v", res.Error)
				t.Logf("Output: %s", output.String())
			}
		} else {
			if !AssertDidNotFail(t, res) {
				t.Logf("Output: %s", output.String())
			}
		}
	}
}

func TestCollectZipAndUpload(t *testing.T) {
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/sgwdev/12345/test.zip", r.URL.Path)
		assert.NotZero(t, r.ContentLength)

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		buf := bytes.NewReader(body)
		reader, err := zip.NewReader(buf, int64(len(body)))
		require.NoError(t, err)
		assert.Len(t, reader.File, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer uploadServer.Close()
	uploadURL, _ := url.Parse(uploadServer.URL)
	opts := &SGCollectOptions{
		UploadHost:         uploadURL,
		UploadCustomer:     "sgwdev",
		UploadTicketNumber: "12345",
	}
	tr, err := NewTaskRunner(opts)
	require.NoError(t, err)
	tr.Run(&RawStringTask{
		name: "test",
		val:  "test",
	})
	tr.Finalize()

	tmpDir := t.TempDir()
	tmpPath := filepath.Join(tmpDir, "test.zip")
	require.NoError(t, tr.ZipResults(tmpPath, "test", io.Copy))

	require.NoError(t, UploadFile(opts, tmpPath))
}
