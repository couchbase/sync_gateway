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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
