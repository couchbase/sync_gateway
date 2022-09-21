package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileTask(t *testing.T) {
	tmpdir := t.TempDir()
	const testFileName = "testFile"
	testFilePath := filepath.Join(tmpdir, testFileName)
	require.NoError(t, ioutil.WriteFile(testFilePath, []byte("test data"), 0600))

	tt := NewTaskTester(t, SGCollectOptions{})
	task := &FileTask{
		name:      "Test",
		inputFile: testFilePath,
	}
	buf, res := tt.RunTask(task)
	AssertRan(t, res)
	assert.Equal(t, buf.String(), "test data")
}

func TestURLTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"foo": "bar"}`))
	}))
	defer server.Close()

	tt := NewTaskTester(t, SGCollectOptions{})
	task := &URLTask{
		name: "Test",
		url:  server.URL + "/",
	}
	buf, res := tt.RunTask(task)
	AssertRan(t, res)
	assert.Equal(t, buf.String(), `{"foo": "bar"}`)
}

func TestURLTaskJSONPrettified(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"foo": "bar"}`))
	}))
	defer server.Close()

	tt := NewTaskTester(t, SGCollectOptions{})
	task := &URLTask{
		name: "Test",
		url:  server.URL + "/",
	}
	buf, res := tt.RunTask(task)
	AssertRan(t, res)
	assert.Equal(t, buf.String(), "{\n\t\"foo\": \"bar\"\n}")
}
