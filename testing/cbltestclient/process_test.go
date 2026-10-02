// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestResolveBinary(t *testing.T) {
	root := t.TempDir()
	t.Setenv(EnvTestServerDir, root)
	_, err := ResolveBinary()
	require.ErrorIs(t, err, ErrNoTestServer)

	// The executable lives in a bin directory because the test server looks for its assets in
	// "<executable dir>/../assets".
	binDir := filepath.Join(root, CBLVersion, runtime.GOOS+"-"+runtime.GOARCH, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o700))
	installed := filepath.Join(binDir, "testserver")
	require.NoError(t, os.WriteFile(installed, nil, 0o700))

	path, err := ResolveBinary()
	require.NoError(t, err)
	assert.Equal(t, installed, path)
}
