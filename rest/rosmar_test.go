// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalrusServerValues(t *testing.T) {
	tempDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	serverConfig := &StartupConfig{API: APIConfig{CORS: &auth.CORSConfig{}, AdminInterface: DefaultAdminInterface}}
	ctx := base.TestCtx(t)
	serverContext := NewServerContext(ctx, serverConfig, false)
	defer func() {
		serverContext.Close(ctx)
		assert.NoError(t, os.RemoveAll(tempDir))
	}()

	onDiskRosmarPath := tempDir
	if runtime.GOOS == "windows" {
		// windows uris look like rosmar:///c:/foo/bar
		onDiskRosmarPath = "/" + strings.ReplaceAll(tempDir, "\\", "/")
	}
	testCases := []struct {
		name   string
		server string
	}{
		{
			name:   "walrusInMemory",
			server: "walrus:foo",
		},
		{
			name:   "rosmarInMemory",
			server: rosmar.InMemoryURL,
		},
		{
			name:   "rosmarOnDisk",
			server: "rosmar://" + onDiskRosmarPath,
		},
	}
	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			dbConfig := DbConfig{
				Name:         fmt.Sprintf("db%d", i),
				BucketConfig: BucketConfig{Server: &testCase.server},
			}

			dbContext, err := serverContext._getOrAddDatabaseFromConfig(ctx, DatabaseConfig{DbConfig: dbConfig}, getOrAddDatabaseConfigOptions{
				failFast:    false,
				useExisting: false,
			})
			require.NoError(t, err)
			require.NotNil(t, dbContext)
		})
	}
}
