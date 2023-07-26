// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterStats(t *testing.T) {
	globalStats, dbStats, err := registerStats()

	assert.NotNil(t, globalStats)
	assert.NotNil(t, dbStats)
	assert.NoError(t, err)
}

func TestFileOutput(t *testing.T) {
	defer func() {
		// Clean up potential left over files at the end, and ignore the error
		_ = removeFile(DefaultFilePath + DefaultFileName + TarFileExtension + GzipFileExtension)
		_ = removeFile(DefaultFilePath + JsonFileName)
		_ = removeFile(DefaultFilePath + DefaultFileName + TarFileExtension)

	}()

	// Set all logging to buffer
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	err := getStats(logger, false)
	assert.NoError(t, err)

	// Make sure no errors where logged
	assert.Empty(t, buf)

	// Test file outputted
	testFile := DefaultFilePath + DefaultFileName + TarFileExtension + GzipFileExtension
	assert.FileExists(t, testFile)

	// Test cleanup was successful
	assert.NoFileExists(t, DefaultFilePath+JsonFileName)
	assert.NoFileExists(t, DefaultFilePath+DefaultFileName+TarFileExtension)
}

func TestStdOutput(t *testing.T) {
	// Set all logging to buffer
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	err := getStats(logger, true)
	assert.NoError(t, err)

	// Make sure no errors where logged
	assert.Empty(t, buf)
}
