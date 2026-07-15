/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"testing"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"
)

// TestIsRecoverableReadErrorCollectionOutdated verifies that a read which failed only because the
// collection was dropped/recreated underneath it is treated as unrecoverable. gocbcore marks
// KV_COLLECTION_OUTDATED as always-retry, so such an operation burns its whole deadline and
// surfaces as a TimeoutError carrying that retry reason — retrying it again cannot help.
func TestIsRecoverableReadErrorCollectionOutdated(t *testing.T) {
	c := &Collection{}

	// A generic timeout (no collection-outdated cause) is transient and should be retried.
	require.True(t, c.isRecoverableReadError(&gocb.TimeoutError{InnerError: gocb.ErrTimeout}))

	// A timeout caused solely by an outdated/dropped collection must NOT be retried.
	require.False(t, c.isRecoverableReadError(&gocb.TimeoutError{
		InnerError:   gocb.ErrTimeout,
		RetryReasons: []gocb.RetryReason{gocb.KVCollectionOutdatedRetryReason},
	}))

	// An explicit collection-not-found is likewise unrecoverable.
	require.False(t, c.isRecoverableReadError(gocb.ErrCollectionNotFound))
}
