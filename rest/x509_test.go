/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestX509UnknownAuthorityWrap(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires making a connection to couchbase server")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	clusterSpec := base.TestClusterSpec(t)
	clusterSpec.CACertpath = "" // remove certificate
	_, err := base.NewClusterAgent(ctx, clusterSpec)
	assert.ErrorContains(t, err, "Provide a CA cert, or set tls_skip_verify to true in config")
}
