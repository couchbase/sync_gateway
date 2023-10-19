// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func TestDefaultDbConfig(t *testing.T) {
	useXattrs := true
	sc := DefaultStartupConfig("")
	compactIntervalDays := *(DefaultDbConfig(&sc, useXattrs).CompactIntervalDays)
	require.Equal(t, db.DefaultCompactInterval, time.Duration(compactIntervalDays)*time.Hour*24)
}
