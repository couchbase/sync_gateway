// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

// AssertLogContains can hit the race detector due to swapping the global loggers

func TestBadCORSValuesConfig(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()

	// expect database to be created with bad CORS values, but do log a warning
	dbConfig := rt.NewDbConfig()
	dbConfig.CORS = &auth.CORSConfig{
		Origin: []string{"http://example.com", "1http://example.com"},
	}
	base.AssertLogContains(t, "cors.origin contains values", func() {
		rt.CreateDatabase("db", dbConfig)
	})
}
