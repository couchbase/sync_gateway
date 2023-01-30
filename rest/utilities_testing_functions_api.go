// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func GetBasicDbCfg(tb *base.TestBucket) DbConfig {
	return DbConfig{
		BucketConfig: BucketConfig{
			Bucket: base.StringPtr(tb.GetName()),
		},
		NumIndexReplicas: base.UintPtr(0),
		UseViews:         base.BoolPtr(base.TestsDisableGSI()),
		EnableXattrs:     base.BoolPtr(base.TestUseXattrs()),
	}
}

// Creates a new RestTester using persistent config, and a database "db".
// Only the user-query-related fields are copied from `queryConfig`; the rest are ignored.
func NewRestTesterForUserQueries(t *testing.T, queryConfig DbConfig) *RestTester {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires persistent configs")
		return nil
	}

	rt := NewRestTester(t, &RestTesterConfig{
		groupID:           base.StringPtr(t.Name()), // Avoids race conditions between tests
		EnableUserQueries: true,
		PersistentConfig:  true,
	})

	_ = rt.Bucket() // initializes the bucket as a side effect
	dbConfig := GetBasicDbCfg(rt.TestBucket)

	dbConfig.UserFunctions = queryConfig.UserFunctions
	dbConfig.GraphQL = queryConfig.GraphQL

	resp, err := rt.CreateDatabase("db", dbConfig)
	if !assert.NoError(t, err) || !AssertStatus(t, resp, 201) {
		rt.Close()
		t.FailNow()
		return nil // (never reached)
	}
	return rt
}
