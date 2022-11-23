/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package channels

import (
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
	"github.com/stretchr/testify/assert"
)

func TestRequireUser(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireUser(oldDoc._names) }`
	withSyncRunner(t, funcSource, 0, func(runner *syncRunner) {
		var result interface{}
		result, _ = runner.call(parse(`{}`), `{"_names": "alpha"}`, emptyMetaMap(), parse(`{"name": "alpha"}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_names": ["beta", "gamma"]}`, emptyMetaMap(), parse(`{"name": "beta"}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_names": ["delta"]}`, emptyMetaMap(), parse(`{"name": "beta"}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorWrongUser))
	})
}

func TestRequireRole(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireRole(oldDoc._roles) }`
	withSyncRunner(t, funcSource, 0, func(runner *syncRunner) {
		var result interface{}
		result, _ = runner.call(parse(`{}`), `{"_roles": ["alpha"]}`, emptyMetaMap(), parse(`{"name": "", "roles": {"alpha":""}}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_roles": ["beta", "gamma"]}`, emptyMetaMap(), parse(`{"name": "", "roles": {"beta": ""}}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_roles": ["delta"]}`, emptyMetaMap(), parse(`{"name": "", "roles": {"beta":""}}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingRole))
	})
}

func TestRequireAccess(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAccess(oldDoc._access) }`
	withSyncRunner(t, funcSource, 0, func(runner *syncRunner) {
		var result interface{}
		result, _ = runner.call(parse(`{}`), `{"_access": ["alpha"]}`, emptyMetaMap(), parse(`{"name": "", "channels": ["alpha"]}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_access": ["beta", "gamma"]}`, emptyMetaMap(), parse(`{"name": "", "channels": ["beta"]}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{"_access": ["delta"]}`, emptyMetaMap(), parse(`{"name": "", "channels": ["beta"]}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingChannelAccess))
	})
}

func TestRequireAdmin(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAdmin() }`
	withSyncRunner(t, funcSource, 0, func(runner *syncRunner) {
		var result interface{}
		result, _ = runner.call(parse(`{}`), `{}`, emptyMetaMap(), parse(`{}`))
		assertNotRejected(t, result)
		result, _ = runner.call(parse(`{}`), `{}`, emptyMetaMap(), parse(`{"name": ""}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
		result, _ = runner.call(parse(`{}`), `{}`, emptyMetaMap(), parse(`{"name": "GUEST"}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
		result, _ = runner.call(parse(`{}`), `{}`, emptyMetaMap(), parse(`{"name": "beta"}`))
		assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
	})
}

// Helpers

func withSyncRunner(t *testing.T, syncFn string, timeout time.Duration, fn func(*syncRunner)) {
	var vms js.VMPool
	vms.Init(1)
	defer vms.Close()

	mapper := NewChannelMapper(&vms, syncFn, timeout)
	_, err := mapper.withSyncRunner(func(runner *syncRunner) (any, error) {
		fn(runner)
		return nil, nil
	})
	assert.NoError(t, err)
}

func assertRejected(t *testing.T, result interface{}, err *base.HTTPError) {
	r, ok := result.(*ChannelMapperOutput)
	assert.True(t, ok)
	assert.Equal(t, r.Rejection, err)
}

func assertNotRejected(t *testing.T, result interface{}) {
	r, ok := result.(*ChannelMapperOutput)
	assert.True(t, ok)
	assert.NoError(t, r.Rejection)
}
