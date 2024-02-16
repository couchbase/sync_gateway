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

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequireUser(t *testing.T) {
	ctx := base.TestCtx(t)
	const funcSource = `function(doc, oldDoc) { requireUser(oldDoc._names) }`
	runner, err := NewSyncRunner(ctx, funcSource, 0)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_names": "alpha"}`), emptyMetaMap(), parse(t, `{"name": "alpha"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_names": ["beta", "gamma"]}`), emptyMetaMap(), parse(t, `{"name": "beta"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_names": ["delta"]}`), emptyMetaMap(), parse(t, `{"name": "beta"}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorWrongUser))
}

func TestRequireRole(t *testing.T) {
	ctx := base.TestCtx(t)
	const funcSource = `function(doc, oldDoc) { requireRole(oldDoc._roles) }`
	runner, err := NewSyncRunner(ctx, funcSource, 0)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_roles": ["alpha"]}`), emptyMetaMap(), parse(t, `{"name": "", "roles": {"alpha":""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_roles": ["beta", "gamma"]}`), emptyMetaMap(), parse(t, `{"name": "", "roles": {"beta": ""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_roles": ["delta"]}`), emptyMetaMap(), parse(t, `{"name": "", "roles": {"beta":""}}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingRole))
}

func TestRequireAccess(t *testing.T) {
	ctx := base.TestCtx(t)
	const funcSource = `function(doc, oldDoc) { requireAccess(oldDoc._access) }`
	runner, err := NewSyncRunner(ctx, funcSource, 0)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_access": ["alpha"]}`), emptyMetaMap(), parse(t, `{"name": "", "channels": ["alpha"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_access": ["beta", "gamma"]}`), emptyMetaMap(), parse(t, `{"name": "", "channels": ["beta"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{"_access": ["delta"]}`), emptyMetaMap(), parse(t, `{"name": "", "channels": ["beta"]}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingChannelAccess))
}

func TestRequireAdmin(t *testing.T) {
	ctx := base.TestCtx(t)
	const funcSource = `function(doc, oldDoc) { requireAdmin() }`
	runner, err := NewSyncRunner(ctx, funcSource, 0)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{}`), emptyMetaMap(), parse(t, `{}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{}`), emptyMetaMap(), parse(t, `{"name": ""}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{}`), emptyMetaMap(), parse(t, `{"name": "GUEST"}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(ctx, parse(t, `{}`), parse(t, `{}`), emptyMetaMap(), parse(t, `{"name": "beta"}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
}

// Helpers
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
