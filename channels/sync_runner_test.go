package channels

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequireUser(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireUser(oldDoc._names) }`
	runner, err := NewSyncRunner(funcSource)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": "alpha"}`), nil, parse(`{"name": "alpha"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": ["beta", "gamma"]}`), nil, parse(`{"name": "beta"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": ["delta"]}`), nil, parse(`{"name": "beta"}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorWrongUser))
}

func TestRequireRole(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireRole(oldDoc._roles) }`
	runner, err := NewSyncRunner(funcSource)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["alpha"]}`), nil, parse(`{"name": "", "roles": {"alpha":""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["beta", "gamma"]}`), nil, parse(`{"name": "", "roles": {"beta": ""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["delta"]}`), nil, parse(`{"name": "", "roles": {"beta":""}}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingRole))
}

func TestRequireAccess(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAccess(oldDoc._access) }`
	runner, err := NewSyncRunner(funcSource)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["alpha"]}`), nil, parse(`{"name": "", "channels": ["alpha"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["beta", "gamma"]}`), nil, parse(`{"name": "", "channels": ["beta"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["delta"]}`), nil, parse(`{"name": "", "channels": ["beta"]}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorMissingChannelAccess))
}

func TestRequireAdmin(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAdmin() }`
	runner, err := NewSyncRunner(funcSource)
	require.NoError(t, err)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{}`), nil, parse(`{}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{}`), nil, parse(`{"name": ""}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(parse(`{}`), parse(`{}`), nil, parse(`{"name": "GUEST"}`))
	assertRejected(t, result, base.HTTPErrorf(http.StatusForbidden, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(parse(`{}`), parse(`{}`), nil, parse(`{"name": "beta"}`))
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
