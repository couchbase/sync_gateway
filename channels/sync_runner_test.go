package channels

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
)

func TestRequireUser(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireUser(oldDoc._names) }`
	runner, err := NewSyncRunner(funcSource)
	goassert.Equals(t, err, nil)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": "alpha"}`), parse(`{"name": "alpha"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": ["beta", "gamma"]}`), parse(`{"name": "beta"}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_names": ["delta"]}`), parse(`{"name": "beta"}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorWrongUser))
}

func TestRequireRole(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireRole(oldDoc._roles) }`
	runner, err := NewSyncRunner(funcSource)
	goassert.Equals(t, err, nil)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["alpha"]}`), parse(`{"name": "", "roles": {"alpha":""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["beta", "gamma"]}`), parse(`{"name": "", "roles": {"beta": ""}}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_roles": ["delta"]}`), parse(`{"name": "", "roles": {"beta":""}}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorMissingRole))
}

func TestRequireAccess(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAccess(oldDoc._access) }`
	runner, err := NewSyncRunner(funcSource)
	goassert.Equals(t, err, nil)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["alpha"]}`), parse(`{"name": "", "channels": ["alpha"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["beta", "gamma"]}`), parse(`{"name": "", "channels": ["beta"]}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{"_access": ["delta"]}`), parse(`{"name": "", "channels": ["beta"]}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorMissingChannelAccess))
}

func TestRequireAdmin(t *testing.T) {
	const funcSource = `function(doc, oldDoc) { requireAdmin() }`
	runner, err := NewSyncRunner(funcSource)
	goassert.Equals(t, err, nil)
	var result interface{}
	result, _ = runner.Call(parse(`{}`), parse(`{}`), parse(`{}`))
	assertNotRejected(t, result)
	result, _ = runner.Call(parse(`{}`), parse(`{}`), parse(`{"name": ""}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(parse(`{}`), parse(`{}`), parse(`{"name": "GUEST"}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorAdminRequired))
	result, _ = runner.Call(parse(`{}`), parse(`{}`), parse(`{"name": "beta"}`))
	assertRejected(t, result, base.HTTPErrorf(403, base.SyncFnErrorAdminRequired))
}

// Helpers
func assertRejected(t *testing.T, result interface{}, err *base.HTTPError) {
	r, ok := result.(*ChannelMapperOutput)
	goassert.True(t, ok)
	goassert.DeepEquals(t, r.Rejection, err)
}

func assertNotRejected(t *testing.T, result interface{}) {
	r, ok := result.(*ChannelMapperOutput)
	if !ok || r.Rejection != nil {
		t.Fatalf("%v", r.Rejection)
	}
}
