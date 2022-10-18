/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

type mockFZDelegate struct {
}

func (d *mockFZDelegate) query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rows []any, err error) {
	return nil, base.HTTPErrorf(http.StatusNotImplemented, "query unimplemented")
}

func (d *mockFZDelegate) get(docID string, asAdmin bool) (doc map[string]any, err error) {
	return nil, base.HTTPErrorf(http.StatusNotImplemented, "get unimplemented")
}

func (d *mockFZDelegate) save(docID string, doc map[string]any, asAdmin bool) (revID string, err error) {
	return "", base.HTTPErrorf(http.StatusNotImplemented, "save unimplemented")
}

func (d *mockFZDelegate) delete(docID string, doc map[string]any, asAdmin bool) (err error) {
	return base.HTTPErrorf(http.StatusNotImplemented, "delete unimplemented")
}

func TestFunctionizer(t *testing.T) {
	delegate := mockFZDelegate{}
	fz, err := NewFunctionizer(&kFunctionizerFnsConfig, nil, &delegate)
	if !assert.NoError(t, err) {
		return
	}
	defer fz.Close()

	ctx, err := fz.NewContext(nil, nil, nil)
	assert.NoError(t, err)
	defer ctx.Close()

	result, err := ctx.CallFunction("square", map[string]any{"numero": 13})
	assert.NoError(t, err)
	assert.Equal(t, "169", result)
}

var kFunctionizerFnsConfig = FunctionsConfig{
	Definitions: FunctionsDefs{
		"square": &FunctionConfig{
			Type:  "javascript",
			Code:  "function(context, args) {return args.numero * args.numero;}",
			Args:  []string{"numero"},
			Allow: &Allow{Channels: []string{"wonderland"}},
		},
	},
}
