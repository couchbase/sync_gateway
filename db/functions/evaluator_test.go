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
	"log"
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// A basic bring-up test of the Evaluator and TypeScript engine.
func TestEvaluator(t *testing.T) {
	env, err := NewEnvironment(&kTestFunctionsConfig, &kTestGraphQLConfig)
	if !assert.NoError(t, err) {
		return
	}
	defer env.Close()

	delegate := mockEvaluatorDelegate{}
	eval, err := env.NewEvaluator(&delegate, nil)
	assert.NoError(t, err)
	defer eval.Close()

	result, err := eval.CallFunction("square", map[string]any{"numero": 13})
	assert.NoError(t, err)
	assert.Equal(t, "169", string(result))

	result, err = eval.CallFunction("cube", map[string]any{"numero": 13})
	assert.NoError(t, err)
	assert.Equal(t, "2197", string(result))
}

//////// MOCK EVALUATOR DELEGATE (CRUD FUNCTIONS)

type mockDoc = map[string]any

type mockEvaluatorDelegate struct {
	docs map[string]mockDoc
}

var logLevelNames = []string{"none", "error", "warning", "info", "debug", "trace"}

func (d *mockEvaluatorDelegate) log(level base.LogLevel, message string) {
	log.Printf("JAVASCRIPT %s: %s", logLevelNames[level], message)
}

func (d *mockEvaluatorDelegate) checkTimeout() error { return nil }

func (d *mockEvaluatorDelegate) query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rowsJSON string, err error) {
	return "", base.HTTPErrorf(http.StatusNotImplemented, "query unimplemented")
}

func (d *mockEvaluatorDelegate) get(docID string, asAdmin bool) (doc map[string]any, err error) {
	return d.docs[docID], nil
}

func (d *mockEvaluatorDelegate) save(doc map[string]any, docID string, asAdmin bool) (saved bool, err error) {
	existingDoc := d.docs[docID]
	curRevID, curExists := existingDoc["_rev"].(string)
	if revID, exists := doc["_rev"].(string); exists {
		if revID != curRevID || !curExists {
			return false, nil
		}
	}

	var gen uint64
	if curExists {
		gen, _ = strconv.ParseUint(curRevID, 10, 64)
	}
	doc["_id"] = docID
	doc["_rev"] = strconv.FormatUint(gen+1, 10)

	if d.docs == nil {
		d.docs = map[string]mockDoc{}
	}
	d.docs[docID] = doc
	return true, nil
}

func (d *mockEvaluatorDelegate) delete(docID string, revID string, asAdmin bool) (bool, error) {
	existingDoc := d.docs[docID]
	if existingDoc == nil {
		return true, nil
	}
	if len(revID) > 0 {
		curRevID, _ := existingDoc["_rev"].(string)
		if revID != curRevID {
			return false, nil
		}
	}
	delete(d.docs, docID)
	return true, nil
}
