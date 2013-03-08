//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/sdegutis/go.assert"
	"testing"
)

func TestValidatorFunction(t *testing.T) {
	validator, err := NewValidator(`function(doc, oldDoc) {
										if (doc.foo !=oldDoc.foo)
											throw({forbidden: "bad"});}`)
	assertNoError(t, err, "Couldn't create validator")

	status, msg, err := validator.callValidator(`{"foo":1}`, `{"foo": 1}`, nil)
	assertNoError(t, err, "callValidator failed")
	assert.Equals(t, status, 200)

	status, msg, err = validator.callValidator(`{"foo":1}`, `{"foo": 0}`, nil)
	assertNoError(t, err, "callValidator failed")
	assert.Equals(t, status, 403)
	assert.Equals(t, msg, "bad")
}

func TestValidatorException(t *testing.T) {
	validator, err := NewValidator(`function(doc,oldDoc) {var x; return x[5];}`)
	assertNoError(t, err, "Couldn't create validator")

	_, _, err = validator.callValidator(`{"foo":1}`, `{"foo": 1}`, nil)
	assert.True(t, err != nil)
}

func TestValidatorUser(t *testing.T) {
	validator, err := NewValidator(`function(doc,oldDoc,userCtx) {
										if (doc.owner != userCtx.name)
											throw({"forbidden": userCtx.name});}`)
	assertNoError(t, err, "Couldn't create validator")

	fred := &auth.User{Name: "fred"}

	status, _, err := validator.callValidator(`{"owner":"fred"}`, `{"owner":"fred"}`, fred)
	assertNoError(t, err, "callValidator failed")
	assert.Equals(t, status, 200)

	eve := &auth.User{Name: "eve"}

	status, msg, err := validator.callValidator(`{"owner":"fred"}`, `{"owner":"fred"}`, eve)
	assertNoError(t, err, "callValidator failed")
	assert.Equals(t, status, 403)
	assert.Equals(t, msg, "eve")
}
