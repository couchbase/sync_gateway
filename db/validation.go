//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"fmt"
	"github.com/robertkrimen/otto"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
)

const funcWrapper = `
	function(oldDoc, newDoc, userCtx) {
		var v = %s;
		try {
			v(oldDoc, newDoc, userCtx);
			return null;
		} catch(x) {
			if (x.forbidden)
				return {status: 403, msg: x.forbidden};
			if (x.unauthorized)
				return {status: 401, msg: x.unauthorized};
			return {status: 500, msg: String(x)};
		}
	}`

// Runs CouchDB JavaScript validation functions.
type Validator struct {
	js *base.JSServer
}

type validatorResult struct {
	status  int
	message string
}

// Creates a new Validator given a CouchDB-style JavaScript validation function.
func NewValidator(funcSource string) (*Validator, error) {
	funcSource = fmt.Sprintf(funcWrapper, funcSource)
	validator := &Validator{}
	var err error
	validator.js, err = base.NewJSServer(funcSource)
	if err != nil {
		return nil, err
	}
	validator.js.After = func(result otto.Value, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		if !result.IsObject() {
			return validatorResult{200, ""}, nil
		}
		resultObj := result.Object()
		statusVal, _ := resultObj.Get("status")
		errMsg, _ := resultObj.Get("msg")
		status, _ := statusVal.ToInteger()
		return validatorResult{int(status), errMsg.String()}, nil
	}
	return validator, nil
}

func encodeUser(user *auth.User) string {
	if user == nil {
		return `{"name":null, "channels":[]}`
	}
	info := map[string]interface{}{}
	info["name"] = user.Name
	info["channels"] = user.AllChannels()
	json, _ := json.Marshal(info)
	return string(json)
}

// This is just for testing
func (validator *Validator) callValidator(newDoc string, oldDoc string, user *auth.User) (int, string, error) {
	result, err := validator.js.DirectCallFunction([]string{newDoc, oldDoc, encodeUser(user)})
	if err != nil || result == nil {
		return 0, "", err
	}
	results, _ := result.(validatorResult)
	return results.status, results.message, nil
}

func (validator *Validator) Validate(newDoc string, oldDoc string, user *auth.User) (int, string, error) {
	result, err := validator.js.CallFunction([]string{newDoc, oldDoc, encodeUser(user)})
	if err != nil || result == nil {
		return 0, "", err
	}
	results, _ := result.(validatorResult)
	return results.status, results.message, nil
}

func (validator *Validator) SetFunction(fnSource string) (bool, error) {
	return validator.js.SetFunction(fnSource)
}

func (validator *Validator) Stop() {
	validator.js.Stop()
}
