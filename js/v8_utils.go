//go:build cb_sg_v8

/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/couchbase/sync_gateway/base"
	v8 "github.com/snej/v8go"
)

// CONVERTING V8 VALUES BACK TO GO:

// Converts a JS string to a Go string.
func StringToGo(val *v8.Value) (string, bool) {
	if val.IsString() {
		return val.String(), true
	} else {
		return "", false
	}
}

// Converts a V8 value back to a Go representation.
// Recognizes JS strings, numbers, booleans. `null` and `undefined` are returned as nil.
// Other JS types are run through `JSON.stringify` and `json.Unmarshal`.
func ValueToGo(val *v8.Value) (any, error) {
	switch val.GetType() {
	case v8.UndefinedType, v8.NullType:
		return nil, nil
	case v8.FalseType:
		return false, nil
	case v8.TrueType:
		return true, nil
	case v8.NumberType:
		return intify(val.Number()), nil
	case v8.BigIntType:
		big := val.BigInt()
		if big.IsInt64() {
			return big.Int64(), nil
		}
		return big, nil
	case v8.StringType:
		return val.String(), nil
	default:
		// Otherwise detour through JSON:
		if j, err := val.MarshalJSON(); err == nil {
			var result any
			if json.Unmarshal(j, &result) == nil {
				return result, nil
			}
		}
		return nil, fmt.Errorf("couldn't convert JavaScript value `%s`", val.DetailString())
	}
}

// Converts a float64 to an int or int64 if possible without losing accuracy.
func intify(f float64) any {
	if f == math.Floor(f) && f >= float64(JavascriptMinSafeInt) && f < float64(JavascriptMaxSafeInt) {
		if i64 := int64(f); i64 >= math.MinInt && i64 <= math.MaxInt {
			return int(i64) // Return int if possible
		} else {
			return i64 // Return int64 if out of range of 32-bit int
		}
	} else {
		return f // Return float64 if not integral or not in range of int64
	}
}

// Converts a V8 array of strings to Go. Any non-strings in the array are ignored.
func StringArrayToGo(val *v8.Value) (result []string, err error) {
	obj, err := val.AsObject()
	if err == nil {
		for i := uint32(0); obj.HasIdx(i); i++ {
			if item, err := obj.GetIdx(i); err == nil && item.IsString() {
				result = append(result, item.String())
			}
		}
	}
	return
}

//////// CONVERTING GO TO V8 VALUES:

// Converts a Go string into a JS string value. Assumes this cannot fail.
// (AFAIK, v8.NewValue only fails if the input type is invalid, or V8 runs out of memory.)
func newString(i *v8.Isolate, str string) *v8.Value {
	return mustSucceed(v8.NewValue(i, str))
}

// Marshals a Go value to JSON, and returns the string as a V8 Value.
func newJSONString(ctx *v8.Context, val any) (*v8.Value, error) {
	if val == nil {
		return v8.Null(ctx.Isolate()), nil
	} else if jsonBytes, err := json.Marshal(val); err != nil {
		return nil, err
	} else {
		return ctx.NewValue(string(jsonBytes))
	}
}

//////// ERROR UTILITIES:

// Returns an error back to a V8 caller.
// Calls v8.Isolate.ThrowException, with the Go error's string as the message.
func v8Throw(i *v8.Isolate, err error) *v8.Value {
	var errStr string
	if httpErr, ok := err.(*base.HTTPError); ok {
		errStr = fmt.Sprintf("[%d] %s", httpErr.Status, httpErr.Message)
	} else {
		errStr = err.Error()
	}
	return i.ThrowException(newString(i, errStr))
}

// Simple utility to wrap a function that returns a value and an error; returns just the value, panicking if there was an error.
// This is kind of equivalent to those 3-prong to 2-prong electric plug adapters...
// Needless to say, it should only be used if you know the error cannot occur, or that if it occurs something is very, very wrong.
func mustSucceed[T any](result T, err error) T {
	if err != nil {
		panic(fmt.Sprintf(`ASSERTION FAILURE: expected a %T, got error "%v"`, result, err))
	}
	return result
}
