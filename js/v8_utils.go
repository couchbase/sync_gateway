package js

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go"
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
	if val.IsString() || val.IsStringObject() {
		return val.String(), nil
	} else if val.IsNumber() || val.IsNumberObject() {
		// TODO: Detect BigInts and convert to big.Int?
		return intify(val.Number()), nil
	} else if val.IsBoolean() {
		return val.Boolean(), nil
	} else if val.IsNullOrUndefined() {
		return nil, nil
	} else if val.IsBigInt() {
		big := val.BigInt()
		if big.IsInt64() {
			return big.Int64(), nil
		}
		return big, nil
	} else {
		// Otherwise round-trip through JSON:
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
	if f == math.Floor(f) && f >= float64(kMinFloat64SafeInt) && f < float64(kMaxFloat64SafeInt) {
		if i64 := int64(f); i64 >= math.MinInt && i64 <= math.MaxInt {
			return int(i64) // Return int if possible
		} else {
			return i64 // Return int64 if out of range of 32-bit int
		}
	} else {
		return f // Return float64 if not integral or not in range of int64
	}
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
		return v8.NewValue(ctx.Isolate(), string(jsonBytes))
	}
}

// Creates a JS number value from an integer.
func newInt[T int | int64 | int32](iso *v8.Isolate, i T) *v8.Value {
	// Use `float64` if possible w/o data loss, since that's JS's native number representation;
	// else use `int64` which will convert to a V8::BigInt.
	var val any
	if i64 := int64(i); i64 >= kMinFloat64SafeInt && i64 <= kMaxFloat64SafeInt {
		val = float64(i)
	} else {
		val = i64
	}
	return mustSucceed(v8.NewValue(iso, val))
}

// Creates a v8.Value from a Go number, string, bool or nil.
// As a convenience, also accepts `*v8.Value` and `*v8.Object` and passes them through.
func newValue(iso *v8.Isolate, val any) (*v8.Value, error) {
	if val == nil {
		return v8.Null(iso), nil // v8.NewValue panics if given nil :-p
	}
	v8val, err := v8.NewValue(iso, val)
	if err != nil {
		switch val := val.(type) {
		case int:
			// v8.NewValue doesn't handle `int` for some reason
			return newInt(iso, val), nil
		case json.Number:
			return newValueFromJSONNumber(iso, val)
		case *v8.Value:
			// v8.NewValue rejects the types `*v8.Value` and `*v8.Object`, which seems odd. Handle them:
			return val, nil
		case *v8.Object:
			return val.Value, nil
		}
	}
	return v8val, err
}

// Creates a v8.Value from a json.Number (string) -- will be a regular number or a bigint
func newValueFromJSONNumber(iso *v8.Isolate, val json.Number) (*v8.Value, error) {
	if i, err := val.Int64(); err == nil {
		return newInt(iso, i), nil
	} else if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
		// if int conversion failed because it's too large, try a big.Int, which will be
		// converted to a JS bigint:
		if ibig := bigIntWithString(string(val)); ibig != nil {
			return v8.NewValue(iso, ibig)
		}
	}
	if f, err := val.Float64(); err == nil {
		return v8.NewValue(iso, f)
	} else {
		return nil, err
	}
}

// Parses a string to a big.Int
func bigIntWithString(str string) *big.Int {
	ibig := new(big.Int)
	if ibig, ok := ibig.SetString(str, 10); ok {
		return ibig
	} else {
		return nil
	}
}

// Range of integers that can be stored in a float64 (or a JavaScript number) without losing
// accuracy (https://www.ecma-international.org/ecma-262/5.1/#sec-8.5)
const kMaxFloat64SafeInt = 1<<53 - 1
const kMinFloat64SafeInt = -kMaxFloat64SafeInt

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
