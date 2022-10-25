package functions

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"

	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go"
)

// Converts a Go string into a JS string value. Assumes this cannot fail.
// (AFAIK, v8.NewValue only fails if the input type is invalid, or V8 runs out of memory.)
func goToV8String(i *v8.Isolate, str string) *v8.Value {
	return mustSucceed(v8.NewValue(i, str))
}

// Converts a JS string to a Go string, or returns an error if the JS value isn't a string.
func v8ToGoString(val *v8.Value) ([]byte, error) {
	if val.IsString() {
		return []byte(val.String()), nil
	} else if val.IsNullOrUndefined() {
		return nil, nil
	} else {
		return nil, fmt.Errorf("JavaScript returned non-string, actually %v", val)
	}
}

// Converts a JS string of JSON into a Go map.
func v8JSONToGo(val *v8.Value) (map[string]any, error) {
	if jsonStr, err := v8ToGoString(val); err != nil {
		return nil, err
	} else if jsonStr == nil {
		return nil, nil
	} else {
		var result map[string]any
		if err = json.Unmarshal(jsonStr, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
}

// Converts a result and error to a JSON-encoded string and error.
// If the input has an error, just passes it through.
// Otherwise it JSON-encodes the result and returns it.
func returnAsJSON(result any, err error) (any, error) {
	if result == nil || err != nil {
		return nil, err
	} else if jsonBytes, err := json.Marshal(result); err != nil {
		return nil, err
	} else {
		return string(jsonBytes), nil
	}
}

// Encodes a Go value to JSON and returns the string as a V8 value.
func goToV8JSON(ctx *v8.Context, obj any) (*v8.Value, error) {
	if obj == nil {
		return v8.Null(ctx.Isolate()), nil
	} else if jsonBytes, err := json.Marshal(obj); err != nil {
		return nil, err
	} else {
		return v8.NewValue(ctx.Isolate(), string(jsonBytes))
	}
}

// Calls v8.Isolate.ThrowException, with the Go error's string as the message.
func v8Throw(i *v8.Isolate, err error) *v8.Value {
	var errStr string
	if httpErr, ok := err.(*base.HTTPError); ok {
		errStr = fmt.Sprintf("[%d] %s", httpErr.Status, httpErr.Message)
	} else {
		errStr = err.Error()
	}
	return i.ThrowException(goToV8String(i, errStr))
}

// Gets a named method of a JS object. Panics if not found.
func mustGetV8Fn(owner *v8.Object, name string) *v8.Function {
	fnVal := mustSucceed(owner.Get(name))
	return mustSucceed(fnVal.AsFunction())
}

// (This detects the way HTTP statuses are encoded into error messages by the class HTTPError in types.ts.)
var kHTTPErrRegexp = regexp.MustCompile(`^(Error:\s*)?\[(\d\d\d)\]\s+(.*)`)

// Looks for an HTTP status in an error message; if so, returns it as an HTTPError; else nil.
func unpackJSErrorStr(jsErrMessage string) error {
	if m := kHTTPErrRegexp.FindStringSubmatch(jsErrMessage); m != nil {
		if status, err := strconv.ParseUint(m[2], 10, 16); err == nil {
			return &base.HTTPError{Status: int(status), Message: m[3]}
		}
	}
	return nil
}

// Postprocesses an error returned from running JS code, detecting the HTTPError type defined in types.ts. If the error is a v8.JSError, looks for an HTTP status in brackets at the start of the message; if found, returns a base.HTTPError.
func unpackJSError(err error) error {
	if jsErr, ok := err.(*v8.JSError); ok {
		if unpackedErr := unpackJSErrorStr(jsErr.Message); unpackedErr != nil {
			return unpackedErr
		}
	}
	return err
}

// Simple utility to wrap a function that returns a value and an error; returns just the value, panicking if there was an error.
// This is kind of equivalent to those 3-prong to 2-prong electric plug adapters...
// Needless to say, it should only be used if you know the error cannot occur, or that if it occurs something is very, very wrong.
func mustSucceed[T any](result T, err error) T {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: expected a %T, got %v", result, err)
	}
	return result
}
