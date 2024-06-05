//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Regexp that matches either `${...}`, `$dd` where d is a digit, or a backslash-escaped `$`.
var kDollarSubstituteRegexp = regexp.MustCompile(`(\\\$)|(\$\{[^{}]*\})|(\$\w*)`)

// Expands patterns of the form `${arg}` or `$arg` in `pattern`.
// If braces are not given, 'arg' consists only of word characters.
// Dollar signs can be escaped with backslashes: `\$` is replaced with `$`.
//
// The `replacer` function is called with the pattern minus the dollar sign and braces,
// and its return value replaces the pattern. Any error returned is passed through.
func DollarSubstitute(pattern string, replacer func(string) (string, error)) (string, error) {
	if strings.IndexByte(pattern, '$') < 0 {
		return pattern, nil
	}
	var err error
	result := kDollarSubstituteRegexp.ReplaceAllStringFunc(pattern, func(matched string) string {
		var replacement string
		if err == nil {
			if matched == "\\$" {
				replacement = "$"
			} else if strings.HasPrefix(matched, "${") && strings.HasSuffix(matched, "}") {
				replacement, err = replacer(matched[2 : len(matched)-1])
			} else if matched != "$" {
				replacement, err = replacer(matched[1:])
			} else {
				err = HTTPErrorf(http.StatusInternalServerError, "invalid `$` argument in pattern: %s", pattern)
			}
		}
		return replacement
	})
	return result, err
}

// Evaluates a "key path", like "points[3].x.y", on a JSON-based map.
// If `lenient` is true, errors relating to the input data (missing keys, etc.) are not returned
// as errors, just a nil result.
func EvalKeyPath(root map[string]any, keyPath string, lenient bool) (reflect.Value, error) {
	// Handle the first path component specially because we can access `root` without reflection:
	var value reflect.Value
	var err error
	i := strings.IndexAny(keyPath, ".[")
	if i < 0 {
		i = len(keyPath)
	}
	key := keyPath[0:i]
	keyPath = keyPath[i:]
	firstVal := root[key]
	if firstVal == nil {
		if !lenient {
			err = HTTPErrorf(http.StatusInternalServerError, "parameter %q is not declared in 'args'", key)
		}
		return value, err
	}

	value = reflect.ValueOf(firstVal)
	if len(keyPath) == 0 {
		return value, nil
	}

	for len(keyPath) > 0 {
		ch := keyPath[0]
		keyPath = keyPath[1:]
		if ch == '.' {
			i = strings.IndexAny(keyPath, ".[")
			if i < 0 {
				i = len(keyPath)
			}
			key = keyPath[0:i]
			keyPath = keyPath[i:]

			if value.Kind() != reflect.Map {
				if !lenient {
					err = HTTPErrorf(http.StatusBadRequest, "value is not a map")
				}
				return value, err
			}
			value = value.MapIndex(reflect.ValueOf(key))
		} else if ch == '[' {
			i = strings.IndexByte(keyPath, ']')
			if i < 0 {
				return value, HTTPErrorf(http.StatusInternalServerError, "missing ']")
			}
			key = keyPath[0:i]
			keyPath = keyPath[i+1:]

			index, err := strconv.ParseUint(key, 10, 8)
			if err != nil {
				return value, err
			}
			if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
				if !lenient {
					err = HTTPErrorf(http.StatusBadRequest, "value is a %v not an array", value.Type())
				}
				return value, err
			} else if uint64(value.Len()) <= index {
				if !lenient {
					err = HTTPErrorf(http.StatusBadRequest, "array index out of range")
				}
				return value, err
			}
			value = value.Index(int(index))
		} else {
			return value, HTTPErrorf(http.StatusInternalServerError, "invalid character after a ']'")
		}
		for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
			value = value.Elem()
		}
	}
	return value, nil
}
