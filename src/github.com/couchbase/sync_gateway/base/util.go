//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func GenerateRandomSecret() string {
	randomBytes := make([]byte, 20)
	n, err := io.ReadFull(rand.Reader, randomBytes)
	if n < len(randomBytes) || err != nil {
		panic("RNG failed, can't create password")
	}
	return fmt.Sprintf("%x", randomBytes)
}

// Returns a cryptographically-random 128-bit number encoded as a hex string.
func CreateUUID() string {
	bytes := make([]byte, 16)
	n, err := rand.Read(bytes)
	if n < 16 {
		LogPanic("Failed to generate random ID: %s", err)
	}
	return fmt.Sprintf("%x", bytes)
}

// This is a workaround for an incompatibility between Go's JSON marshaler and CouchDB.
// Go parses JSON numbers into float64 type, and then when it marshals float64 to JSON it uses
// scientific notation if the number is more than six digits long, even if it's an integer.
// However, CouchDB doesn't seem to like scientific notation and throws an exception.
// (See <https://issues.apache.org/jira/browse/COUCHDB-1670>)
// Thus, this function, which walks through a JSON-compatible object and converts float64 values
// to int64 when possible.
// NOTE: This function works on generic map[string]interface{}, but *not* on types based on it,
// like db.Body. Thus, db.Body has a special FixJSONNumbers method -- call that instead.
// TODO: In Go 1.1 we will be able to use a new option in the JSON parser that converts numbers
// to a special number type that preserves the exact formatting.
func FixJSONNumbers(value interface{}) interface{} {
	switch value := value.(type) {
	case float64:
		var asInt int64 = int64(value)
		if float64(asInt) == value {
			return asInt // Representable as int, so return it as such
		}
	case map[string]interface{}:
		for k, v := range value {
			value[k] = FixJSONNumbers(v)
		}
	case []interface{}:
		for i, v := range value {
			value[i] = FixJSONNumbers(v)
		}
	default:
	}
	return value
}

var kBackquoteStringRegexp *regexp.Regexp

// Preprocesses a string containing `...`-delimited strings. Converts the backquotes into double-quotes,
// and escapes any newlines or double-quotes within them with backslashes.
func ConvertBackQuotedStrings(data []byte) []byte {
	if kBackquoteStringRegexp == nil {
		kBackquoteStringRegexp = regexp.MustCompile("`((?s).*?)[^\\\\]`")
	}
	// Find backquote-delimited strings and replace them:
	return kBackquoteStringRegexp.ReplaceAllFunc(data, func(bytes []byte) []byte {
		str := string(bytes)
		// Remove \r  and Escape newlines and double-quotes:
		str = strings.Replace(str, "\r", "", -1)
		str = strings.Replace(str, "\n", `\n`, -1)
		str = strings.Replace(str, "\t", `\t`, -1)
		str = strings.Replace(str, `"`, `\"`, -1)
		bytes = []byte(str)
		// Replace the backquotes with double-quotes
		bytes[0] = '"'
		bytes[len(bytes)-1] = '"'
		return bytes
	})
}

// Concatenates and merges multiple string arrays into one, discarding all duplicates (including
// duplicates within a single array.) Ordering is preserved.
func MergeStringArrays(arrays ...[]string) (merged []string) {
	seen := make(map[string]bool)
	for _, array := range arrays {
		for _, str := range array {
			if !seen[str] {
				seen[str] = true
				merged = append(merged, str)
			}
		}
	}
	return
}

func ToInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int64:
		return value, true
	case float64:
		return int64(value), true
	case int:
		return int64(value), true
	case json.Number:
		if n, err := value.Int64(); err == nil {
			return n, true
		}
	}
	return 0, false
}

// IntMax is an expvar.Value that tracks the maximum value it's given.
type IntMax struct {
	i  int64
	mu sync.RWMutex
}

func (v *IntMax) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.FormatInt(v.i, 10)
}

func (v *IntMax) SetIfMax(value int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if value > v.i {
		v.i = value
	}
}
