//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import (
	"encoding/json"
)

// A set of channel names, represented as a map.
// A Set contains a channel iff the channel name maps to true.
type Set map[string]bool

// Creates a new Set from an array of strings.
func SetFromArray(names []string) Set {
	result := make(Set, len(names))
	for _, name := range names {
		result[name] = true
	}
	return result
}

// Converts a Set to an array of strings (ordering is undefined).
func (set Set) ToArray() []string {
	result := make([]string, 0, len(set))
	for name, present := range set {
		if present {
			result = append(result, name)
		}
	}
	return result
}

func (set Set) AddAll(strings []string) {
	for _, s := range strings {
		set[s] = true
	}
}

func (set Set) Remove(name string) {
	delete(set, name)
}

// JSON encoding/decoding:

func (set Set) MarshalJSON() ([]byte, error) {
	return json.Marshal(set.ToArray())
}

func (set Set) UnmarshalJSON(data []byte) error {
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}
	if len(set) > 0 {
		panic("unmarshaling into non-empty Set") // I don't _think_ this can ever occur?
	}
	for _, name := range names {
		set[name] = true
	}
	return nil
}
