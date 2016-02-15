//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// An immutable set of strings, represented as a map.
type Set map[string]present

type present struct{}

// Creates a new Set from an array of strings.
func SetFromArray(names []string) Set {
	result := make(Set, len(names))
	for _, name := range names {
		result[name] = present{}
	}
	return result
}

// Creates a new Set from zero or more inline string arguments.
func SetOf(names ...string) Set {
	return SetFromArray(names)
}

// Converts a Set to an array of strings (ordering is undefined).
func (set Set) ToArray() []string {
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	return result
}

func (set Set) String() string {
	list := set.ToArray()
	sort.Strings(list)
	return fmt.Sprintf("{%s}", strings.Join(list, ", "))
}

func (set Set) copy() Set {
	result := make(Set, len(set))
	for name := range set {
		result[name] = present{}
	}
	return result
}

// Returns true if the set includes the channel.
func (set Set) Contains(ch string) bool {
	_, exists := set[ch]
	return exists
}

func (set Set) Equals(other Set) bool {
	if len(other) != len(set) {
		return false
	}
	for name := range set {
		if _, exists := other[name]; !exists {
			return false
		}
	}
	return true
}

// Returns the union of two sets.
func (set Set) Union(other Set) Set {
	if len(set) == 0 {
		return other
	} else if len(other) == 0 {
		return set
	}
	result := set.copy()
	for ch := range other {
		result[ch] = present{}
	}
	return result
}

// Returns a set with any instance of 'str' removed
func (set Set) Removing(str string) Set {
	if _, exists := set[str]; exists {
		set = set.copy()
		delete(set, str)
	}
	return set
}

// JSON encoding/decoding:

func (set Set) MarshalJSON() ([]byte, error) {
	if set == nil {
		return []byte("null"), nil
	}
	list := set.ToArray()
	sort.Strings(list) // sort the array so it's written in a consistent order; helps testability
	return json.Marshal(list)
}

func (setPtr *Set) UnmarshalJSON(data []byte) error {
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}
	if names == nil {
		*setPtr = nil
		return nil
	}
	set := Set{}
	for _, name := range names {
		set[name] = present{}
	}
	*setPtr = set
	return nil
}
