//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"fmt"
	"sort"
	"strings"
)

// An set of strings, represented as a map.
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

// Returns the union of two sets as a new set.
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

// Updates the set based on the contents of another set
func (set Set) Update(other Set) Set {
	if len(set) == 0 {
		return other
	} else if len(other) == 0 {
		return set
	}
	for ch := range other {
		set[ch] = present{}
	}
	return set
}

func (set Set) UpdateWithSlice(slice []string) Set {
	if len(slice) == 0 {
		return set
	} else if len(set) == 0 {
		set = make(Set, len(slice))
	}
	for _, ch := range slice {
		set[ch] = present{}
	}
	return set
}

// Adds a value to a set
func (set Set) Add(value string) Set {
	set[value] = present{}
	return set
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
	return JSONMarshal(list)
}

func (setPtr *Set) UnmarshalJSON(data []byte) error {
	var names []string
	if err := JSONUnmarshal(data, &names); err != nil {
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
