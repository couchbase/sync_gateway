//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

// A set of strings, each of which is assigned a unique non-negative integer index.
// Mapping from string->int and int->string are both efficient.
// The type parameter `T` is the custom integer type to use.
type NameSet[T ~int | ~int32 | ~int64] struct {
	byName  map[string]T // names -> indexes
	byIndex []string     // indexes -> names
}

// Initializes an empty set.
// This is optional, but allows you to pre-allocate the array and map with a given initial capacity.
func (set *NameSet[T]) Init(capacity int) {
	set.byName = make(map[string]T, capacity)
	set.byIndex = make([]string, 0, capacity)
}

// Adds a name to the set (if not already present), returning its numeric index.
// Indices are assigned in numeric order starting at 0.
func (set *NameSet[T]) Add(name string) T {
	if set.byName == nil {
		set.byName = map[string]T{}
		set.byIndex = []string{}
	}
	if i, found := set.byName[name]; found {
		return i
	} else {
		i := T(len(set.byIndex))
		set.byIndex = append(set.byIndex, name)
		set.byName[name] = i
		return i
	}
}

// The number of strings in the set.
func (set *NameSet[T]) Length() int {
	return len(set.byIndex)
}

// Returns the integer index of a name, if it's in the set.
func (set *NameSet[T]) GetIndex(name string) (T, bool) {
	i, found := set.byName[name]
	return i, found
}

// Returns the string with an index, if that index is valid.
func (set *NameSet[T]) GetString(i T) (string, bool) {
	if i >= 0 && int(i) < len(set.byIndex) {
		return set.byIndex[i], true
	} else {
		return "", false
	}
}

// Initializes or resets a NameSet from an array of unique strings.
// Note: Takes ownership of the input array -- do not modify it afterwards.
func (set *NameSet[T]) SetArray(array []string) {
	set.byName = make(map[string]T, len(array))
	set.byIndex = array
	for i, name := range array {
		set.byName[name] = T(i)
	}
}

// Returns the NameSet as an array of strings ordered by index.
func (set *NameSet[T]) AsArray() []string {
	array := make([]string, len(set.byName))
	for name, i := range set.byName {
		array[int(i)] = name
	}
	return array
}

// Encodes to JSON (as an array of unique strings.)
func (set NameSet[T]) MarshalJSON() ([]byte, error) {
	return JSONMarshal(set.byIndex)
}

// Decodes from JSON (an array of unique strings.)
func (set *NameSet[T]) UnmarshalJSON(data []byte) error {
	var array []string
	err := JSONUnmarshal(data, &array)
	if err == nil {
		set.SetArray(array)
	}
	return err
}
