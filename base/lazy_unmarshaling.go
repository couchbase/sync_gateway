//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import "encoding/json"

// A wrapper around a value of type T, for use as a field in a struct that's marshaled to and
// from JSON. (T itself must be JSON-marshalable and -unmarshalable.) It avoids the overhead of
// unmarshaling a value that may never be read.
//
// When unmarshaled from JSON, the LazyUnmarshaling value just captures the raw JSON and defers
// unmarshaling until the first time the Get method is called.
//
// If the Get method is not called and the LazyUnmarshaling value needs to JSON-marshal itself,
// it just returns the original JSON it was read from.
type LazyUnmarshaling[T any] struct {
	value     *T
	marshaled json.RawMessage
	err       error
}

// Gets (a pointer to) the current value.
// If it was initialized by unmarshaling from JSON, it will unmarshal the value the first time
// this is called (and discard the JSON.)
// If there is no value, returns nil.
// If JSON unmarshaling failed, will return the error.
func (lazy *LazyUnmarshaling[T]) Get() (*T, error) {
	if lazy.err != nil {
		return nil, lazy.err
	} else if lazy.value == nil && lazy.marshaled != nil {
		lazy.err = JSONUnmarshal(lazy.marshaled, &lazy.value)
		lazy.marshaled = nil
	}
	return lazy.value, lazy.err
}

// Stores a new value (by pointer.)
func (lazy *LazyUnmarshaling[T]) Set(val *T) {
	lazy.value = val
	lazy.marshaled = nil
	lazy.err = nil
}

func (lazy LazyUnmarshaling[T]) MarshalJSON() ([]byte, error) {
	if lazy.marshaled != nil {
		return lazy.marshaled, nil
	} else if lazy.value != nil {
		return JSONMarshal(*lazy.value)
	} else {
		return []byte("null"), nil
	}
}

func (lazy *LazyUnmarshaling[T]) UnmarshalJSON(data []byte) error {
	lazy.marshaled = data
	lazy.err = nil
	lazy.value = nil
	return nil
}

//////// MAP VERSION:

// Variant of LazyUnmarshaling for maps. Slightly more convenient since you don't have to deal
// with pointers to maps. The value type is map[string]T.
type LazyUnmarshalingMap[T any] struct {
	value     map[string]T
	marshaled json.RawMessage
	err       error
}

// Gets the current value.
// If it was initialized by unmarshaling from JSON, it will unmarshal the value the first time
// this is called (and discard the JSON.)
// If there is no value, returns nil.
// If JSON unmarshaling failed, will return the error.
func (lazy *LazyUnmarshalingMap[T]) Get() (map[string]T, error) {
	if lazy.err != nil {
		return nil, lazy.err
	} else if lazy.value == nil && lazy.marshaled != nil {
		lazy.err = JSONUnmarshal(lazy.marshaled, &lazy.value)
		lazy.marshaled = nil
	}
	return lazy.value, lazy.err
}

// Stores a new value.
func (lazy *LazyUnmarshalingMap[T]) Set(val map[string]T) {
	lazy.value = val
	lazy.marshaled = nil
	lazy.err = nil
}

func (lazy LazyUnmarshalingMap[T]) MarshalJSON() ([]byte, error) {
	if lazy.marshaled != nil {
		return lazy.marshaled, nil
	} else {
		return JSONMarshal(lazy.value)
	}
}

func (lazy *LazyUnmarshalingMap[T]) UnmarshalJSON(data []byte) error {
	lazy.marshaled = data
	lazy.err = nil
	lazy.value = nil
	return nil
}
