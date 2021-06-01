/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package clistruct

import (
	"encoding/json"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStruct struct {
	String  string  `json:"string" help:"string field"`
	Bool    bool    `json:"bool" help:"bool field"`
	Int     int     `json:"int" help:"int field"`
	Int64   int64   `json:"int64" help:"int64 field"`
	Uint    uint    `json:"uint" help:"uint field"`
	Uint64  uint64  `json:"uint64" help:"uint64 field"`
	Float64 float64 `json:"float64" help:"float64 field"`

	Struct struct {
		String string `json:"string" help:"string field"`
	} `json:"struct" help:"struct field"`

	StringSlice  []string      `json:"stringslice" help:"string slice field"`
	JSONNumber   json.Number   `json:"jsonnumber" help:"json.Number field"`
	TimeDuration time.Duration `json:"duration" help:"duration field"`

	pointerStruct // embedded struct with pointer types

	// Intentionally unhandled fields/types - Causes panic in MustRegisterFlags for easy/early detection.
	// unexported  string            `json:"unexported"`
	// Int8        int8              `json:"int8" help:"int8 field"`
	// Int16       int16             `json:"int16" help:"int16 field"`
	// Int32       int32             `json:"int32" help:"int32 field"`
	// Uint8       uint8             `json:"uint8" help:"uint8 field"`
	// Uint16      uint16            `json:"uint16" help:"uint16 field"`
	// Uint32      uint32            `json:"uint32" help:"uint32 field"`
	// Float32     float32           `json:"float32" help:"float32 field"`
	// Map         map[string]string `json:"map" help:"map field"`
}

type pointerStruct struct {
	String  *string  `json:"stringptr" help:"string pointer"`
	Bool    *bool    `json:"boolptr" help:"bool pointer"`
	Int     *int     `json:"intptr" help:"int pointer"`
	Int64   *int64   `json:"int64ptr" help:"int64 pointer"`
	Uint    *uint    `json:"uintptr" help:"uint pointer"`
	Uint64  *uint64  `json:"uint64ptr" help:"uint64 pointer"`
	Float64 *float64 `json:"float64ptr" help:"float64 pointer"`

	StringSlice  *[]string      `json:"stringsliceptr" help:"string slice pointer"`
	JSONNumber   *json.Number   `json:"jsonnumberptr" help:"json.Number pointer"`
	TimeDuration *time.Duration `json:"durationptr" help:"duration pointer"`

	Struct *stringStruct `json:"structptr" help:"struct pointer"`
}

type stringStruct struct {
	String string `json:"string" help:"string field"`
}

// TestCLIStructTypes tests that all handled types can be assigned values through a mapped cli flag.
func TestCLIStructTypes(t *testing.T) {
	tests := map[string]struct {
		flag   []string // flag.Parse supports `-k v` and `-k=v` style so test both
		assert func(*testing.T, testStruct)
	}{
		"string": {
			flag: []string{"-string=foo"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, "foo", val.String)
			},
		},
		"bool": {
			flag: []string{"-bool"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, true, val.Bool)
			},
		},
		"int": {
			flag: []string{"-int", "42"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, 42, val.Int)
			},
		},
		"int64": {
			flag: []string{"-int64=-9999999"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, int64(-9999999), val.Int64)
			},
		},
		"uint": {
			flag: []string{"-uint=42"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, uint(42), val.Uint)
			},
		},
		"uint64": {
			flag: []string{"-uint64", "99999999"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, uint64(99999999), val.Uint64)
			},
		},
		"float64": {
			flag: []string{"-float64=1234567890.0123456789"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, 1234567890.0123456789, val.Float64)
			},
		},
		"struct.string": {
			flag: []string{"-struct.string=structstring"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, "structstring", val.Struct.String)
			},
		},
		"stringslice": {
			flag: []string{"-stringslice", "a,b,c,d"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, []string{"a", "b", "c", "d"}, val.StringSlice)
			},
		},
		"json.Number": {
			flag: []string{"-jsonnumber", "3.1415926535897932384626433832795028841971693"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, json.Number("3.1415926535897932384626433832795028841971693"), val.JSONNumber)
			},
		},
		"time.Duration": {
			flag: []string{"-duration=25m14s"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t,
					25*time.Minute+
						14*time.Second,
					val.TimeDuration)
			},
		},
		"stringptr": {
			flag: []string{"-stringptr=bar"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.String)
				assert.Equal(t, "bar", *val.pointerStruct.String)
			},
		},
		"boolptr": {
			flag: []string{"-boolptr"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Bool)
				assert.Equal(t, true, *val.pointerStruct.Bool)
			},
		},
		"intptr": {
			flag: []string{"-intptr", "64"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Int)
				assert.Equal(t, 64, *val.pointerStruct.Int)
			},
		},
		"int64ptr": {
			flag: []string{"-int64ptr=-777777777"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Int64)
				assert.Equal(t, int64(-777777777), *val.pointerStruct.Int64)
			},
		},
		"uintptr": {
			flag: []string{"-uintptr=128"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Uint)
				assert.Equal(t, uint(128), *val.pointerStruct.Uint)
			},
		},
		"uint64ptr": {
			flag: []string{"-uint64ptr", "777777777"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Uint64)
				assert.Equal(t, uint64(777777777), *val.pointerStruct.Uint64)
			},
		},
		"float64ptr": {
			flag: []string{"-float64ptr=91234567890.012345678"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Float64)
				assert.Equal(t, 91234567890.012345678, *val.pointerStruct.Float64)
			},
		},
		"structptr.string": {
			flag: []string{"-structptr.string=structptrstring"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.Struct)
				assert.Equal(t, "structptrstring", val.pointerStruct.Struct.String)
			},
		},
		"stringsliceptr": {
			flag: []string{"-stringsliceptr", "x,y,z"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.StringSlice)
				assert.Equal(t, []string{"x", "y", "z"}, *val.pointerStruct.StringSlice)
			},
		},
		"json.Numberptr": {
			flag: []string{"-jsonnumberptr", "3.1415926535897932384626433832795028841971693"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.JSONNumber)
				assert.Equal(t, json.Number("3.1415926535897932384626433832795028841971693"), *val.pointerStruct.JSONNumber)
			},
		},
		"time.Durationptr": {
			flag: []string{"-durationptr=3h14m25s15ms"},
			assert: func(t *testing.T, val testStruct) {
				require.NotNil(t, val.pointerStruct.TimeDuration)
				assert.Equal(t,
					3*time.Hour+
						14*time.Minute+
						25*time.Second+
						15*time.Millisecond,
					*val.pointerStruct.TimeDuration)
			},
		},
	}

	// cli args as they're seen from os.Args (with no parsing of flag/values)
	args := make([]string, 0)
	for _, a := range tests {
		args = append(args, a.flag...)
	}

	var val testStruct
	err := parseTagsForArgs(&val, args)
	require.NoError(t, err)

	for name, a := range tests {
		t.Run(name, func(t *testing.T) {
			a.assert(t, val)
		})
	}
}

func TestSkippedFields(t *testing.T) {
	type test struct {
		A string `json:"a" help:"foo"`
		B string `help:"bar"`
		C string `json:"-"`
		D string `json:",omitempty"`
	}

	var val test
	err := parseTagsForArgs(&val, []string{"-a=1", "-B", "2", "-D=4"})
	require.NoError(t, err)
	assert.Equal(t, "1", val.A)
	assert.Equal(t, "2", val.B)
	assert.Equal(t, "", val.C)
	assert.Equal(t, "4", val.D)

	val = test{}
	err = parseTagsForArgs(&val, []string{"-C", "3"})
	require.EqualError(t, err, "flag provided but not defined: -C")
	assert.Equal(t, "", val.C)
}

func TestUndefinedFlag(t *testing.T) {
	var val struct {
		A bool `json:"a" help:"foo"`
		B bool `help:"bar"`
	}

	err := parseTagsForArgs(&val, []string{"-b"})
	assert.EqualError(t, err, "flag provided but not defined: -b")
}

func TestEmbeddedStruct(t *testing.T) {
	type innerStruct struct {
		C struct {
			D struct {
				E bool `json:"e" help:"foo"`
			}
		} `json:"c"`
	}

	var val struct {
		A struct {
			innerStruct // b
		} `json:"a"`
	}

	err := parseTagsForArgs(&val, []string{"-a.c.D.e"})
	require.NoError(t, err)

	assert.True(t, val.A.C.D.E)
}

func TestInvalidTypeErrors(t *testing.T) {
	var val struct {
		A map[string]bool `json:"a"`
	}

	err := parseTagsForArgs(&val, []string{"-a=b"})
	assert.EqualError(t, err, `unsupported type *map[string]bool to assign flag to struct field "a" - try a flag.Value implementing wrapper for the type (e.g: stringSliceValue)`)

	err = parseTagsForArgs(1234, []string{""})
	assert.EqualError(t, err, "expected val to be a struct, but was int")
}

func TestNilFlagSet(t *testing.T) {
	var val struct{}
	err := registerFlags(nil, &val, jsonStructNameTag)
	assert.EqualError(t, err, "expected flag.FlagSet in RegisterFlags but got nil")
}

// parseTagsForArgs is a convenience function for testing without callers having to deal with their own FlagSets.
func parseTagsForArgs(val interface{}, args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	err := registerFlags(fs, val, jsonStructNameTag)
	if err != nil {
		return err
	}
	return fs.Parse(args)
}

func TestRemoveJSONTagOpts(t *testing.T) {
	tests := []struct {
		input      string
		expected   string
		shouldSkip bool
	}{
		{input: "foo", expected: "foo"},
		{input: "foo,", expected: "foo"},
		{input: "foo,omitempty", expected: "foo"},
		{input: "foo,asdf", expected: "foo"},
		{input: "-", expected: "", shouldSkip: true},
		{input: "-,", expected: "-"},
		{input: ",omitempty", expected: ""},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actual, skip := removeJSONTagOpts(test.input)
			assert.Equal(t, test.expected, actual)
			assert.Equal(t, test.shouldSkip, skip)
		})
	}
}
