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
	String       string        `cli:"string" help:"string field"`
	Bool         bool          `cli:"bool" help:"bool field"`
	Int          int           `cli:"int" help:"int field"`
	Int64        int64         `cli:"int64" help:"int64 field"`
	Uint         uint          `cli:"uint" help:"uint field"`
	Uint64       uint64        `cli:"uint64" help:"uint64 field"`
	Float64      float64       `cli:"float64" help:"float64 field"`
	JSONNumber   json.Number   `cli:"jsonnumber" help:"json.Number field"`
	TimeDuration time.Duration `cli:"duration" help:"duration field"`
	Struct       struct {
		String string `cli:"string" help:"string field"`
	} `cli:"struct" help:"struct field"`

	// Intentionally unhandled fields/types - Causes panic in MustRegisterFlags for easy/early detection.
	// StringSlice []string          `cli:"stringslice" help:"string slice field"`
	// unexported  string            `cli:"unexported"`
	// Int8        int8              `cli:"int8" help:"int8 field"`
	// Int16       int16             `cli:"int16" help:"int16 field"`
	// Int32       int32             `cli:"int32" help:"int32 field"`
	// Uint8       uint8             `cli:"uint8" help:"uint8 field"`
	// Uint16      uint16            `cli:"uint16" help:"uint16 field"`
	// Uint32      uint32            `cli:"uint32" help:"uint32 field"`
	// Float32     float32           `cli:"float32" help:"float32 field"`
	// Map         map[string]string `cli:"map" help:"map field"`
	// Slice       []string          `cli:"slice" help:"slice field"`
	// Ptrs        struct {
	// 	String       *string        `cli:"stringptr" help:"string pointer"`
	// 	Bool         *bool          `cli:"boolptr" help:"bool pointer"`
	// 	Int          *int           `cli:"intptr" help:"int pointer"`
	// 	Int64        *int64         `cli:"int64ptr" help:"int64 pointer"`
	// 	Uint         *uint          `cli:"uintptr" help:"uint pointer"`
	// 	Uint64       *uint64        `cli:"uint64ptr" help:"uint64 pointer"`
	// 	Float64      *float64       `cli:"float64ptr" help:"float64 pointer"`
	// 	JSONNumber   *json.Number   `cli:"jsonnumberptr" help:"json.Number pointer"`
	// 	TimeDuration *time.Duration `cli:"durationptr" help:"duration pointer"`
	// } `cli:"ptrs" help:"pointer types"`
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
		"json.Number": {
			flag: []string{"-jsonnumber", "3.1415926535897932384626433832795028841971693"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, json.Number("3.1415926535897932384626433832795028841971693"), val.JSONNumber)
			},
		},
		"time.Duration": {
			flag: []string{"-duration=25m14s"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, 25*time.Minute+14*time.Second, val.TimeDuration)
			},
		},
		"struct.string": {
			flag: []string{"-struct.string=structstring"},
			assert: func(t *testing.T, val testStruct) {
				assert.Equal(t, "structstring", val.Struct.String)
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
	var val struct {
		A bool `cli:"a" help:"foo"`
		B bool `help:"bar"`
	}

	err := parseTagsForArgs(&val, []string{"-a"})
	require.NoError(t, err)

	assert.True(t, val.A)
	assert.False(t, val.B)
}

func TestUndefinedFlag(t *testing.T) {
	var val struct {
		A bool `cli:"a" help:"foo"`
		B bool `help:"bar"`
	}

	err := parseTagsForArgs(&val, []string{"-b"})
	assert.EqualError(t, err, "flag provided but not defined: -b")
}

func TestEmbeddedStructs(t *testing.T) {
	var val struct {
		A struct {
			B struct {
				C struct {
					D struct {
						E struct {
							F bool `cli:"f" help:"foo"`
						} `cli:""`
					} `cli:""`
				} `cli:"c"`
			} `cli:""`
		} `cli:"a"`
	}

	err := parseTagsForArgs(&val, []string{"-a.c.f"})
	require.NoError(t, err)

	assert.True(t, val.A.B.C.D.E.F)
}

func TestEmbeddedInvalidType(t *testing.T) {
	var val struct {
		A struct {
			B string `cli:""`
		} `cli:"a"`
	}

	err := parseTagsForArgs(&val, []string{"-a.b=foo"})
	assert.EqualError(t, err, `field "B" can't embed for a non-struct type: string`)
}

func TestInvalidTypeErrors(t *testing.T) {
	var val struct {
		A []string `cli:"a"`
	}

	err := parseTagsForArgs(&val, []string{"-a=b"})
	assert.EqualError(t, err, `unsupported type []string to assign flag to struct field "a"`)

	err = parseTagsForArgs(1234, []string{""})
	assert.EqualError(t, err, "expected val to be a struct, but was int")
}

func TestNilFlagSet(t *testing.T) {
	var val struct{}
	err := registerFlags(nil, &val)
	assert.EqualError(t, err, "expected flag.FlagSet in RegisterFlags but got nil")
}

// parseTagsForArgs is a convenience function for testing without callers having to deal with their own FlagSets.
func parseTagsForArgs(val interface{}, args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	err := registerFlags(fs, val)
	if err != nil {
		return err
	}
	return fs.Parse(args)
}
