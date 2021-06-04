/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package clistruct provides the ability to automatically assign command line flags to fields in structs that declare `json` and optionally `help` struct tags.
package clistruct

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	jsonStructNameTag = "json"
	cliStructHelpTag  = "help"
)

// MustRegisterJSONFlags will register flags on the given FlagSet from json struct tags in the given struct.
// FlagSet.Parse() must be called _after_ this has been run.
//
// Panics if the given struct cannot be assigned to a FlagSet (unsupported types, duplicate flags, etc.)
func MustRegisterJSONFlags(fs *flag.FlagSet, cliStruct interface{}) {
	if err := registerFlags(fs, cliStruct, jsonStructNameTag); err != nil {
		panic("Can't register struct flags: " + err.Error())
	}
}

// registerFlags is the non-panicking version of MustRegisterFlags, for testing.
func registerFlags(fs *flag.FlagSet, cliStruct interface{}, structNameTag string) error {
	if fs == nil {
		return errors.New("expected flag.FlagSet in RegisterFlags but got nil")
	}

	refVal, err := reflectStructValue(cliStruct)
	if err != nil {
		return err
	}

	if err := register(fs, refVal, refVal.Type(), "", structNameTag); err != nil {
		return err
	}

	return nil
}

// register recursively assigns flags to any fields that have a cli struct tag, are addressable, and have a supported type.
func register(fs *flag.FlagSet, val reflect.Value, valType reflect.Type, namePrefix string, structNameTag string) error {
	for i := 0; i < valType.NumField(); i++ {
		valTypeField := valType.Field(i)

		// Handle embedded structs without nesting struct tag values
		if valTypeField.Type.Kind() == reflect.Struct && valTypeField.Anonymous {
			if err := register(fs, val.Field(i), val.Field(i).Type(), namePrefix, structNameTag); err != nil {
				return err
			}
			continue
		}

		flagName := valTypeField.Tag.Get(structNameTag)

		// Handle JSON tags as a special case for the options and struct field fallback handling.
		if structNameTag == jsonStructNameTag {
			var skip bool
			flagName, skip = removeJSONTagOpts(flagName)
			if skip {
				continue
			}
			if flagName == "" {
				// json.Marshal takes the struct field name in this case
				flagName = valTypeField.Name
			}
		}

		// skip untagged fields
		if flagName == "" {
			continue
		}

		// nest the given flagName under the parent tags
		if namePrefix != "" {
			flagName = namePrefix + "." + flagName
		}

		fieldType := valTypeField.Type
		fieldValue := val.Field(i)

		if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
			// deref pointers to structs in order to recurse
			fieldType = fieldType.Elem()
			if fieldValue.IsNil() {
				fieldValue.Set(reflect.New(fieldType))
			}
			fieldValue = fieldValue.Elem()
		}

		if fieldType.Kind() == reflect.Struct {
			// recurse for nested structs
			if err := register(fs, fieldValue, fieldType, flagName, structNameTag); err != nil {
				return err
			}
			continue
		}

		if !fieldValue.CanSet() {
			return fmt.Errorf("value %q (%s) can't be set - not addressable or unexported", valTypeField.Name, structNameTag)
		}

		flagHelp := valTypeField.Tag.Get(cliStructHelpTag)
		if err := assignFlagToStructFieldVar(fs, fieldValue, flagName, flagHelp); err != nil {
			return err
		}

	}
	return nil
}

// assignFlagToStructFieldVar creates a flag associated with the given struct field, if the type can be set from a flag.
func assignFlagToStructFieldVar(fs *flag.FlagSet, fieldValue reflect.Value, flagName, flagHelp string) error {

	// get the pointer for non-pointer fields
	if fieldValue.Kind() != reflect.Ptr && fieldValue.CanAddr() {
		fieldValue = fieldValue.Addr()
	}

	// The assignments below take 2 forms:
	// If v is nil, create a flag var and assign it to the struct
	// If v is not nil, bind the flag to the variable in the struct

	switch v := fieldValue.Interface().(type) {
	// These common types have flag wrappers from flag_value_wrappers.go
	case *[]string:
		if v == nil {
			tmp := make(stringSliceValue, 0)
			fieldValue.Set(reflect.ValueOf((*[]string)(&tmp)))
			fs.Var(&tmp, flagName, flagHelp)
		} else {
			fs.Var((*stringSliceValue)(v), flagName, flagHelp)
		}
	case *json.Number:
		if v == nil {
			tmp := jsonNumberFlagValue("0")
			fieldValue.Set(reflect.ValueOf((*json.Number)(&tmp)))
			fs.Var(&tmp, flagName, flagHelp)
		} else {
			fs.Var((*jsonNumberFlagValue)(v), flagName, flagHelp)
		}
	// All below types are natively handled by the flag package.
	case flag.Value:
		fs.Var(v, flagName, flagHelp)
	case *time.Duration:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Duration(flagName, 0, flagHelp)))
		} else {
			fs.DurationVar(v, flagName, *v, flagHelp)
		}
	case *bool:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Bool(flagName, false, flagHelp)))
		} else {
			fs.BoolVar(v, flagName, *v, flagHelp)
		}
	case *int:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Int(flagName, 0, flagHelp)))
		} else {
			fs.IntVar(v, flagName, *v, flagHelp)
		}
	case *int64:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Int64(flagName, 0, flagHelp)))
		} else {
			fs.Int64Var(v, flagName, *v, flagHelp)
		}
	case *uint:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Uint(flagName, 0, flagHelp)))
		} else {
			fs.UintVar(v, flagName, *v, flagHelp)
		}
	case *uint64:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Uint64(flagName, 0, flagHelp)))
		} else {
			fs.Uint64Var(v, flagName, *v, flagHelp)
		}
	case *float64:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.Float64(flagName, 0, flagHelp)))
		} else {
			fs.Float64Var(v, flagName, *v, flagHelp)
		}
	case *string:
		if v == nil {
			fieldValue.Set(reflect.ValueOf(fs.String(flagName, "", flagHelp)))
		} else {
			fs.StringVar(v, flagName, *v, flagHelp)
		}
	default:
		return fmt.Errorf("unsupported type %T to assign flag to struct field %q - try a flag.Value implementing wrapper for the type (e.g: stringSliceValue)", v, flagName)
	}

	return nil
}

// removeJSONTagOpts removes JSON tag options like 'omitempty' and 'string' from JSON struct tag values.
func removeJSONTagOpts(tagVal string) (cleanTagVal string, skip bool) {
	if idx := strings.Index(tagVal, ","); idx != -1 {
		// grab the key before opts appear
		return tagVal[:idx], false
	}
	// json:"-," is handled in the above case (where we found a separator)
	// but this case is an explicitly ignored field
	if tagVal == "-" {
		return "", true
	}
	// normal key, no opts
	return tagVal, false
}
