// Package clistruct provides the ability to assign command line flags to fields in structs that declare `cli` and `help` struct tags.
package clistruct

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"time"
)

const (
	cliStructNameTag = "cli"
	cliStructHelpTag = "help"
)

// MustRegisterFlags will register flags on the given FlagSet from cli struct tags in the given struct.
// FlagSet.Parse() must be called _after_ this has been run.
//
// Panics if the given struct cannot be assigned to a FlagSet (unsupported types, duplicate flags, etc.)
func MustRegisterFlags(fs *flag.FlagSet, cliStruct interface{}) {
	if err := registerFlags(fs, cliStruct); err != nil {
		panic(err.Error())
	}
}

// registerFlags is the non-panicking version of MustRegisterFlags, for testing.
func registerFlags(fs *flag.FlagSet, cliStruct interface{}) error {
	if fs == nil {
		return errors.New("expected flag.FlagSet in RegisterFlags but got nil")
	}

	refVal, err := reflectStructValue(cliStruct)
	if err != nil {
		return err
	}

	if err := register(fs, refVal, ""); err != nil {
		return err
	}

	return nil
}

// register recursively assigns flags to any fields that have a cli struct tag, are addressable, and have a supported type.
func register(fs *flag.FlagSet, val reflect.Value, namePrefix string) error {
	for i := 0; i < val.Type().NumField(); i++ {
		field := val.Type().Field(i)

		flagName, ok := field.Tag.Lookup(cliStructNameTag)
		if !ok {
			// skip untagged fields
			continue
		}

		// Embed struct types without nesting when `cli:""`
		if flagName == "" {
			if field.Type.Kind() != reflect.Struct {
				return fmt.Errorf("field %q can't embed for a non-struct type: %s", field.Name, field.Type.Kind())
			}

			// recurse for embedded struct
			if err := register(fs, val.Field(i), namePrefix); err != nil {
				return err
			}
			continue
		}

		// nest the given flagName under the parent tags
		if namePrefix != "" {
			flagName = namePrefix + "." + flagName
		}

		fieldType := field.Type
		fieldValue := val.Field(i)

		if fieldType.Kind() == reflect.Struct {
			// recurse for nested structs
			if err := register(fs, fieldValue, flagName); err != nil {
				return err
			}
			continue
		}

		if !fieldValue.CanSet() {
			return fmt.Errorf("field %q can't be set - not addressable or unexported", field.Name)
		}

		flagHelp, _ := field.Tag.Lookup(cliStructHelpTag)
		if err := assignFlagToStructFieldVar(fs, fieldValue, flagName, flagHelp); err != nil {
			return err
		}
	}
	return nil
}

// assignFlagToStructFieldVar creates a flag associated with the given struct field, if the type can be set from a flag.
func assignFlagToStructFieldVar(fs *flag.FlagSet, fieldValue reflect.Value, flagName, flagHelp string) error {

	// get the pointer for non-pointer fields
	if fieldValue.Kind() != reflect.Ptr {
		fieldValue = fieldValue.Addr()
	}

	switch v := fieldValue.Interface().(type) {
	case flag.Value:
		fs.Var(v, flagName, flagHelp)
	case *time.Duration:
		fs.DurationVar(v, flagName, *v, flagHelp)
	case *json.Number:
		fs.Var((*jsonNumberFlagValue)(v), flagName, flagHelp)
	case *bool:
		fs.BoolVar(v, flagName, *v, flagHelp)
	case *int:
		fs.IntVar(v, flagName, *v, flagHelp)
	case *int64:
		fs.Int64Var(v, flagName, *v, flagHelp)
	case *uint:
		fs.UintVar(v, flagName, *v, flagHelp)
	case *uint64:
		fs.Uint64Var(v, flagName, *v, flagHelp)
	case *float64:
		fs.Float64Var(v, flagName, *v, flagHelp)
	case *string:
		fs.StringVar(v, flagName, *v, flagHelp)
	default:
		return fmt.Errorf("unsupported type %T to assign flag to struct field %q", v, flagName)
	}

	return nil
}

// jsonNumberFlagValue implements flag.Value for json.Number
type jsonNumberFlagValue json.Number

func (d *jsonNumberFlagValue) Set(s string) error {
	*d = jsonNumberFlagValue(s)
	return nil
}

func (d *jsonNumberFlagValue) String() string {
	return (*json.Number)(d).String()
}
