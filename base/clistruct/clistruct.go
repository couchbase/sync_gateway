// Package clistruct provides the ability to assign command line flags to fields in structs that declare `cli` and `help` struct tags.
package clistruct

import (
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"time"
	"unsafe"
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
		panic("expected flag.FlagSet in RegisterFlags but got nil")
	}

	if err := register(fs, reflectStructValue(cliStruct), ""); err != nil {
		panic(err.Error())
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

		switch fieldType.Kind() {
		case reflect.Struct:
			// recurse for nested structs
			if err := register(fs, fieldValue, flagName); err != nil {
				return err
			}
			continue
		case reflect.Ptr, reflect.Interface:
			// dereference and see if we can set it below, but it's unlikely if the given struct has not had values assigned to it.
			fieldValue = fieldValue.Elem()
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

	switch v := reflectInterfaceValue(fieldValue).(type) {
	case flag.Value:
		fs.Var(v, flagName, flagHelp)
	case *time.Duration:
		fs.DurationVar(v, flagName, 0, flagHelp)
	case *json.Number:
		fs.Var((*jsonNumberFlagValue)(v), flagName, flagHelp)
	default:
		// See docs: unsafe.Pointer (5) Conversion of the result of ... reflect.Value.UnsafeAddr
		// uintptr -> unsafe.Pointer conversion MUST be done without an intermediate variable
		p := unsafe.Pointer(fieldValue.UnsafeAddr())

		// we can only map struct field types which have a matching flag var type
		switch fieldValue.Type().Kind() {
		case reflect.Bool:
			fs.BoolVar((*bool)(p), flagName, false, flagHelp)
		case reflect.Int:
			fs.IntVar((*int)(p), flagName, 0, flagHelp)
		case reflect.Int64:
			fs.Int64Var((*int64)(p), flagName, 0, flagHelp)
		case reflect.Uint:
			fs.UintVar((*uint)(p), flagName, 0, flagHelp)
		case reflect.Uint64:
			fs.Uint64Var((*uint64)(p), flagName, 0, flagHelp)
		case reflect.Float64:
			fs.Float64Var((*float64)(p), flagName, 0, flagHelp)
		case reflect.String:
			fs.StringVar((*string)(p), flagName, "", flagHelp)
		default:
			return fmt.Errorf("unsupported type %s to assign flag to struct field %q", fieldValue.Type().String(), flagName)
		}
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
