package clistruct

import (
	"encoding/json"
	"fmt"
	"strings"
)

// stringSliceValue implements flag.Value for []string
type stringSliceValue []string

func (v *stringSliceValue) Set(s string) error {
	if v == nil {
		return fmt.Errorf("nil stringSliceValue reciever")
	}
	*v = strings.Split(s, ",")
	return nil
}

func (v *stringSliceValue) String() string {
	if v == nil {
		return ""
	}
	return strings.Join(*v, ",")
}

// jsonNumberFlagValue implements flag.Value for json.Number
type jsonNumberFlagValue json.Number

func (v *jsonNumberFlagValue) Set(s string) error {
	if v == nil {
		return fmt.Errorf("nil jsonNumberFlagValue reciever")
	}
	*v = jsonNumberFlagValue(s)
	return nil
}

func (v *jsonNumberFlagValue) String() string {
	if v == nil {
		return ""
	}
	return (*json.Number)(v).String()
}
