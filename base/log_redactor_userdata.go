package base

import "fmt"

const (
	userDataPrefix = "<ud>"
	userDataSuffix = "</ud>"
)

// RedactUserData is a global toggle for UserData redaction.
var RedactUserData = true

// UserData implements the Redactor interface for logging purposes.
type UserData string

// Compile-time interface check.
var _ Redactor = UserData("")

// Redact tags the string with UserData tags for post-processing.
func (ud UserData) Redact() string {
	if !RedactUserData {
		return string(ud)
	}
	return userDataPrefix + string(ud) + userDataSuffix
}

// ToUD is a helper function for getting a UserData type.
func ToUD(i interface{}) UserData {
	switch v := i.(type) {
	case string:
		return UserData(v)
	case fmt.Stringer:
		return UserData(v.String())
	default:
		return UserData(fmt.Sprintf("%v", v))
	}
}
