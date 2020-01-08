package base

import (
	"fmt"
	"reflect"

	"github.com/couchbase/clog"
)

// RedactUserData is a global toggle for user data redaction.
var RedactUserData = false

// UserData is a type which implements the Redactor interface for logging purposes of user data.
//
//  User data is data that is stored into Couchbase by the application user account:
//  - Key and value pairs in JSON documents, or the key exclusively
//  - Application/Admin usernames that identify the human person
//  - Query statements included in the log file collected by support that leak the document fields (Select floor_price from stock).
//  - Names and email addresses asked during product registration and alerting
//  - Usernames
//  - Document xattrs
type UserData string

func (ud UserData) String() string {
	return string(ud)
}

// Redact tags the string with UserData tags for post-processing.
func (ud UserData) Redact() string {
	if !RedactUserData {
		return string(ud)
	}
	return clog.Tag(clog.UserData, string(ud)).(string)
}

// Compile-time interface check.
var _ Redactor = UserData("")

// UD returns a UserData type for any given value.
func UD(i interface{}) RedactorFunc {
	switch v := i.(type) {
	case string:
		return func() Redactor {
			return UserData(v)
		}
	case fmt.Stringer:
		return func() Redactor {
			return UserData(v.String())
		}
	case []byte:
		return func() Redactor {
			return UserData(string(v))
		}
	default:
		return func() Redactor {
			valueOf := reflect.ValueOf(i)
			if valueOf.Kind() == reflect.Slice {
				return buildRedactorFuncSlice(valueOf, UD)
			}
			// Fall back to a slower but safe way of getting a string from any type.
			return UserData(fmt.Sprintf("%+v", v))
		}
	}
}
