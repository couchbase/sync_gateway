package base

import (
	"fmt"
	"reflect"

	"github.com/couchbase/clog"
)

// RedactMetadata is a global toggle for system data redaction.
var RedactMetadata = false

// Metadata is a type which implements the Redactor interface for logging purposes of metadata.
//
// Metadata is logical data needed by Couchbase to store and process User data.
// - Cluster name
// - Bucket names
// - DDoc/view names
// - View code
// - Index names
// - Mapreduce Design Doc Name and Definition (IP)
// - XDCR Replication Stream Names
// - And other couchbase resource specific meta data
type Metadata string

func (md Metadata) String() string {
	return string(md)
}

// Redact tags the string with Metadata tags for post-processing.
func (md Metadata) Redact() string {
	if !RedactMetadata {
		return string(md)
	}
	return clog.Tag(clog.MetaData, string(md)).(string)
}

// Compile-time interface check.
var _ Redactor = Metadata("")

// MD returns a Metadata type for any given value.
func MD(i interface{}) RedactorFunc {
	switch v := i.(type) {
	case string:
		return func() Redactor {
			return Metadata(v)
		}
	case fmt.Stringer:
		return func() Redactor {
			return Metadata(v.String())

		}
	default:
		return func() Redactor {
			typeOf := reflect.ValueOf(i)
			if typeOf.Kind() == reflect.Slice {
				return buildRedactorFuncSlice(typeOf, MD)
			}
			// Fall back to a slower but safe way of getting a string from any type.
			return Metadata(fmt.Sprintf("%+v", v))
		}
	}
}
