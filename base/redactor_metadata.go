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
type MetadataSlice []Metadata

// Redact tags the string with Metadata tags for post-processing.
func (md Metadata) Redact() string {
	if !RedactMetadata {
		return string(md)
	}
	return clog.Tag(clog.MetaData, string(md)).(string)
}

func mds(interfaceSlice interface{}) MetadataSlice {
	valueOf := reflect.ValueOf(interfaceSlice)

	length := valueOf.Len()
	retVal := make([]Metadata, 0, length)
	for i := 0; i < length; i++ {
		retVal = append(retVal, MD(valueOf.Index(i).Interface()).(Metadata))
	}
	return retVal
}

func (udSlice MetadataSlice) Redact() string {
	tmp := []byte{}
	for _, item := range udSlice {
		tmp = append(tmp, []byte(item.Redact())...)
		tmp = append(tmp, ' ')
	}
	return "[ " + string(tmp) + "]"
}

// Compile-time interface check.
var _ Redactor = Metadata("")

// MD returns a Metadata type for any given value.
func MD(i interface{}) Redactor {
	switch v := i.(type) {
	case string:
		return Metadata(v)
	case fmt.Stringer:
		return Metadata(v.String())
	default:
		if reflect.ValueOf(i).Kind() == reflect.Slice {
			return mds(i)
		}
		// Fall back to a slower but safe way of getting a string from any type.
		return Metadata(fmt.Sprintf("%+v", v))
	}
}
