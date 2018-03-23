package base

import "fmt"

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

// Redact is a stub which will eventaully tag or redact data in logs.
func (md Metadata) Redact() string {
	// FIXME: Stub (#3370)
	return string(md)
}

// Compile-time interface check.
var _ Redactor = Metadata("")

// MD returns a Metadata type for any given value.
func MD(i interface{}) Metadata {
	switch v := i.(type) {
	case string:
		return Metadata(v)
	case fmt.Stringer:
		return Metadata(v.String())
	default:
		// Fall back to a slower but safe way of getting a string from any type.
		return Metadata(fmt.Sprintf("%+v", v))
	}
}
