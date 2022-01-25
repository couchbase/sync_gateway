/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"reflect"
)

const (
	metaDataPrefix = "<md>"
	metaDataSuffix = "</md>"
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
	return metaDataPrefix + string(md) + metaDataSuffix
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
	case Set:
		return func() Redactor {
			return v.buildRedactorSet(MD)
		}
	case fmt.Stringer:
		return func() Redactor {
			return Metadata(v.String())

		}
	case []byte:
		return func() Redactor {
			return Metadata(string(v))
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
