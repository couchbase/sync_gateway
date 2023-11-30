// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"log"
	"reflect"

	"github.com/couchbase/sync_gateway/base"
)

func traverseAndRetrieveStats(logger *log.Logger, s any) StatDefinitions {
	stats := make(StatDefinitions)

	topLevel := reflect.ValueOf(s)
	if topLevel.IsNil() {
		logger.Println("Nil value for", topLevel.String())
		return nil
	}
	if topLevel.Kind() == reflect.Ptr {
		// Dereference the pointer so the value can be manipulated
		topLevel = topLevel.Elem()
	}

	for _, sf := range reflect.VisibleFields(topLevel.Type()) {
		if !sf.IsExported() {
			continue
		}

		// Get by name (rather than index) so anonymous fields don't cause a problem
		field := topLevel.FieldByName(sf.Name)

		// Check if stat is a wrapper and information can be extracted
		stat := field.Interface()
		statWrapper, ok := stat.(base.SgwStatWrapper)
		if ok {
			stats[statWrapper.Name()] = newStatDefinition(statWrapper)
			continue
		}

		// Check if it is a map that needs to be delved in to
		if field.Kind() == reflect.Map {
			values := field.MapRange()
			// Make sure map value does exist
			if !values.Next() {
				logger.Println("No stats found for", sf.Name)
				continue
			}
			// Only traverse the first value of the map to avoid duplicate stat definitions
			field = values.Value()
		}

		// Follow to struct down a level and append the stats to the map
		for k, v := range traverseAndRetrieveStats(logger, field.Interface()) {
			stats[k] = v
		}
	}

	return stats
}
