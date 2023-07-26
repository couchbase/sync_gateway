// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"github.com/couchbase/sync_gateway/base"
)

type StatDefinition struct {
	Name   string   `json:"name"`             // The fully qualified name of the stat
	Labels []string `json:"labels,omitempty"` // The labels that Prometheus uses to organise some of the stats such as database, collection, etc
	Help   string   `json:"help,omitempty"`   // A description of what the stat does
	Format string   `json:"format,omitempty"` // The format of the value such as int, float, duration
	Type   string   `json:"type,omitempty"`   // The prometheus.ValueType such as counter, gauge, etc
}

func newStatDefinition(stat base.SgwStatWrapper) StatDefinition {
	return StatDefinition{
		Name:   stat.Name(),
		Labels: stat.LabelKeys(),
		Help:   stat.Help(),
		Format: stat.FormatString(),
		Type:   stat.ValueTypeString(),
	}
}
