//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"math/rand"
	"time"

	"github.com/couchbase/sync_gateway/rest"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// Simple Sync Gateway launcher tool.
func main() {
	rest.ServerMain()
}
