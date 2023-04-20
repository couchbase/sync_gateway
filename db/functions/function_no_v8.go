// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !cb_sg_v8

package functions

import (
	"context"
	"fmt"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/js"
)

// Validates a FunctionsConfig & GraphQLConfig.
func ValidateFunctions(ctx context.Context, vm js.ServiceHost, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) error {
	if fnConfig != nil || gqConfig != nil {
		return fmt.Errorf("ValidateFunctions is not supported in non v8 build")
	}
	return nil
}

func (fnc *Config) Compile(vms *js.VMPool) (*db.UserFunctions, db.GraphQL, error) {
	return nil, nil, fmt.Errorf("functions.Config.Compile is not supported in non v8 build")
}

func (fnc *Config) N1QLQueryNames() []string {
	return nil
}
