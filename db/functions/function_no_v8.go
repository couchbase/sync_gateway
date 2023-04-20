package functions

import (
	"context"
	"fmt"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/js"
)

// -build cb_sg_v8

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
