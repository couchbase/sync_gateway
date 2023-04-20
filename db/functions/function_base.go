// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package functions

import "github.com/couchbase/sync_gateway/base"

// Combines functions & GraphQL configuration. Implements db.IFunctionsAndGraphQLConfig.
type Config struct {
	Functions *FunctionsConfig
	GraphQL   *GraphQLConfig
}

// Top level user-function config object: the map of names to queries.
type FunctionsConfig struct {
	Definitions      FunctionsDefs `json:"definitions"`                  // The function definitions
	MaxFunctionCount *int          `json:"max_function_count,omitempty"` // Maximum number of functions
	MaxCodeSize      *int          `json:"max_code_size,omitempty"`      // Maximum length (in bytes) of a function's code
	MaxRequestSize   *int          `json:"max_request_size,omitempty"`   // Maximum size of the JSON-encoded function arguments
}

type FunctionsDefs = map[string]*FunctionConfig

// Defines a JavaScript or N1QL function that a client can invoke by name.
// (Its name is the key in the FunctionsDefs.)
type FunctionConfig struct {
	Type     string   `json:"type"`
	Code     string   `json:"code"`               // Javascript function or N1QL 'SELECT'
	Args     []string `json:"args,omitempty"`     // Names of parameters/arguments
	Mutating bool     `json:"mutating,omitempty"` // Allowed to modify database?
	Allow    *Allow   `json:"allow,omitempty"`    // Permissions (admin-only if nil)
}

// Permissions for a function
type Allow struct {
	Channels []string `json:"channels,omitempty"` // Names of channel(s) that grant access
	Roles    []string `json:"roles,omitempty"`    // Names of role(s) that have access
	Users    base.Set `json:"users,omitempty"`    // Names of user(s) that have access
}

// Configuration for GraphQL.
type GraphQLConfig struct {
	Schema           *string         `json:"schema,omitempty"`             // Schema in SDL syntax
	SchemaFile       *string         `json:"schemaFile,omitempty"`         // Path of schema file
	Resolvers        GraphQLTypesMap `json:"resolvers"`                    // Defines query/mutation code
	Subgraph         bool            `json:"subgraph,omitempty"`           // Enable Apollo Subgraph support
	MaxSchemaSize    *int            `json:"max_schema_size,omitempty"`    // Maximum length (in bytes) of GraphQL schema
	MaxResolverCount *int            `json:"max_resolver_count,omitempty"` // Maximum number of GraphQL resolvers
	MaxCodeSize      *int            `json:"max_code_size,omitempty"`      // Maximum length (in bytes) of a function's code
	MaxRequestSize   *int            `json:"max_request_size,omitempty"`   // Maximum size of the encoded query & arguments
}

//////// CONFIGURATION TYPES:

// Maps GraphQL type names (incl. "Query") to their resolvers.
type GraphQLTypesMap map[string]GraphQLResolverConfig

// Maps GraphQL field names to the resolvers that implement them.
type GraphQLResolverConfig map[string]FunctionConfig
