/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"net/http"
	"os"

	gqltools "github.com/bhoriuchi/graphql-go-tools"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
)


type queryContextKey string

var dbKey = queryContextKey("db")
var mutAllowedKey = queryContextKey("mutAllowed")
var _graphQLSchema *graphql.Schema

// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserGraphQLQuery(query string, operationName string, variables map[string]interface{}, mutationAllowed bool) (*graphql.Result, error) {
	if db.Options.GraphQL == nil {
		return nil, base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	}
	logCtx := context.TODO()
	schema, err := db.getGraphQLSchema()
	if err != nil {
		base.WarnfCtx(logCtx, "GraphQL schema compilation error: %+v", err)
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Server GraphQL configuration error")
	}
	params := graphql.Params{
		Schema:         *schema,
		RequestString:  query,
		VariableValues: variables,
		OperationName:  operationName,
		Context:        context.WithValue(context.WithValue(db.Ctx, dbKey, db), mutAllowedKey, mutationAllowed),
	}
	r := graphql.Do(params)
	if len(r.Errors) > 0 {
		base.WarnfCtx(logCtx, "GraphQL query failed with error: %+v", r.Errors)
	}
	return r, nil
}

func (db *Database) getGraphQLSchema() (*graphql.Schema, error) {
	if _graphQLSchema == nil {
		// Get the schema source, from either `schema` or `schemaFile`:
		var schemaSource string
		if db.Options.GraphQL.Schema != nil {
			if db.Options.GraphQL.SchemaFile != nil {
				return nil, fmt.Errorf("Only one of `schema` and `schemaFile` may be used")
			}
			schemaSource = *db.Options.GraphQL.Schema
		} else {
			if db.Options.GraphQL.SchemaFile == nil {
				return nil, fmt.Errorf("Either `schema` or `schemaFile` must be defined")
			}
			src, err := os.ReadFile(*db.Options.GraphQL.SchemaFile)
			if err != nil {
				return nil, fmt.Errorf("Can't read graphql.schemaFile %s", *db.Options.GraphQL.SchemaFile)
			}
			schemaSource = string(src)
		}

		// Assemble the resolvers:
		resolvers := map[string]interface{}{}
		for name, resolver := range db.Options.GraphQL.Resolvers {
			fieldMap := map[string]*gqltools.FieldResolve{}
			for fieldName, jsCode := range resolver {
				if fn, err := db.compileResolver(name, fieldName, jsCode); err == nil {
					fieldMap[fieldName] = &gqltools.FieldResolve{Resolve: fn}
				} else {
					return nil, err
				}
			}
			resolvers[name] = &gqltools.ObjectResolver{Fields: fieldMap}
		}

		// Now generate the Schema object:
		schema, err := gqltools.MakeExecutableSchema(gqltools.ExecutableSchema{
			TypeDefs:  schemaSource,
			Resolvers: resolvers,
		})
		if err != nil {
			return nil, err
		}
		_graphQLSchema = &schema
	}
	return _graphQLSchema, nil
}

//////////////////// RESOLVERS ///////////////////////////

func (db *Database) compileResolver(resolverName string, fieldName string, jsCode string) (graphql.FieldResolveFn, error) {
	// TODO: This is where the hackiness lives. We ignore the JS code and just return a hardcoded
	// resolver function based on the name.
	//fmt.Printf("*** 'Compiling' GraphQL resolver %s.%s: %s\n", resolverName, fieldName, jsCode)
	switch resolverName {
	case "Query":
		switch fieldName {
		case "airline":
			return resolveByDocID("airline")
		case "airport":
			return resolveByDocID("airport")
		case "hotel":
			return resolveByDocID("hotel")
		case "route":
			return resolveByDocID("route")
		case "airports":
			return resolveToListByN1QL("SELECT ts.*,  meta().id FROM $_keyspace AS ts WHERE type=\"airport\" and city=$city")
		}

	case "Route":
		switch fieldName {
		case "airline":
			return resolveJoinProperty("airline", "airline", "iata")
		case "sourceairport":
			return resolveJoinProperty("sourceairport", "airport", "faa")
		case "destinationairport":
			return resolveJoinProperty("destinationairport", "airport", "faa")
		}

	case "Mutation":
		switch fieldName {
		case "saveAirport":
			return saveAirport, nil
		case "saveAirports":
			return saveAirports, nil
		}
	}
	return nil, fmt.Errorf("Unknown resolver %q field %q (not hardwired)", resolverName, fieldName)
}

func saveAirport(params graphql.ResolveParams) (interface{}, error) {
	return graphQLPutDoc("airport", "airport", params)
}

func saveAirports(params graphql.ResolveParams) (interface{}, error) {
	return graphQLPutDocList("airports", "airport", params)
}

//////////////////// GENERAL PURPOSE ///////////////////////////

// Returns a resolver function that will return a document whose docID is given by the GraphQL
// `id` parameter, and is of the given `docType`.
func resolveByDocID(docType string) (graphql.FieldResolveFn, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		return graphQLDocIDQuery(docType, params)
	}, nil
}

// Returns a resolver function that will run a N1QL query, parameterized by the GraphQL arguments,
// and return the results as a list/array.
func resolveToListByN1QL(n1qlQuery string) (graphql.FieldResolveFn, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		return graphQLListQuery(n1qlQuery, params)
	}, nil
}

// Returns a resolver function that gets the value of the `sourceKey` key in the source object,
// then uses that to look up a document of type `docType` whose `docKey` property matches it.
func resolveJoinProperty(sourceKey string, docType string, docKey string) (graphql.FieldResolveFn, error) {
	n1ql := fmt.Sprintf("SELECT db.*,  meta().id FROM $_keyspace AS db WHERE type=%q and `%s`=$VALUE", docType, docKey)
	return func(params graphql.ResolveParams) (interface{}, error) {
		value, found := params.Source.(Body)[sourceKey].(string)
		if !found {
			return nil, nil
		}
		n1qlParams := map[string]interface{}{
			"VALUE": value,
		}
		result, err := graphQLSingleDocQuery(n1ql, n1qlParams, params)
		if result == nil && err == nil {
			base.WarnfCtx(params.Context, "GraphQL resolveJoinProperty: Couldn't join %q to a %s with %q=%q", sourceKey, docType, docKey, value)
		}
		return result, err
	}, nil
}

// General purpose GraphQL query handler that gets a doc by docID (called "id").
// It checks that the "type" property equals docType.
func graphQLDocIDQuery(docType string, params graphql.ResolveParams) (interface{}, error) {
	db, ok := params.Context.Value(dbKey).(*Database)
	if !ok {
		panic("No db in context")
	}
	docID := params.Args["id"].(string) // graphql ensures "id" exists and is a string
	rev, err := db.GetRev(docID, "", false, nil)
	if err != nil {
		status, _ := base.ErrorAsHTTPStatus(err)
		if status == http.StatusNotFound {
			// Not-found is not an error; just return null.
			return nil, nil
		}
		return nil, err
	}
	body, err := rev.Body()
	if err != nil {
		return nil, err
	}
	if body["type"] != docType {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No doc with this ID and type")
	}
	body["id"] = docID
	return body, nil
}

// General purpose GraphQL query handler that returns a single object returned by a N1QL query.
// A separate `queryParams` is passed to the query.
func graphQLSingleDocQuery(queryN1QL string, queryParams map[string]interface{}, params graphql.ResolveParams) (interface{}, error) {
	db, ok := params.Context.Value(dbKey).(*Database)
	if !ok {
		panic("No db in context")
	}
	results, err := db.N1QLQueryWithStats(
		db.Ctx,
		QueryTypeUsers, //FIX: bogus
		queryN1QL,
		queryParams,
		base.RequestPlus,
		false)
	if err != nil {
		return nil, err
	}
	var row interface{}
	if !results.Next(&row) {
		row = nil
	}
	if err = results.Close(); err != nil {
		return nil, err
	}
	return row, nil
}

// General purpose GraphQL query handler that returns a list of objects based on a N1QL query.
// The GraphQL parameters are passed to the query as its parameters.
func graphQLListQuery(queryN1QL string, params graphql.ResolveParams) (interface{}, error) {
	db, ok := params.Context.Value(dbKey).(*Database)
	if !ok {
		panic("No db in context")
	}
	results, err := db.N1QLQueryWithStats(
		db.Ctx,
		QueryTypeUsers, //FIX: bogus
		queryN1QL,
		params.Args,
		base.RequestPlus,
		false)
	if err != nil {
		return nil, err
	}
	result := []interface{}{}
	var row interface{}
	for results.Next(&row) {
		result = append(result, row)
	}
	if err = results.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func graphQLPutDoc(argName string, docType string, params graphql.ResolveParams) (interface{}, error) {
	if body, ok := params.Args[argName].(map[string]interface{}); ok {
		return graphQLPutDocObject(body, docType, params)
	} else {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "%q arg is missing or wrong type", argName)
	}
}

func graphQLPutDocObject(body Body, docType string, params graphql.ResolveParams) (interface{}, error) {
	if !params.Context.Value(mutAllowedKey).(bool) {
		return nil, base.HTTPErrorf(http.StatusForbidden, "Can't mutate from a read-only request")
	}
	db, ok := params.Context.Value(dbKey).(*Database)
	if !ok {
		panic("No db in context")
	}
	body["type"] = docType
	docID := body["id"].(string)
	delete(body, "id")

	rev, err := db.GetRev(docID, "", false, []string{})
	if err == nil {
		curBody, err := rev.Body()
		if err != nil {
			return nil, err
		}
		if curBody["type"] != docType {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Doc exists but with different type %q", curBody["type"])
		}
		body["_rev"] = rev.RevID
	}

	_, _, err = db.Put(docID, body)
	if err != nil {
		return nil, err
	}

	return true, nil
}

func graphQLPutDocList(argName string, docType string, params graphql.ResolveParams) (interface{}, error) {
	if !params.Context.Value(mutAllowedKey).(bool) {
		return nil, base.HTTPErrorf(http.StatusForbidden, "Can't mutate from a read-only request")
	}
	objects, ok := params.Args[argName].([]interface{})
	if !ok {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "%q arg is missing or wrong type", argName)
	}
	result := []interface{}{}
	for i, object := range objects {
		object, ok := object.(map[string]interface{})
		if !ok {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "%q[%d] arg is wrong type", argName, i)
		}
		oneResult, err := graphQLPutDocObject(object, docType, params)
		if err != nil {
			return nil, err
		}
		result = append(result, oneResult)
	}

	return true, nil
}
