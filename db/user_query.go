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
	"reflect"
	"regexp"
	"strings"

	gqltools "github.com/bhoriuchi/graphql-go-tools"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
)

// Name of built-in user-info query parameter
const userQueryUserParam = "user"

// Value of user-info query parameter
type userQueryUserInfo struct {
	name     string   `json:"name"`
	email    string   `json:"email"`
	channels []string `json:"channels"`
	roles    []string `json:"roles"`
}

// Regexp that matches a property pattern -- either `$xxx` or `$(xxx)` where `xxx` is one or more
// alphanumeric characters or underscore. It also matches `$$` so it can be subtituted with `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`\$(\w+|\([^)]+\)|\$)`)

// Runs a named N1QL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserQuery(name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	// Look up the query name in the server config:
	query, found := db.Options.UserQueries[name]
	if !found {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No such query '%s'", name)
	}

	// Make sure each specified parameter has a value in `params`:
	for _, paramName := range query.Parameters {
		if paramName == userQueryUserParam {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Query %q uses reserved parameter name '$user'", name)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid")
		}
		if _, found := params[paramName]; !found {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Parameter '%s' is missing", paramName)
		}
	}

	// Any extra parameters in `params` are illegal:
	if len(params) != len(query.Parameters) {
		for _, paramName := range query.Parameters {
			delete(params, paramName)
		}
		for badKey, _ := range params {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter '%s'", badKey)
		}
	}

	// Add `user` parameter, for query's use in filtering the output:
	params[userQueryUserParam] = &userQueryUserInfo{
		name:     db.user.Name(),
		email:    db.user.Email(),
		channels: db.user.Channels().AllKeys(),
		roles:    db.user.RoleNames().AllKeys(),
	}

	// Verify the user has access to at least one of the given channels.
	// Channel names in the config are parameter-expanded like the query string.
	authorized := false
	for channelPattern, _ := range query.Channels {
		if channel, err := expandChannelPattern(name, channelPattern, params); err != nil {
			return nil, err
		} else if db.user.CanSeeChannel(channel) {
			authorized = true
			break
		}
	}
	if !authorized {
		return nil, db.user.UnauthError("You do not have access to this query")
	}

	// Run the query:
	return db.N1QLQueryWithStats(db.Ctx, QueryTypeUserPrefix+name, query.Statement, params,
		base.RequestPlus, false)
}

// Expands patterns of the form `$param` or `$(param)` in `channelPattern`, looking up each such
// `param` in the `params` map and substituting its value. `$$` is replaced with `$`.
// It is an error if any `param` has no value, or if its value is not a string or integer.
func expandChannelPattern(queryName string, channelPattern string, params map[string]interface{}) (string, error) {
	if strings.IndexByte(channelPattern, '$') < 0 {
		return channelPattern, nil
	}
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(channelPattern, func(param string) string {
		param = param[1:]
		if param == "$" {
			return param
		}
		if param[0] == '(' {
			param = param[1 : len(param)-1]
		}
		// Look up the parameter:
		if value, found := params[param]; !found {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Query %q has invalid channel pattern %q", queryName, channelPattern)
			err = base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid")
			return ""
		} else if valueStr, ok := value.(string); ok {
			return valueStr
		} else if reflect.ValueOf(value).CanInt() || reflect.ValueOf(value).CanUint() {
			return fmt.Sprintf("%v", value)
		} else if param == userQueryUserParam {
			// Special case: for `$user`, get value of params["user"]["name"]
			return value.(*userQueryUserInfo).name
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Value of parameter '%s' must be a string or int", param)
			return ""
		}
	})
	return channel, err
}

//////////////////// GRAPHQL ///////////////////////////

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
			return getAirportsInCity, nil
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

func getAirportsInCity(params graphql.ResolveParams) (interface{}, error) {
	return graphQLListQuery(
		"SELECT ts.*,  meta().id FROM `travel-sample` AS ts WHERE type=\"airport\" and city=$city",
		params)
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

// Returns a resolver that gets the value of the `sourceKey` key in the source object,
// then uses that to look up a document of type `docType` whose `docKey` property matches it.
func resolveJoinProperty(sourceKey string, docType string, docKey string) (graphql.FieldResolveFn, error) {
	n1ql := fmt.Sprintf("SELECT ts.*,  meta().id FROM `travel-sample` AS ts WHERE type=%q and %s=$VALUE", docType, docKey)
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
