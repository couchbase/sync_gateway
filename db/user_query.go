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
	"reflect"
	"regexp"
	"strings"

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

type queryContextKey string

var dbKey = queryContextKey("db")

// Runs a GraphQL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserGraphQLQuery(request string) (*graphql.Result, error) {
	params := graphql.Params{
		Schema:        graphQLSchema(),
		RequestString: request,
		Context:       context.WithValue(db.Ctx, dbKey, db),
	}
	r := graphql.Do(params)
	if len(r.Errors) > 0 {
		logCtx := context.TODO()
		base.WarnfCtx(logCtx, "GraphQL query failed with error: %+v", r.Errors)
	}
	return r, nil
}

var _graphQLSchema *graphql.Schema

func graphQLSchema() graphql.Schema {
	if _graphQLSchema == nil {
		geoType := graphql.NewObject(
			graphql.ObjectConfig{
				Name: "Geo",
				Fields: graphql.Fields{
					"lat": &graphql.Field{Type: graphql.NewNonNull(graphql.Float)},
					"lon": &graphql.Field{Type: graphql.NewNonNull(graphql.Float)},
					"alt": &graphql.Field{Type: graphql.NewNonNull(graphql.Float)},
				},
			},
		)
		airportType := graphql.NewObject(
			graphql.ObjectConfig{
				Name: "Airport",
				Fields: graphql.Fields{
					"id":          &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
					"airportname": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
					"icao":        &graphql.Field{Type: graphql.String},
					"country":     &graphql.Field{Type: graphql.String},
					"city":        &graphql.Field{Type: graphql.String},
					"faa":         &graphql.Field{Type: graphql.String},
					"tz":          &graphql.Field{Type: graphql.String},
					"geo":         &graphql.Field{Type: geoType},
				},
			},
		)

		rootQuery := graphql.ObjectConfig{
			Name: "RootQuery",
			Fields: graphql.Fields{
				"getAirport": &graphql.Field{
					Type:        airportType,
					Description: "Get single airport by id",
					Args: graphql.FieldConfigArgument{
						"id": &graphql.ArgumentConfig{Type: graphql.ID},
					},
					Resolve: getAirportByID,
				},
				"airportsByCity": &graphql.Field{
					Type:        graphql.NewList(airportType),
					Description: "Get all airports in a city",
					Args: graphql.FieldConfigArgument{
						"city": &graphql.ArgumentConfig{Type: graphql.String},
					},
					Resolve: getAirportsInCity,
				},
			},
		}

		rootMutation := graphql.ObjectConfig{
			Name: "RootMutation",
			Fields: graphql.Fields{
				"saveAirport": &graphql.Field{
					Type:        airportType,
					Description: "Create new Airport",
					Args: graphql.FieldConfigArgument{
						// "airport": &graphql.ArgumentConfig{Type: airportType},
						"id": 		   &graphql.ArgumentConfig{Type: graphql.String},
						"airportname": &graphql.ArgumentConfig{Type: graphql.String},
						"icao":        &graphql.ArgumentConfig{Type: graphql.String},
						"country":     &graphql.ArgumentConfig{Type: graphql.String},
						"city":        &graphql.ArgumentConfig{Type: graphql.String},
						"faa":         &graphql.ArgumentConfig{Type: graphql.String},
						"tz":          &graphql.ArgumentConfig{Type: graphql.String},
						"geo":         &graphql.ArgumentConfig{Type: geoType},
					},
					Resolve: saveAirport,
				},
				"saveAirports": &graphql.Field{
					Type:        graphql.NewList(airportType),
					Description: "Create multiple Airports",
					Args: graphql.FieldConfigArgument{
						"airports": &graphql.ArgumentConfig{Type: graphql.NewList(airportType)},
					},
					Resolve: saveAirports,
				},
			},
		}

		schemaConfig := graphql.SchemaConfig{
			Query:    graphql.NewObject(rootQuery),
			Mutation: graphql.NewObject(rootMutation),
		}
		schema, err := graphql.NewSchema(schemaConfig)
		if err != nil {
			panic(fmt.Sprintf("Failed to create GraphQL schema: %v", err)) // TODO
		}
		_graphQLSchema = &schema
	}
	return *_graphQLSchema
}

func getAirportByID(params graphql.ResolveParams) (interface{}, error) {
	return graphQLDocIDQuery("airport", params)
}

func getAirportsInCity(params graphql.ResolveParams) (interface{}, error) {
	return graphQLListQuery(
		"SELECT ts.*,  meta().id FROM `travel-sample` AS ts WHERE type=\"airport\" and city=$city",
		params)
}

func saveAirport(params graphql.ResolveParams) (interface{}, error) {
	return graphQLPutDoc("airport", params)
}

func saveAirports(params graphql.ResolveParams) (interface{}, error) {
	return nil, fmt.Errorf("Unimplemented")
}

//////////////////// GENERAL PURPOSE ///////////////////////////

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

// General purpose GraphQL query handler that returns a list of objects.
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
	return result, nil
}

func graphQLPutDoc(docType string, params graphql.ResolveParams) (interface{}, error) {
	db, ok := params.Context.Value(dbKey).(*Database)
	if !ok {
		panic("No db in context")
	}
	body := params.Args
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

	// Return the body in its original form:
	body["id"] = docID
	delete(body, "type")
	delete(body, "_rev")
	return body, nil
}

