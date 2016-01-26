// Package main Sync Gateway REST API
//
// The REST API for Couchbase Sync Gateway.
//
//     Schemes: http, https
//     BasePath: /
//     Version: 1.2.0
//     License: Apache 2.0
//     Contact: mobile-couchbase@googlegroups.com
//
//     Consumes:
//     - application/json
//
//     Produces:
//     - application/json
//
// swagger:meta
package main

// VendorInfo information for the server information response.
type VendorInfo struct {
    Name string `json:"name"`
    Version float64 `json:"version"`
}

// ServerInfoResponse represents Sync Gateway server-wide metadata.
//
// swagger:response ServerInfoResponse
type ServerInfoResponse struct {
	// swagger:allOf
    VendorData VendorInfo `json:"vendor"`
    
	ServerMessage string `json:"couchdb"`
    
    LongVersion string `json:"version"`
    
    AdminPort bool `json:"ADMIN"`
}

// AllDatabasesResponse lists the names of all databases.
//
// swagger:response AllDatabasesResponse
type AllDatabasesResponse []string

// CompactDatabaseResponse returns a list of all revision ids compacted.
//
// swagger:response CompactDatabaseResponse
type CompactDatabaseResponse struct {
    // Count of revisions deleted.
    Revisions int `json:"revs"`
}


/* Need structs to identify typed parameters. */

// DatabaseName Sync Gateway database name
//
// This is used for operations that want the name of a Sync Gateway database
// swagger:parameters CompactDatabase
type DatabaseName struct {
	// The Name of the database
	//
	// in: path
	// required: true
	Name string `json:"db"`
}
