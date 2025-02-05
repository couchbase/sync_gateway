//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

// DatabaseError denotes an error that occurred during database startup
type DatabaseError struct {
	ErrMsg string            `json:"error_message"`
	Code   databaseErrorCode `json:"error_code"`
}

type databaseErrorCode uint8

const (
	DatabaseBucketConnectionError      databaseErrorCode = 0
	DatabaseInvalidDatastore           databaseErrorCode = 1
	DatabaseInitSyncInfoError          databaseErrorCode = 2
	DatabaseInitialisationIndexError   databaseErrorCode = 3
	DatabaseCreateDatabaseContextError databaseErrorCode = 4
	DatabaseSGRClusterError            databaseErrorCode = 5
	DatabaseCreateReplicationError     databaseErrorCode = 6
)

var DatabaseErrorString = []string{
	DatabaseBucketConnectionError:      "Error connecting to bucket",
	DatabaseInvalidDatastore:           "Collection(s) not available",
	DatabaseInitSyncInfoError:          "Error initialising sync info",
	DatabaseInitialisationIndexError:   "Error initialising database indexes",
	DatabaseCreateDatabaseContextError: "Error creating database context",
	DatabaseSGRClusterError:            "Error with fetching SGR cluster definition",
	DatabaseCreateReplicationError:     "Error creating replication during database init",
}

func NewDatabaseError(code databaseErrorCode) *DatabaseError {
	return &DatabaseError{
		ErrMsg: DatabaseErrorString[code],
		Code:   code,
	}
}
