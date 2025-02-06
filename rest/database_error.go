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

var DatabaseErrorMap = map[databaseErrorCode]string{
	DatabaseBucketConnectionError:      "Error connecting to bucket",
	DatabaseInvalidDatastore:           "Collection(s) not available",
	DatabaseInitSyncInfoError:          "Error initialising sync info",
	DatabaseInitialisationIndexError:   "Error initialising database indexes",
	DatabaseCreateDatabaseContextError: "Error creating database context",
	DatabaseSGRClusterError:            "Error with fetching SGR cluster definition",
	DatabaseCreateReplicationError:     "Error creating replication during database init",
}

type databaseErrorCode uint8

const (
	DatabaseBucketConnectionError databaseErrorCode = iota
	DatabaseInvalidDatastore
	DatabaseInitSyncInfoError
	DatabaseInitialisationIndexError
	DatabaseCreateDatabaseContextError
	DatabaseSGRClusterError
	DatabaseCreateReplicationError
)

func NewDatabaseError(code databaseErrorCode) *DatabaseError {
	return &DatabaseError{
		ErrMsg: DatabaseErrorMap[code],
		Code:   code,
	}
}
