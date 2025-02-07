//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

// DatabaseError denotes an error that occurred during database startup
type DatabaseError struct {
	ErrMsg string            `json:"error_message"`
	Code   databaseErrorCode `json:"error_code"`
}

var DatabaseErrorMap = map[databaseErrorCode]string{
	DatabaseBucketConnectionError:      "Error connecting to bucket",
	DatabaseInvalidDatastore:           "Collection(s) not available",
	DatabaseInitSyncInfoError:          "Error initialising sync info",
	DatabaseInitializationIndexError:   "Error initialising database indexes",
	DatabaseCreateDatabaseContextError: "Error creating database context",
	DatabaseSGRClusterError:            "Error with fetching SGR cluster definition",
	DatabaseCreateReplicationError:     "Error creating replication during database init",
	DatabaseOnlineProcessError:         "Error attempting to start online process",
}

type databaseErrorCode uint8

// Error codes exposed for each error a database can encounter on load. These codes are consumed by Capella so must remain stable.
const (
	DatabaseBucketConnectionError      databaseErrorCode = 1
	DatabaseInvalidDatastore           databaseErrorCode = 2
	DatabaseInitSyncInfoError          databaseErrorCode = 3
	DatabaseInitializationIndexError   databaseErrorCode = 4
	DatabaseCreateDatabaseContextError databaseErrorCode = 5
	DatabaseSGRClusterError            databaseErrorCode = 6
	DatabaseCreateReplicationError     databaseErrorCode = 7
	DatabaseOnlineProcessError         databaseErrorCode = 8
)

func NewDatabaseError(code databaseErrorCode) *DatabaseError {
	return &DatabaseError{
		ErrMsg: DatabaseErrorMap[code],
		Code:   code,
	}
}
