//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	pkgerrors "github.com/pkg/errors"
)

const (
	MaxConcurrentSingleOps = 1000 // Max 1000 concurrent single bucket ops
	MaxConcurrentBulkOps   = 35   // Max 35 concurrent bulk ops
	MaxConcurrentQueryOps  = 1000 // Max concurrent query ops
	MaxBulkBatchSize       = 100  // Maximum number of ops per bulk call

	// Causes the write op to block until the change has been replicated to numNodesReplicateTo many nodes.
	// In our case, we only want to block until it's durable on the node we're writing to, so this is set to 0.
	numNodesReplicateTo = uint(0)

	// Causes the write op to block until the change has been persisted (made durable -- written to disk) on
	// numNodesPersistTo.  In our case, we only want to block until it's durable on the node we're writing to,
	// so this is set to 1
	numNodesPersistTo = uint(1)

	// CRC-32 checksum represents the body hash of "Deleted" document.
	DeleteCrc32c = "0x00000000"
)

// If the error is a net/url.Error and the error message is:
//
//	net/http: request canceled while waiting for connection
//
// Then it means that the view request timed out, most likely due to the fact that it's a stale=false query and
// it's rebuilding the index.  In that case, it's desirable to return a more informative error than the
// underlying net/url.Error. See https://github.com/couchbase/sync_gateway/issues/2639
func isGoCBQueryTimeoutError(err error) bool {

	if err == nil {
		return false
	}

	// If it's not a *url.Error, then it's not a viewtimeout error
	netUrlError, ok := pkgerrors.Cause(err).(*url.Error)
	if !ok {
		return false
	}

	// If it's a *url.Error and contains the "request canceled" substring, then it's a viewtimeout error.
	return strings.Contains(netUrlError.Error(), "request canceled")

}

// putDDocForTombstones uses the provided client and endpoints to create a design doc with index_xattr_on_deleted_docs=true
func putDDocForTombstones(name string, payload []byte, capiEps []string, client *http.Client, username string, password string) error {

	// From gocb.Bucket.getViewEp() - pick view endpoint at random
	if len(capiEps) == 0 {
		return errors.New("No available view nodes.")
	}
	viewEp := capiEps[rand.Intn(len(capiEps))]

	// Based on implementation in gocb.BucketManager.UpsertDesignDocument
	uri := fmt.Sprintf("/_design/%s", name)
	body := bytes.NewReader(payload)

	// Build the HTTP request
	req, err := http.NewRequest("PUT", viewEp+uri, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer ensureBodyClosed(resp.Body)
	if resp.StatusCode != 201 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("Client error: %s", string(data))
	}

	return nil

}

// QueryBucketItemCount uses a request plus query to get the number of items in a bucket, as the REST API can be slow to update its value.
// Requires a primary index on the bucket.
func QueryBucketItemCount(n1qlStore N1QLStore) (itemCount int, err error) {
	statement := fmt.Sprintf("SELECT COUNT(1) AS count FROM %s", KeyspaceQueryToken)
	fmt.Println("HONK statement= ", statement)
	r, err := n1qlStore.Query(statement, nil, RequestPlus, true)
	if err != nil {
		return -1, err
	}
	var val struct {
		Count int `json:"count"`
	}
	err = r.One(&val)
	if err != nil {
		return -1, err
	}
	return val.Count, nil
}

func isMinimumVersion(major, minor, requiredMajor, requiredMinor uint64) bool {
	if major < requiredMajor {
		return false
	}

	if major == requiredMajor && minor < requiredMinor {
		return false
	}

	return true
}

func normalizeIntToUint(value interface{}) (uint, error) {
	switch typeValue := value.(type) {
	case int:
		return uint(typeValue), nil
	case uint64:
		return uint(typeValue), nil
	case string:
		i, err := strconv.Atoi(typeValue)
		return uint(i), err
	default:
		return uint(0), fmt.Errorf("Unable to convert %v (%T) -> uint.", value, value)
	}
}

func asBool(value interface{}) bool {

	switch typeValue := value.(type) {
	case string:
		parsedVal, err := strconv.ParseBool(typeValue)
		if err != nil {
			WarnfCtx(context.Background(), "asBool called with unknown value: %v.  defaulting to false", typeValue)
			return false
		}
		return parsedVal
	case bool:
		return typeValue
	default:
		WarnfCtx(context.Background(), "asBool called with unknown type: %T.  defaulting to false", typeValue)
		return false
	}

}

// AsLeakyBucket tries to return the given bucket as a LeakyBucket.
func AsLeakyBucket(bucket Bucket) (*LeakyBucket, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *LeakyBucket:
		return typedBucket, true
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsLeakyBucket(underlyingBucket)
}

func GoCBBucketMgmtEndpoints(bucket CouchbaseBucketStore) (url []string, err error) {
	return bucket.MgmtEps()
}

// Get one of the management endpoints.  It will be a string such as http://couchbase
func GoCBBucketMgmtEndpoint(bucket CouchbaseBucketStore) (url string, err error) {
	mgmtEps, err := bucket.MgmtEps()
	if err != nil {
		return "", err
	}
	bucketEp := mgmtEps[rand.Intn(len(mgmtEps))]
	return bucketEp, nil
}
