// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"errors"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
)

var _ BootstrapConnection = &RosmarCluster{}

// RosmarCluster implements BootstrapConnection and is used for connecting to a rosmar cluster
type RosmarCluster struct {
	serverURL string
}

// NewRosmarCluster creates a from a given URL
func NewRosmarCluster(serverURL string) *RosmarCluster {
	return &RosmarCluster{
		serverURL: serverURL,
	}
}

// GetConfigBuckets returns all the buckets registered in rosmar.
func (c *RosmarCluster) GetConfigBuckets() ([]string, error) {
	return rosmar.GetBucketNames(), nil
}

// GetMetadataDocument returns a metadata document from the default collection for the specified bucket.
func (c *RosmarCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr interface{}) (cas uint64, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return 0, err
	}
	defer bucket.Close(ctx)

	return bucket.DefaultDataStore().Get(docID, valuePtr)
}

// InsertMetadataDocument inserts a metadata document, and fails if it already exists or returns a CasMismatchError
func (c *RosmarCluster) InsertMetadataDocument(ctx context.Context, location, key string, value interface{}) (newCAS uint64, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return 0, err
	}
	defer bucket.Close(ctx)

	return bucket.DefaultDataStore().WriteCas(key, 0, 0, 0, value, 0)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch
func (c *RosmarCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value interface{}) (newCAS uint64, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return 0, err
	}
	defer bucket.Close(ctx)

	return c.writeWithOptionalCas(bucket.DefaultDataStore(), docID, cas, value)
}

// TouchMetadataDocument sets the specified property in a bootstrap metadata document for a given bucket and key.  Used to
// trigger CAS update on the document, to block any racing updates. Does not retry on CAS failure.

func (c *RosmarCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return 0, err
	}
	defer bucket.Close(ctx)

	// FIXME to not touch the whole document?
	return bucket.DefaultDataStore().Touch(docID, 0)
}

// writeWithOptionalCas writes a document, optionally using a CAS value if provided.  If the document already exists, the CAS value must match the existing CAS value. This is a workaround for behavior of WriteCas and has race conditions on getting cas if the document is updated after Set and before Get when cas = 0.
func (c *RosmarCluster) writeWithOptionalCas(dataStore DataStore, docID string, cas uint64, value any) (uint64, error) {
	var unused any
	_, err := dataStore.Get(docID, &unused)
	if err != nil {
		return 0, err
	}
	newDoc := IsDocNotFoundError(err)
	if cas != 0 && !newDoc {
		return dataStore.WriteCas(docID, 0, 0, cas, value, 0)

	}
	err = dataStore.Set(docID, 0, nil, value)
	if err != nil {
		return 0, err
	}
	return dataStore.Get(docID, &unused)
}

// DeleteMetadataDocument deletes an existing bootstrap metadata document for a given bucket and key.
func (c *RosmarCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) error {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return err
	}
	defer bucket.Close(ctx)

	_, err = bucket.DefaultDataStore().Remove(key, cas)
	return err
}

// UpdateMetadataDocument retries on CAS mismatch
func (c *RosmarCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return 0, err
	}
	defer bucket.Close(ctx)
	for {
		var bucketValue []byte
		cas, err := bucket.DefaultDataStore().Get(docID, &bucketValue)
		if err != nil {
			return 0, err
		}
		newConfig, err := updateCallback(bucketValue, cas)
		if err != nil {
			return 0, err
		}
		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := bucket.DefaultDataStore().Remove(docID, cas)
			if err != nil {
				// retry on cas failure
				if errors.As(err, &sgbucket.CasMismatchErr{}) {
					continue
				}
				return 0, err
			}
			return removeCasOut, nil
		}

		replaceCfgCasOut, err := bucket.DefaultDataStore().WriteCas(docID, 0, 0, cas, newConfig, 0)
		if err != nil {
			if errors.As(err, &sgbucket.CasMismatchErr{}) {
				// retry on cas failure
				continue
			}
			return 0, err
		}

		return replaceCfgCasOut, nil
	}

}

// KeyExists checks whether a key exists in the default collection for the specified bucket
func (c *RosmarCluster) KeyExists(ctx context.Context, location, docID string) (exists bool, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, location, rosmar.CreateOrOpen)
	if err != nil {
		return false, err
	}
	defer bucket.Close(ctx)

	return bucket.DefaultDataStore().Exists(docID)
}

// GetDocument fetches a document from the default collection.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (c *RosmarCluster) GetDocument(ctx context.Context, bucketName, docID string, rv interface{}) (exists bool, err error) {
	bucket, err := rosmar.OpenBucket(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return false, err
	}
	defer bucket.Close(ctx)

	_, err = bucket.DefaultDataStore().Get(docID, rv)
	if IsDocNotFoundError(err) {
		return false, nil
	}
	return err != nil, err

}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (c *RosmarCluster) Close() {
}

func (c *RosmarCluster) SetConnectionStringServerless() error { return nil }

func (c *RosmarCluster) GetClusterN1QLStore(bucketName, scopeName, collectionName string) (*ClusterOnlyN1QLStore, error) {
	return nil, errors.New("rosmar doesn't support a N1QL store")
}
