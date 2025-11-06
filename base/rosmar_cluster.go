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
	"fmt"
	"net/url"
	"os"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
)

var _ BootstrapConnection = &RosmarCluster{}

// RosmarCluster implements BootstrapConnection and is used for connecting to a rosmar cluster
type RosmarCluster struct {
	serverURL       string
	bucketDirectory string
}

// NewRosmarCluster creates a from a given URL
func NewRosmarCluster(serverURL string) (*RosmarCluster, error) {
	cluster := &RosmarCluster{
		serverURL: serverURL,
	}
	if serverURL != rosmar.InMemoryURL {
		u, err := url.Parse(serverURL)
		if err != nil {
			return nil, err
		}
		directory := u.Path
		err = os.MkdirAll(directory, 0750)
		if err != nil {
			return nil, fmt.Errorf("could not create or access directory to open rosmar cluster %q: %w", serverURL, err)
		}
		cluster.bucketDirectory = directory
	}
	return cluster, nil
}

// GetConfigBuckets returns all the buckets registered in rosmar.
func (c *RosmarCluster) GetConfigBuckets(ctx context.Context) ([]string, error) {
	// If the cluster is a serialized rosmar cluster, we need to open each bucket to add to rosmar.bucketRegistry.
	if c.bucketDirectory != "" {
		d, err := os.ReadDir(c.bucketDirectory)
		if err != nil {
			return nil, err
		}
		for _, bucketName := range d {
			bucket, err := c.openBucket(bucketName.Name())
			if err != nil {
				return nil, fmt.Errorf("could not open bucket %s from %s :%w", bucketName, c.serverURL, err)
			}
			defer bucket.Close(ctx)

		}
	}
	return rosmar.GetBucketNames(), nil
}

// openBucket opens a rosmar bucket with the given name.
func (c *RosmarCluster) openBucket(bucketName string) (*rosmar.Bucket, error) {
	// OpenBucketIn is required to open a bucket from a serialized rosmar implementation.
	return rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
}

// getDefaultDataStore returns the default datastore for the specified bucket. Returns a bucket close function and an
// error.
func (c *RosmarCluster) getDefaultDataStore(ctx context.Context, bucketName string) (sgbucket.DataStore, func(ctx context.Context), error) {
	bucket, err := rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return nil, nil, err
	}
	closeFn := func(ctx context.Context) { bucket.Close(ctx) }

	ds, err := bucket.NamedDataStore(DefaultScopeAndCollectionName())
	if err != nil {
		AssertfCtx(ctx, "Unexpected error getting default collection for bucket %q: %v", bucketName, err)
		closeFn(ctx)
		return nil, nil, err
	}
	return ds, closeFn, nil
}

// GetMetadataDocument returns a metadata document from the default collection for the specified bucket.
func (c *RosmarCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr any) (cas uint64, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	return ds.Get(docID, valuePtr)
}

// InsertMetadataDocument inserts a metadata document, and fails if it already exists.
func (c *RosmarCluster) InsertMetadataDocument(ctx context.Context, location, key string, value any) (newCAS uint64, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	return ds.WriteCas(key, 0, 0, value, 0)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch
func (c *RosmarCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value any) (newCAS uint64, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)
	return ds.WriteCas(docID, 0, cas, value, 0)
}

// TouchMetadataDocument sets the specified property in a bootstrap metadata document for a given bucket and key.  Used to
// trigger CAS update on the document, to block any racing updates. Does not retry on CAS failure.

func (c *RosmarCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	// FIXME to not touch the whole document?
	return ds.Touch(docID, 0)
}

// DeleteMetadataDocument deletes an existing bootstrap metadata document for a given bucket and key.
func (c *RosmarCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) error {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return err
	}
	defer closer(ctx)

	_, err = ds.Remove(key, cas)
	return err
}

// UpdateMetadataDocument updates a given document and retries on CAS mismatch.
func (c *RosmarCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)
	for {
		var bucketValue []byte
		cas, err := ds.Get(docID, &bucketValue)
		if err != nil {
			return 0, err
		}
		newConfig, err := updateCallback(bucketValue, cas)
		if err != nil {
			return 0, err
		}
		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := ds.Remove(docID, cas)
			if err != nil {
				// retry on cas failure
				if errors.As(err, &sgbucket.CasMismatchErr{}) {
					continue
				}
				return 0, err
			}
			return removeCasOut, nil
		}

		replaceCfgCasOut, err := ds.WriteCas(docID, 0, cas, newConfig, 0)
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
	ds, closer, err := c.getDefaultDataStore(ctx, location)
	if err != nil {
		return false, err
	}
	defer closer(ctx)

	return ds.Exists(docID)
}

// GetDocument fetches a document from the default collection.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (c *RosmarCluster) GetDocument(ctx context.Context, bucketName, docID string, rv any) (exists bool, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, bucketName)
	if err != nil {
		return false, err
	}
	defer closer(ctx)

	_, err = ds.Get(docID, rv)
	if IsDocNotFoundError(err) {
		return false, nil
	}
	return err != nil, err

}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (c *RosmarCluster) Close() {
}

func (c *RosmarCluster) SetConnectionStringServerless() error { return nil }
