/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
)

// GET /_rosmar/
func (h *handler) handleRosmarGet() error {
	if h.server.BootstrapContext == nil || h.server.BootstrapContext.Connection == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "This requires persistent config")
	}

	serverURL := h.server.Config.Bootstrap.Server
	if !base.ServerIsWalrus(serverURL) {
		return base.HTTPErrorf(http.StatusBadRequest, "This is not a rosmar bucket")
	}

	bucketNames, err := h.server.BootstrapContext.Connection.GetConfigBuckets(h.ctx())
	if err != nil {
		return err
	}
	result := make(map[string]base.CollectionNames)

	for _, bucketName := range bucketNames {
		bucket, err := rosmar.OpenBucketIn(serverURL, bucketName, rosmar.ReOpenExisting)
		if err != nil {
			return err
		}
		defer bucket.Close(h.ctx())

		dsNames, err := bucket.ListDataStores()
		if err != nil {
			return err
		}

		result[bucketName] = base.NewCollectionNames(dsNames...)
	}
	h.writeJSON(result)
	return nil
}

// DELETE /_rosmar/{bucketkeyspace}
func (h *handler) handleRosmarDelete() error {
	if h.server.BootstrapContext == nil || h.server.BootstrapContext.Connection == nil {
		return base.HTTPErrorf(http.StatusBadRequest, "This requires persistent config")
	}

	serverURL := h.server.Config.Bootstrap.Server
	if !base.ServerIsWalrus(serverURL) {
		return base.HTTPErrorf(http.StatusBadRequest, "This is not a rosmar bucket")
	}

	bucketkeyspace := h.PathVar("bucketkeyspace")
	parts := strings.Split(bucketkeyspace, ".")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid bucketkeyspace format. Expected formats are: 'bucket', 'bucket.scope', or 'bucket.scope.collection'")
		}
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid bucketkeyspace format. Expected formats are: 'bucket', 'bucket.scope', or 'bucket.scope.collection'")
		}
	case 3:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid bucketkeyspace format. Expected formats are: 'bucket', 'bucket.scope', or 'bucket.scope.collection'")
		}
	default:
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid bucketkeyspace format. Expected formats are: 'bucket', 'bucket.scope', or 'bucket.scope.collection'")
	}
	bucketName := parts[0]

	// Check if the bucket exists
	bucketNames, err := h.server.BootstrapContext.Connection.GetConfigBuckets(h.ctx())
	if err != nil {
		return err
	}

	existsInCluster := false
	for _, b := range bucketNames {
		if b == bucketName {
			existsInCluster = true
			break
		}
	}

	if !existsInCluster {
		return base.HTTPErrorf(http.StatusNotFound, "Bucket %s not found", bucketName)
	}

	bucket, err := rosmar.OpenBucketIn(serverURL, bucketName, rosmar.ReOpenExisting)
	if err != nil {
		return fmt.Errorf("could not open bucket: %w", err)
	}
	defer bucket.Close(h.ctx())

	switch len(parts) {
	case 1:
		// DELETE /_rosmar/bucketname
		return bucket.CloseAndDelete(h.ctx())
	case 2:
		// DELETE /_rosmar/bucketname.scopename
		scopeName := parts[1]

		dsNames, err := bucket.ListDataStores()
		if err != nil {
			return err
		}

		droppedAny := false
		for _, ds := range dsNames {
			if ds.ScopeName() == scopeName {
				droppedAny = true
				if err := bucket.DropDataStore(ds); err != nil {
					return err
				}
			}
		}

		if !droppedAny {
			return base.HTTPErrorf(http.StatusNotFound, "Scope %s not found", scopeName)
		}
		return nil
	case 3:
		// DELETE /_rosmar/bucketname.scopename.collectionname
		scopeName := parts[1]
		collectionName := parts[2]

		err = bucket.DropDataStore(base.NewScopeAndCollectionName(scopeName, collectionName))
		if err != nil {
			return err
		}
		return nil
	}
	return base.HTTPErrorf(http.StatusBadRequest, "Invalid bucketkeyspace format. Expected formats are: 'bucket', 'bucket.scope', or 'bucket.scope.collection'")
}
