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
	"runtime"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
)

var _ BootstrapConnection = &RosmarCluster{}

// RosmarCluster implements BootstrapConnection and is used for connecting to a rosmar cluster
type RosmarCluster struct {
	serverURL                   string
	bucketDirectory             string
	useSystemMetadataCollection bool // When true, bootstrap metadata is stored in _system._mobile, with read-fallback to _default._default during migration
	// bucketBootstrapTargets caches the resolved bootstrap-doc target for each bucket.
	// Mirrors CouchbaseCluster.bucketBootstrapTargets — see that comment for the decision tree.
	bucketBootstrapTargets SyncMap[string, bucketBootstrapTarget]
	// bucketsBootstrapMigrationComplete records per-bucket bootstrap-migration completion.
	// Mirrors CouchbaseCluster.bucketsBootstrapMigrationComplete — see that comment.
	bucketsBootstrapMigrationComplete SyncMap[string, bool]
}

// NewRosmarCluster creates a from a given URL. useSystemMetadataCollection mirrors
// NewCouchbaseCluster: when true, bootstrap metadata is read/written against _system._mobile with
// transparent read-fallback to _default._default; when false, _default._default is used
// unconditionally.
func NewRosmarCluster(serverURL string, useSystemMetadataCollection bool) (*RosmarCluster, error) {
	cluster := &RosmarCluster{
		serverURL:                   serverURL,
		useSystemMetadataCollection: useSystemMetadataCollection,
	}
	if serverURL != rosmar.InMemoryURL {
		u, err := url.Parse(serverURL)
		if err != nil {
			return nil, err
		}
		directory := u.Path
		if runtime.GOOS == "windows" {
			directory = strings.TrimPrefix(directory, "/")
		}
		err = os.MkdirAll(directory, 0700)
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

// getDefaultDataStore returns the default datastore for the specified bucket. Returns a bucket close function and an error.
func (c *RosmarCluster) getDefaultDataStore(ctx context.Context, bucketName string) (*rosmar.Collection, func(ctx context.Context), error) {
	bucket, err := rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return nil, nil, err
	}
	closeFn := func(ctx context.Context) { bucket.Close(ctx) }

	ds, err := bucket.NamedDataStore(ctx, DefaultScopeAndCollectionName())
	if err != nil {
		AssertfCtx(ctx, "Unexpected error getting default collection for bucket %q: %v", bucketName, err)
		closeFn(ctx)
		return nil, nil, err
	}
	rosmarCollection, ok := ds.(*rosmar.Collection)
	if !ok {
		AssertfCtx(ctx, "Unexpected type for default collection for bucket %q: %T", bucketName, ds)
		closeFn(ctx)
		return nil, nil, fmt.Errorf("unexpected type for default collection for bucket %q: %T", bucketName, ds)
	}
	return rosmarCollection, closeFn, nil
}

// metadataDataStores opens the bucket and returns the primary metadata DataStore plus an optional
// fallback. The per-bucket target cache (populated by SetBucketBootstrapTargetHint or the lazy
// probe in this function) determines the choice; absent a cached entry the connection-wide flag
// drives the decision.
//
// Fallback semantics are symmetric so reads can self-heal across a stale cache:
//   - target=systemMobile: primary=_system._mobile, fallback=_default._default (legacy docs not
//     yet migrated by the bucket-level migration step). Skipped when SetMigrationComplete has
//     been called.
//   - target=default (or no opt-in indication yet): primary=_default._default,
//     fallback=_system._mobile. A successful fallback hit here indicates a peer has migrated
//     this bucket's bootstrap docs — the caller is expected to invoke noteBucketFallbackHit so
//     the cache is corrected before the next op.
//
// Both are returned as *rosmar.Collection so callers can use rosmar-specific subdoc primitives
// (e.g. TouchXattrWithCas).
func (c *RosmarCluster) metadataDataStores(ctx context.Context, bucketName string) (primary, fallback *rosmar.Collection, closer func(ctx context.Context), err error) {
	bucket, err := rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return nil, nil, nil, err
	}
	closer = func(ctx context.Context) { bucket.Close(ctx) }

	defaultDS, err := bucket.NamedDataStore(ctx, DefaultScopeAndCollectionName())
	if err != nil {
		AssertfCtx(ctx, "Unexpected error getting default collection for bucket %q: %v", bucketName, err)
		closer(ctx)
		return nil, nil, nil, err
	}
	defaultCol, ok := defaultDS.(*rosmar.Collection)
	if !ok {
		AssertfCtx(ctx, "Unexpected type for default collection for bucket %q: %T", bucketName, defaultDS)
		closer(ctx)
		return nil, nil, nil, fmt.Errorf("unexpected type for default collection for bucket %q: %T", bucketName, defaultDS)
	}
	systemDS, err := bucket.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())
	if err != nil {
		closer(ctx)
		return nil, nil, nil, fmt.Errorf("unable to access %s.%s on bucket %q: %w", SystemScope, SystemCollectionMobile, bucketName, err)
	}
	systemCol, ok := systemDS.(*rosmar.Collection)
	if !ok {
		AssertfCtx(ctx, "Unexpected type for system collection for bucket %q: %T", bucketName, systemDS)
		closer(ctx)
		return nil, nil, nil, fmt.Errorf("unexpected type for system collection for bucket %q: %T", bucketName, systemDS)
	}

	cached, hasCachedTarget := c.bucketBootstrapTargets.Load(bucketName)
	useSystemMobile := c.useSystemMetadataCollection
	if hasCachedTarget {
		useSystemMobile = cached == bucketTargetSystemMobile
	}
	if !useSystemMobile {
		// Reads/writes route to default first. When this bucket has any opt-in indication —
		// either cached as bucketTargetDefault (set by SetBucketBootstrapTargetHint after probing
		// a legacy registry) or the connection-wide flag — systemMobile is wired as a self-heal
		// fallback so a peer-migrated bucket doesn't return not-found from a stale-cached
		// perspective. A bucket with no cache entry and no connection-wide opt-in skips the
		// fallback entirely so non-opt-in clusters don't pay an extra _system._mobile probe per op.
		if hasCachedTarget || c.useSystemMetadataCollection {
			return defaultCol, systemCol, closer, nil
		}
		return defaultCol, nil, closer, nil
	}
	// Lazy-cache the target only when a registry is found in one of the collections — that's
	// the only case where we have authoritative information. With no registry yet the choice
	// stays uncached so a subsequent SetBucketBootstrapTargetHint(optIn=true) can still claim
	// _system._mobile.
	if _, alreadyCached := c.bucketBootstrapTargets.Load(bucketName); !alreadyCached {
		if target, found := c.probeRegistryLocation(ctx, systemCol, defaultCol); found {
			c.bucketBootstrapTargets.LoadOrStore(bucketName, target)
			// Registry in _system._mobile means this bucket has been bootstrap-migrated. Read its
			// status doc and, if bootstrap.state=complete, mark the bucket migration-complete so its
			// reads skip the _default._default fallback — covering cluster-level and per-DB opt-in
			// alike. Mirrors CouchbaseCluster.ensureBucketBootstrapTargetCached. Read from the
			// already-open systemCol handle rather than GetMetadataMigrationStatus, which would
			// re-open the bucket while we already hold a handle to it.
			if target == bucketTargetSystemMobile {
				status := &MetadataMigrationStatus{}
				if _, getErr := systemCol.Get(ctx, MetadataMigrationStatusDocID, status); getErr == nil && status.Bootstrap.State == MigrationStateComplete {
					c.SetMigrationComplete(bucketName)
				}
			}
		}
	}
	if c.bucketBootstrapMigrationComplete(bucketName) {
		return systemCol, nil, closer, nil
	}
	return systemCol, defaultCol, closer, nil
}

// noteBucketFallbackHit updates the per-bucket cache to _system._mobile after a successful read
// or existence check against the fallback collection, when that hit indicates the cache was stale
// (the cached target was _default._default — i.e. this node had not yet observed that a peer ran
// the bucket-level migration). A no-op when the cache already says _system._mobile, since the
// systemMobile→default fallback direction is the legitimate in-progress-migration legacy path.
func (c *RosmarCluster) noteBucketFallbackHit(bucketName string) {
	if cached, ok := c.bucketBootstrapTargets.Load(bucketName); ok && cached == bucketTargetSystemMobile {
		return
	}
	c.bucketBootstrapTargets.Store(bucketName, bucketTargetSystemMobile)
}

// SetBucketBootstrapTargetHint resolves and caches the bootstrap-doc target for a rosmar bucket.
// Mirrors CouchbaseCluster.SetBucketBootstrapTargetHint.
func (c *RosmarCluster) SetBucketBootstrapTargetHint(ctx context.Context, bucketName string, optInHint bool) error {
	if c == nil {
		return errors.New("nil RosmarCluster")
	}
	if _, cached := c.bucketBootstrapTargets.Load(bucketName); cached {
		return nil
	}
	bucket, err := rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return err
	}
	defer bucket.Close(ctx)

	defaultDS, err := bucket.NamedDataStore(ctx, DefaultScopeAndCollectionName())
	if err != nil {
		return err
	}
	defaultCol, ok := defaultDS.(*rosmar.Collection)
	if !ok {
		return fmt.Errorf("unexpected type for default collection on bucket %q: %T", bucketName, defaultDS)
	}
	systemDS, err := bucket.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())
	if err != nil {
		return err
	}
	systemCol, ok := systemDS.(*rosmar.Collection)
	if !ok {
		return fmt.Errorf("unexpected type for system collection on bucket %q: %T", bucketName, systemDS)
	}
	target, found := c.probeRegistryLocation(ctx, systemCol, defaultCol)
	if !found {
		if optInHint || c.useSystemMetadataCollection {
			target = bucketTargetSystemMobile
		} else {
			target = bucketTargetDefault
		}
	}
	c.bucketBootstrapTargets.LoadOrStore(bucketName, target)
	return nil
}

// probeRegistryLocation reports where _sync:registry already lives, or that it doesn't exist yet.
// Mirrors CouchbaseCluster.probeRegistryLocation.
func (c *RosmarCluster) probeRegistryLocation(ctx context.Context, systemCol, defaultCol *rosmar.Collection) (target bucketBootstrapTarget, found bool) {
	if exists, err := systemCol.Exists(ctx, SGRegistryKey); err == nil && exists {
		return bucketTargetSystemMobile, true
	}
	if exists, err := defaultCol.Exists(ctx, SGRegistryKey); err == nil && exists {
		return bucketTargetDefault, true
	}
	return 0, false
}

// shouldFallback returns true when a primary error indicates not-found and a fallback collection
// is configured (i.e. useSystemMetadataCollection is enabled and migration is not yet complete).
func (c *RosmarCluster) shouldFallback(err error, fallback *rosmar.Collection) bool {
	return fallback != nil && IsDocNotFoundError(err)
}

// SetMigrationComplete marks bootstrap-metadata migration as finished for the given bucket;
// subsequent bootstrap reads for that bucket stop falling back to _default._default. Per-bucket so
// completing one bucket never disables another's fallback. Safe to call concurrently with in-flight
// operations.
func (c *RosmarCluster) SetMigrationComplete(bucketName string) {
	c.bucketsBootstrapMigrationComplete.Store(bucketName, true)
}

// bucketBootstrapMigrationComplete reports whether bootstrap-metadata migration has been marked
// complete for the given bucket. Absence of an entry means not-complete, so reads keep the
// _default._default fallback.
func (c *RosmarCluster) bucketBootstrapMigrationComplete(bucketName string) bool {
	complete, _ := c.bucketsBootstrapMigrationComplete.Load(bucketName)
	return complete
}

// RefreshBucketBootstrapTarget reads the bucket's metadata-migration status doc; if bootstrap.state
// is complete it updates the per-bucket cache to _system._mobile via noteBucketFallbackHit and marks
// the bucket migration-complete so reads stop falling back to _default._default. ErrNotFound
// (no migration ever started here) and any other transient error are returned to the caller for
// telemetry but never escalate — this is best-effort cache convergence, not a correctness path.
func (c *RosmarCluster) RefreshBucketBootstrapTarget(ctx context.Context, bucket string) error {
	status, _, err := c.GetMetadataMigrationStatus(ctx, bucket)
	if err != nil {
		return err
	}
	if status.Bootstrap.State == MigrationStateComplete {
		c.noteBucketFallbackHit(bucket)
		c.SetMigrationComplete(bucket)
	}
	return nil
}

// loadConfigWithFallback resolves which of primary/fallback currently holds docID and returns its
// value+CAS along with the resolved datastore. Callers should pass the returned datastore back in
// subsequent writes so the CAS stays valid. Re-call on CAS mismatch to handle the doc being
// migrated between collections mid-operation. On a successful fallback hit the bucket's cached
// bootstrap target is updated (via noteBucketFallbackHit) so subsequent ops route correctly.
func (c *RosmarCluster) loadConfigWithFallback(ctx context.Context, bucketName string, primary, fallback *rosmar.Collection, docID string) (ds *rosmar.Collection, value []byte, cas uint64, err error) {
	cas, err = primary.Get(ctx, docID, &value)
	if c.shouldFallback(err, fallback) {
		cas, err = fallback.Get(ctx, docID, &value)
		if err == nil {
			c.noteBucketFallbackHit(bucketName)
			return fallback, value, cas, nil
		}
	}
	return primary, value, cas, err
}

// GetMetadataDocument returns a metadata document. When useSystemMetadataCollection is enabled,
// reads consult _system._mobile first and transparently fall back to _default._default for any
// pre-migration metadata. Returns the underlying rosmar not-found error when the doc is absent
// from both collections; callers should use IsDocNotFoundError to test for not-found.
func (c *RosmarCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr any) (cas uint64, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	cas, err = primary.Get(ctx, docID, valuePtr)
	if c.shouldFallback(err, fallback) {
		cas, err = fallback.Get(ctx, docID, valuePtr)
		if err == nil {
			c.noteBucketFallbackHit(location)
		}
	}
	return cas, err
}

// InsertMetadataDocument inserts a metadata document into the primary collection. When
// useSystemMetadataCollection is enabled and the doc already lives in the fallback collection,
// returns ErrAlreadyExists rather than creating a divergent primary copy.
func (c *RosmarCluster) InsertMetadataDocument(ctx context.Context, location, key string, value any) (newCAS uint64, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	// Surface a fallback existence-check error rather than silently writing to primary — a
	// swallowed error here could let a divergent legacy copy survive in fallback.
	if fallback != nil {
		exists, existsErr := fallback.Exists(ctx, key)
		if existsErr != nil {
			return 0, existsErr
		}
		if exists {
			c.noteBucketFallbackHit(location)
			return 0, ErrAlreadyExists
		}
	}
	return primary.WriteCas(ctx, key, 0, 0, value, 0)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch. When the supplied
// CAS came from a fallback read (i.e. the document still lives in _default._default), the write is
// replayed against the fallback collection so the CAS stays valid.
func (c *RosmarCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value any) (newCAS uint64, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	newCAS, err = primary.WriteCas(ctx, docID, 0, cas, value, 0)
	if c.shouldFallback(err, fallback) {
		newCAS, err = fallback.WriteCas(ctx, docID, 0, cas, value, 0)
		if err == nil {
			c.noteBucketFallbackHit(location)
		}
	}
	return newCAS, err
}

// TouchMetadataDocument sets the specified property in the _sync xattr of a bootstrap metadata
// document via subdoc operation with CAS check. Mirrors CouchbaseCluster's implementation. When
// useSystemMetadataCollection is enabled and the primary returns ErrNotFound, the operation is
// retried against the fallback collection so legacy metadata in _default._default can still be
// touched without migration.
func (c *RosmarCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	newCAS, err = primary.TouchXattrWithCas(ctx, docID, cfgXattrKey, property, value, cas)
	if c.shouldFallback(err, fallback) {
		newCAS, err = fallback.TouchXattrWithCas(ctx, docID, cfgXattrKey, property, value, cas)
		if err == nil {
			c.noteBucketFallbackHit(location)
		}
	}
	return newCAS, err
}

// DeleteMetadataDocument deletes an existing bootstrap metadata document.
func (c *RosmarCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) error {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return err
	}
	defer closer(ctx)

	_, removeErr := primary.Remove(ctx, key, cas)
	if c.shouldFallback(removeErr, fallback) {
		_, removeErr = fallback.Remove(ctx, key, cas)
		if removeErr == nil {
			c.noteBucketFallbackHit(location)
		}
	}
	return removeErr
}

// UpdateMetadataDocument updates a given document and retries on CAS mismatch. The owning
// datastore (primary or fallback) is re-resolved on every CAS retry so a doc that migrates between
// collections concurrently with this Update is picked up on the next iteration rather than vanishing.
func (c *RosmarCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	ds, bucketValue, cas, err := c.loadConfigWithFallback(ctx, location, primary, fallback, docID)
	if err != nil {
		return 0, err
	}

	for {
		newConfig, err := updateCallback(bucketValue, cas)
		if err != nil {
			return 0, err
		}
		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := ds.Remove(ctx, docID, cas)
			if err != nil {
				if errors.As(err, &sgbucket.CasMismatchErr{}) {
					ds, bucketValue, cas, err = c.loadConfigWithFallback(ctx, location, primary, fallback, docID)
					if err != nil {
						return 0, err
					}
					continue
				}
				return 0, err
			}
			return removeCasOut, nil
		}

		replaceCfgCasOut, err := ds.WriteCas(ctx, docID, 0, cas, newConfig, 0)
		if err != nil {
			if errors.As(err, &sgbucket.CasMismatchErr{}) {
				ds, bucketValue, cas, err = c.loadConfigWithFallback(ctx, location, primary, fallback, docID)
				if err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}

		return replaceCfgCasOut, nil
	}

}

// KeyExists checks whether a key exists for the specified bucket. When useSystemMetadataCollection
// is enabled, checks primary then fallback, mirroring CouchbaseCluster.KeyExists.
func (c *RosmarCluster) KeyExists(ctx context.Context, location, docID string) (exists bool, err error) {
	primary, fallback, closer, err := c.metadataDataStores(ctx, location)
	if err != nil {
		return false, err
	}
	defer closer(ctx)

	exists, err = primary.Exists(ctx, docID)
	if err != nil || exists || fallback == nil {
		return exists, err
	}
	exists, err = fallback.Exists(ctx, docID)
	if err == nil && exists {
		c.noteBucketFallbackHit(location)
	}
	return exists, err
}

// GetDocument fetches a document from the default collection.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (c *RosmarCluster) GetDocument(ctx context.Context, bucketName, docID string, rv any) (exists bool, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, bucketName)
	if err != nil {
		return false, err
	}
	defer closer(ctx)

	_, err = ds.Get(ctx, docID, rv)
	if IsDocNotFoundError(err) {
		return false, nil
	}
	return err == nil, err
}

// GetRawDocument fetches a document from the default collection as raw bytes.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (c *RosmarCluster) GetRawDocument(ctx context.Context, bucket, docID string) (value []byte, exists bool, err error) {
	ds, closer, err := c.getDefaultDataStore(ctx, bucket)
	if err != nil {
		return nil, false, err
	}
	defer closer(ctx)

	value, _, err = ds.GetRaw(ctx, docID)
	if IsDocNotFoundError(err) {
		return nil, false, nil
	}
	return value, err == nil, err
}

// systemMobileDataStore opens the bucket and returns the _system._mobile DataStore plus a closer.
// Used by the metadata-migration status doc path, which always targets _system._mobile directly
// (independent of useSystemMetadataCollection / migrationComplete).
func (c *RosmarCluster) systemMobileDataStore(ctx context.Context, bucketName string) (*rosmar.Collection, func(ctx context.Context), error) {
	bucket, err := rosmar.OpenBucketIn(c.serverURL, bucketName, rosmar.CreateOrOpen)
	if err != nil {
		return nil, nil, err
	}
	closeFn := func(ctx context.Context) { bucket.Close(ctx) }
	ds, err := bucket.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())
	if err != nil {
		closeFn(ctx)
		return nil, nil, fmt.Errorf("unable to access %s.%s on bucket %q: %w", SystemScope, SystemCollectionMobile, bucketName, err)
	}
	col, ok := ds.(*rosmar.Collection)
	if !ok {
		closeFn(ctx)
		return nil, nil, fmt.Errorf("unexpected type for system collection for bucket %q: %T", bucketName, ds)
	}
	return col, closeFn, nil
}

// GetMetadataMigrationStatus reads the status doc directly from _system._mobile.
func (c *RosmarCluster) GetMetadataMigrationStatus(ctx context.Context, bucket string) (*MetadataMigrationStatus, uint64, error) {
	col, closer, err := c.systemMobileDataStore(ctx, bucket)
	if err != nil {
		return nil, 0, err
	}
	defer closer(ctx)

	status := &MetadataMigrationStatus{}
	cas, err := col.Get(ctx, MetadataMigrationStatusDocID, status)
	if err != nil {
		if IsDocNotFoundError(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	return status, cas, nil
}

// InsertMetadataMigrationStatus stamps the initial status doc. Returns ErrAlreadyExists if another
// node won the race.
func (c *RosmarCluster) InsertMetadataMigrationStatus(ctx context.Context, bucket string, status *MetadataMigrationStatus) (uint64, error) {
	if status == nil {
		return 0, errors.New("nil MetadataMigrationStatus")
	}
	col, closer, err := c.systemMobileDataStore(ctx, bucket)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	cas, err := col.WriteCas(ctx, MetadataMigrationStatusDocID, 0, 0, status, 0)
	if err != nil {
		if errors.Is(err, sgbucket.ErrKeyExists) {
			return 0, ErrAlreadyExists
		}
		return 0, err
	}
	return cas, nil
}

// UpdateMetadataMigrationStatus runs the callback inside a CAS-safe loop, seeding a fresh status
// doc when none exists yet.
func (c *RosmarCluster) UpdateMetadataMigrationStatus(ctx context.Context, bucket string, callback func(*MetadataMigrationStatus) error) (uint64, error) {
	col, closer, err := c.systemMobileDataStore(ctx, bucket)
	if err != nil {
		return 0, err
	}
	defer closer(ctx)

	for {
		var (
			status = &MetadataMigrationStatus{}
		)
		cas, getErr := col.Get(ctx, MetadataMigrationStatusDocID, status)
		if getErr != nil {
			if !IsDocNotFoundError(getErr) {
				return 0, getErr
			}
			status = NewMetadataMigrationStatus()
		}
		if err := callback(status); err != nil {
			return 0, err
		}
		newCAS, err := col.WriteCas(ctx, MetadataMigrationStatusDocID, 0, cas, status, 0)
		if err != nil {
			if errors.As(err, &sgbucket.CasMismatchErr{}) {
				continue
			}
			if errors.Is(err, sgbucket.ErrKeyExists) {
				// peer SG inserted between our miss and our write — retry against the real doc
				continue
			}
			return 0, err
		}
		return newCAS, nil
	}
}

// MigrateBootstrapDocs copies the supplied keys from _default._default into _system._mobile and
// removes the fallback copies. Idempotent; CAS-mismatch on the fallback delete is logged and the
// caller is expected to retry.
func (c *RosmarCluster) MigrateBootstrapDocs(ctx context.Context, bucket string, docIDs []string) error {
	bucketHandle, err := rosmar.OpenBucketIn(c.serverURL, bucket, rosmar.CreateOrOpen)
	if err != nil {
		return err
	}
	defer bucketHandle.Close(ctx)

	defaultDS, err := bucketHandle.NamedDataStore(ctx, DefaultScopeAndCollectionName())
	if err != nil {
		return err
	}
	defaultCol, ok := defaultDS.(*rosmar.Collection)
	if !ok {
		return fmt.Errorf("unexpected type for default collection on bucket %q: %T", bucket, defaultDS)
	}
	systemDS, err := bucketHandle.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())
	if err != nil {
		return err
	}
	systemCol, ok := systemDS.(*rosmar.Collection)
	if !ok {
		return fmt.Errorf("unexpected type for system collection on bucket %q: %T", bucket, systemDS)
	}

	for _, docID := range docIDs {
		var body []byte
		fallbackCas, err := defaultCol.Get(ctx, docID, &body)
		if err != nil {
			if IsDocNotFoundError(err) {
				continue
			}
			return fmt.Errorf("read fallback %q during bootstrap migration: %w", docID, err)
		}
		_, insertErr := systemCol.WriteCas(ctx, docID, 0, 0, body, 0)
		if insertErr != nil && !errors.Is(insertErr, sgbucket.ErrKeyExists) {
			return fmt.Errorf("insert primary %q during bootstrap migration: %w", docID, insertErr)
		}
		if _, err := defaultCol.Remove(ctx, docID, fallbackCas); err != nil {
			if errors.As(err, &sgbucket.CasMismatchErr{}) {
				InfofCtx(ctx, KeyConfig, "Bootstrap migration: skipping fallback delete for %q on bucket %q (CAS mismatch — concurrent write), caller should re-run", MD(docID), MD(bucket))
				continue
			}
			if IsDocNotFoundError(err) {
				continue
			}
			return fmt.Errorf("delete fallback %q during bootstrap migration: %w", docID, err)
		}
	}
	// The bootstrap docs now live in _system._mobile on this bucket; any prior cache entry that
	// pointed at _default._default is stale and would route subsequent reads/writes to a deleted
	// location. Store (not LoadOrStore) so a pre-migration bucketTargetDefault is overwritten.
	c.bucketBootstrapTargets.Store(bucket, bucketTargetSystemMobile)
	return nil
}

// CachedBootstrapTargets returns the cached bootstrap doc target for each bucket, for observability purposes. Values are "system_mobile", "default".
// The snapshot is not guaranteed to be consistent across concurrent updates.
func (c *RosmarCluster) CachedBootstrapTargets() map[string]string {
	targets := make(map[string]string)
	for key, value := range c.bucketBootstrapTargets.Range {
		if s := value.String(); s != "" {
			targets[key] = s
		} else {
			targets[key] = "unknown"
		}
	}
	return targets
}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (c *RosmarCluster) Close() {
}

func (c *RosmarCluster) SetConnectionStringServerless() error { return nil }

// AsRosmarBucket returns a bucket as a rosmar.Bucket, or an error if it is not one.
func AsRosmarBucket(bucket Bucket) (*rosmar.Bucket, error) {
	baseBucket := GetBaseBucket(bucket)
	if b, ok := baseBucket.(*rosmar.Bucket); ok {
		return b, nil
	}
	return nil, fmt.Errorf("bucket is not a rosmar bucket (type %T)", baseBucket)
}
