// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"dario.cat/mergo"
	"github.com/couchbase/gocb/v2"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
// Manages retrieval of set of buckets, and generic interaction with bootstrap metadata documents from those buckets.
type BootstrapConnection interface {
	// GetConfigBuckets returns a list of bucket names where a bootstrap metadata documents could reside.
	GetConfigBuckets(context.Context) ([]string, error)
	// GetMetadataDocument fetches a bootstrap metadata document for a given bucket and key, along with the CAS of the config document.
	GetMetadataDocument(ctx context.Context, bucket, key string, valuePtr any) (cas uint64, err error)
	// InsertMetadataDocument saves a new bootstrap metadata document for a given bucket and key.
	InsertMetadataDocument(ctx context.Context, bucket, key string, value any) (newCAS uint64, err error)
	// DeleteMetadataDocument deletes an existing bootstrap metadata document for a given bucket and key.
	DeleteMetadataDocument(ctx context.Context, bucket, key string, cas uint64) (err error)
	// UpdateMetadataDocument updates an existing bootstrap metadata document for a given bucket and key. updateCallback can return nil to remove the config.  Retries on CAS failure.
	UpdateMetadataDocument(ctx context.Context, bucket, key string, updateCallback func(rawBucketConfig []byte, rawBucketConfigCas uint64) (updatedConfig []byte, err error)) (newCAS uint64, err error)
	// WriteMetadataDocument writes a bootstrap metadata document for a given bucket and key.  Does not retry on CAS failure.
	WriteMetadataDocument(ctx context.Context, bucket, key string, cas uint64, valuePtr any) (casOut uint64, err error)
	// TouchMetadataDocument sets the specified property in a bootstrap metadata document for a given bucket and key.  Used to
	// trigger CAS update on the document, to block any racing updates. Does not retry on CAS failure.
	TouchMetadataDocument(ctx context.Context, bucket, key string, property string, value string, cas uint64) (casOut uint64, err error)
	// KeyExists checks whether the specified key exists in the bucket's default collection
	KeyExists(ctx context.Context, bucket, key string) (exists bool, err error)
	// GetDocument retrieves the document with the specified key from the bucket's default collection.
	// Returns exists=false if key is not found, returns error for any other error.
	GetDocument(ctx context.Context, bucket, docID string, rv any) (exists bool, err error)
	// GetRawDocument retrieves the document with the specified key from the bucket's default collection as raw bytes.
	// Returns exists=false if key is not found, returns error for any other error.
	GetRawDocument(ctx context.Context, bucket, docID string) (value []byte, exists bool, err error)
	// SetMigrationComplete signals that bootstrap-metadata migration to _system._mobile has finished;
	// subsequent reads stop falling back to _default._default. No-op when useSystemMetadataCollection is false.
	SetMigrationComplete()
	// GetMetadataMigrationStatus reads the bucket-level metadata-migration status doc directly from
	// _system._mobile (never via the dual-collection wrapper). Returns ErrNotFound when the doc has
	// not yet been stamped on this bucket.
	GetMetadataMigrationStatus(ctx context.Context, bucket string) (status *MetadataMigrationStatus, cas uint64, err error)
	// InsertMetadataMigrationStatus stamps the initial status doc in _system._mobile. Returns
	// ErrAlreadyExists if another SG node won the race.
	InsertMetadataMigrationStatus(ctx context.Context, bucket string, status *MetadataMigrationStatus) (cas uint64, err error)
	// UpdateMetadataMigrationStatus runs the callback inside a CAS-safe read-modify-write loop
	// against _system._mobile. The callback receives the current status (or a fresh seed if absent)
	// and may mutate it in place. Returns the new CAS once the update lands.
	UpdateMetadataMigrationStatus(ctx context.Context, bucket string, callback func(*MetadataMigrationStatus) error) (newCAS uint64, err error)
	// MigrateBootstrapDocs copies the supplied document keys from _default._default into
	// _system._mobile. Caller assembles the key list (registry, enumerated dbconfig keys, well-known
	// cbgt cfg keys). Idempotent: already-present primary docs are skipped; fallback-resident docs
	// are copied and then CAS-deleted from the fallback. Intended for the bucket-level migration
	// step that fires once every per-DB migration reports complete.
	MigrateBootstrapDocs(ctx context.Context, bucket string, docIDs []string) error
	// RefreshBucketBootstrapTarget reads the bucket's metadata-migration status doc and, if the
	// bootstrap phase has completed (i.e. a peer node moved this bucket's bootstrap docs into
	// _system._mobile), updates the local per-bucket cache to point at _system._mobile so subsequent
	// reads/writes route there directly without paying the self-heal fallback cost. No-op when the
	// status doc is absent or bootstrap.state is anything other than complete. Intended to be called
	// periodically from the config-polling loop so peer nodes converge on the post-migration cache
	// state proactively rather than only on a failed read.
	RefreshBucketBootstrapTarget(ctx context.Context, bucket string) error
	// SetBucketBootstrapTargetHint resolves and caches where bootstrap docs land for a given bucket.
	// The decision: (1) registry already in _system._mobile → that collection; (2) registry in
	// _default._default → that collection (legacy registry stays put); (3) no registry anywhere →
	// _system._mobile if optInHint or the cluster-wide flag is on, otherwise _default._default.
	// Once cached the choice is sticky. Callers should invoke this when they have authoritative
	// information about whether a new DB on this bucket has opted in (e.g., PUT /<db>/ with
	// use_system_metadata_collection: true) so the very first bootstrap doc lands correctly.
	SetBucketBootstrapTargetHint(ctx context.Context, bucket string, optInHint bool) error
	// Close releases any long-lived connections
	Close()
}

// CouchbaseClusterSpec define how to make a connection to Couchbase Server
type CouchbaseClusterSpec struct {
	Server               string // connection string to connect to the Couchbase cluster
	Username             string // RBAC username to authenticate with the cluster
	Password             string // RBAC password to authenticate with the cluster
	X509Certpath         string // X.509 cert path to authenticate with the cluster
	X509Keypath          string // X.509 key path to authenticate with the cluster
	CACertpath           string // CA cert path to use for TLS connections
	TLSSkipVerify        bool   // If true, do not validate TLS certificate
	UseGOCBFastFailRetry bool   // When true, readiness checks fail fast instead of using the best-effort retry strategy
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	server                      string
	clusterOptions              gocb.ClusterOptions
	forcePerBucketAuth          bool // Forces perBucketAuth authenticators to be used to connect to the bucket
	perBucketAuth               map[string]*gocb.Authenticator
	bucketConnectionMode        BucketConnectionMode    // Whether to cache cluster connections
	cachedClusterConnection     *gocb.Cluster           // Cached cluster connection, should only be used by GetConfigBuckets
	cachedBucketConnections     cachedBucketConnections // Per-bucket cached connections
	cachedConnectionLock        sync.Mutex              // mutex for access to cachedBucketConnections
	configPersistence           ConfigPersistence       // ConfigPersistence mode
	useSystemMetadataCollection bool                    // When true, bootstrap metadata is stored in _system._mobile, with read-fallback to _default._default during migration
	migrationComplete           atomic.Bool             // When set, fallback reads are skipped even if useSystemMetadataCollection is true
	// bucketBootstrapTargets caches the resolved bootstrap-doc location for each bucket.
	// Resolved lazily on first interaction (or eagerly via SetBucketBootstrapTargetHint). Values
	// are bucketBootstrapTarget; absence of an entry means "fall back to the connection-wide flag."
	bucketBootstrapTargets sync.Map
	useGOCBFastFailRetry    bool                    // When true, readiness checks fail fast instead of using the best-effort retry strategy
}

// bucketBootstrapTarget records where bootstrap docs (registry, dbconfig, cbgt cfg) live for a
// particular bucket. Caches the result of probing _sync:registry across both collections.
type bucketBootstrapTarget int

const (
	bucketTargetDefault      bucketBootstrapTarget = iota // _default._default — legacy registry or no opt-in
	bucketTargetSystemMobile                              // _system._mobile primary; _default._default fallback until migration complete
)

type BucketConnectionMode int

const (
	// CachedClusterConnections mode reuses a cached cluster connection.  Should be used for recurring operations
	CachedClusterConnections BucketConnectionMode = iota
	// PerUseClusterConnections mode establishes a new cluster connection per cluster operation.  Should be used for adhoc operations
	PerUseClusterConnections
)

type cachedBucket struct {
	bucket        *gocb.Bucket // underlying bucket
	bucketCloseFn func()       // teardown function which will close the gocb connection
	refcount      int          // count of how many functions are using this cachedBucket
	shouldClose   bool         // mark this cachedBucket as needing to be closed with ref
}

// cahedBucketConnections is a lockable map cached buckets containing refcounts
type cachedBucketConnections struct {
	buckets map[string]*cachedBucket
	lock    sync.Mutex
}

// removeOutdatedBuckets marks any active buckets for closure and removes the cached connections.
func (c *cachedBucketConnections) removeOutdatedBuckets(activeBuckets Set) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for bucketName, bucket := range c.buckets {
		_, exists := activeBuckets[bucketName]
		if exists {
			continue
		}
		bucket.shouldClose = true
		c._teardown(bucketName)
	}
}

// closeAll removes all cached bucekts
func (c *cachedBucketConnections) closeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, bucket := range c.buckets {
		bucket.shouldClose = true
		bucket.bucketCloseFn()
	}
}

// teardown closes the cached bucket connection while locked, suitable for CouchbaseCluster.getBucket() teardowns
func (c *cachedBucketConnections) teardown(bucketName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.buckets[bucketName].refcount--
	c._teardown(bucketName)
}

// _teardown closes expects the lock to be acquired before calling this function and the reference count to be up to date.
func (c *cachedBucketConnections) _teardown(bucketName string) {
	if !c.buckets[bucketName].shouldClose || c.buckets[bucketName].refcount > 0 {
		return
	}
	c.buckets[bucketName].bucketCloseFn()
	delete(c.buckets, bucketName)
}

// get returns a cachedBucket for a given bucketName, or nil if it doesn't exist
func (c *cachedBucketConnections) _get(bucketName string) *cachedBucket {
	bucket, ok := c.buckets[bucketName]
	if !ok {
		return nil
	}
	c.buckets[bucketName].refcount++
	return bucket
}

// set adds a cachedBucket for a given bucketName, or nil if it doesn't exist
func (c *cachedBucketConnections) _set(bucketName string, bucket *cachedBucket) {
	c.buckets[bucketName] = bucket
}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
// useSystemMetadataCollection controls where bootstrap metadata is read/written: when true,
// _system._mobile is the source of truth and reads transparently fall back to _default._default
// for any keys that have not yet been migrated; when false, _default._default is used unconditionally.
func NewCouchbaseCluster(ctx context.Context, clusterSpec CouchbaseClusterSpec,
	forcePerBucketAuth bool, perBucketCreds PerBucketCredentialsConfig,
	useXattrConfig bool, useSystemMetadataCollection bool, bucketMode BucketConnectionMode) (*CouchbaseCluster, error) {
	securityConfig, err := GoCBv2SecurityConfig(ctx, Ptr(clusterSpec.TLSSkipVerify), clusterSpec.CACertpath)
	if err != nil {
		return nil, err
	}

	clusterAuthConfig, err := GoCBv2Authenticator(
		clusterSpec.Username, clusterSpec.Password,
		clusterSpec.X509Certpath, clusterSpec.X509Keypath,
	)
	if err != nil {
		return nil, err
	}

	// Populate individual bucket credentials
	perBucketAuth := make(map[string]*gocb.Authenticator, len(perBucketCreds))
	for bucket, credentials := range perBucketCreds {
		authenticator, err := GoCBv2Authenticator(
			credentials.Username, credentials.Password,
			credentials.X509CertPath, credentials.X509KeyPath,
		)
		if err != nil {
			return nil, err
		}
		perBucketAuth[bucket] = &authenticator
	}

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  clusterAuthConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: GoCBv2TimeoutsConfig(nil, nil),
		RetryStrategy:  gocb.NewBestEffortRetryStrategy(nil),
	}

	cbCluster := &CouchbaseCluster{
		server:                      clusterSpec.Server,
		forcePerBucketAuth:          forcePerBucketAuth,
		perBucketAuth:               perBucketAuth,
		clusterOptions:              clusterOptions,
		bucketConnectionMode:        bucketMode,
		useSystemMetadataCollection: useSystemMetadataCollection,
		useGOCBFastFailRetry: clusterSpec.UseGOCBFastFailRetry,
	}

	if bucketMode == CachedClusterConnections {
		cbCluster.cachedBucketConnections = cachedBucketConnections{buckets: make(map[string]*cachedBucket)}
	}

	cbCluster.configPersistence = &DocumentBootstrapPersistence{}
	if useXattrConfig {
		cbCluster.configPersistence = &XattrBootstrapPersistence{}
	}

	return cbCluster, nil
}

// connect attempts to open a gocb.Cluster connection. Callers will be responsible for closing the connection.
// Pass an authenticator to use that to connect instead of using the cluster credentials.
func (cc *CouchbaseCluster) connect(auth *gocb.Authenticator) (*gocb.Cluster, error) {
	clusterOptions := cc.clusterOptions
	if auth != nil {
		clusterOptions.Authenticator = *auth
		clusterOptions.Username = ""
		clusterOptions.Password = ""
	}

	cluster, err := gocb.Connect(cc.server, clusterOptions)
	if err != nil {
		return nil, err
	}

	err = cluster.WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeManagement},
		RetryStrategy: gocbRetryStrategy(cc.useGOCBFastFailRetry),
	})
	if err != nil {
		_ = cluster.Close(nil)
		return nil, err
	}

	return cluster, nil
}

func (cc *CouchbaseCluster) getClusterConnection() (*gocb.Cluster, error) {

	if cc.bucketConnectionMode == PerUseClusterConnections {
		return cc.connect(nil)
	}

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()
	if cc.cachedClusterConnection != nil {
		return cc.cachedClusterConnection, nil
	}

	clusterConnection, err := cc.connect(nil)
	if err != nil {
		return nil, err
	}
	cc.cachedClusterConnection = clusterConnection
	return cc.cachedClusterConnection, nil

}

func (cc *CouchbaseCluster) GetConfigBuckets(context.Context) ([]string, error) {
	if cc == nil {
		return nil, errors.New("nil CouchbaseCluster")
	}

	connection, err := cc.getClusterConnection()
	if err != nil {
		return nil, err
	}

	defer func() {
		if cc.bucketConnectionMode == PerUseClusterConnections {
			_ = connection.Close(nil)
		}
	}()

	buckets, err := connection.Buckets().GetAllBuckets(nil)
	if err != nil {
		cc.cachedClusterConnection = nil
		return nil, err
	}

	bucketList := make([]string, 0, len(buckets))
	for bucketName := range buckets {
		bucketList = append(bucketList, bucketName)
	}

	sort.Strings(bucketList)
	cc.cachedBucketConnections.removeOutdatedBuckets(SetOf(bucketList...))

	return bucketList, nil
}

// metadataCollections returns the primary collection where bootstrap metadata writes go,
// and an optional fallback collection that reads consult when the primary returns ErrNotFound.
// Per-bucket target cache (populated by SetBucketBootstrapTargetHint or the probe in getBucket)
// takes precedence; if a bucket isn't cached yet the connection-wide flag drives the decision.
//
// Fallback semantics are symmetric so reads can self-heal across a stale cache:
//   - target=systemMobile: primary=_system._mobile, fallback=_default._default (legacy docs not
//     yet migrated by the bucket-level migration step). Skipped once SetMigrationComplete has
//     been called.
//   - target=default (or no opt-in indication yet): primary=_default._default,
//     fallback=_system._mobile. A successful fallback hit indicates a peer has migrated this
//     bucket's bootstrap docs — the caller is expected to invoke noteBucketFallbackHit so the
//     cache is corrected before the next op.
func (cc *CouchbaseCluster) metadataCollections(b *gocb.Bucket) (primary, fallback *gocb.Collection) {
	cached, hasCachedTarget := cc.bucketBootstrapTargets.Load(b.Name())
	useSystemMobile := cc.useSystemMetadataCollection
	if hasCachedTarget {
		useSystemMobile = cached.(bucketBootstrapTarget) == bucketTargetSystemMobile
	}
	if !useSystemMobile {
		// When this bucket has any opt-in indication — either cached as bucketTargetDefault
		// (set by SetBucketBootstrapTargetHint after probing a legacy registry) or the
		// connection-wide flag — wire systemMobile as a self-heal fallback so a peer-migrated
		// bucket doesn't return not-found from a stale-cached perspective. A bucket with no
		// cache entry and no connection-wide opt-in skips the fallback entirely so non-opt-in
		// clusters don't pay an extra _system._mobile probe per op.
		if hasCachedTarget || cc.useSystemMetadataCollection {
			return b.DefaultCollection(), b.Scope(SystemScope).Collection(SystemCollectionMobile)
		}
		return b.DefaultCollection(), nil
	}
	primary = b.Scope(SystemScope).Collection(SystemCollectionMobile)
	if cc.migrationComplete.Load() {
		return primary, nil
	}
	return primary, b.DefaultCollection()
}

// noteBucketFallbackHit updates the per-bucket cache to _system._mobile after a successful read
// or existence check against the fallback collection, when that hit indicates the cache was stale
// (the cached target was _default._default — i.e. this node had not yet observed that a peer ran
// the bucket-level migration). A no-op when the cache already says _system._mobile, since the
// systemMobile→default fallback direction is the legitimate in-progress-migration legacy path.
func (cc *CouchbaseCluster) noteBucketFallbackHit(bucketName string) {
	if cached, ok := cc.bucketBootstrapTargets.Load(bucketName); ok && cached.(bucketBootstrapTarget) == bucketTargetSystemMobile {
		return
	}
	cc.bucketBootstrapTargets.Store(bucketName, bucketTargetSystemMobile)
}

// SetBucketBootstrapTargetHint resolves and caches the bootstrap-doc target for a bucket. See the
// interface doc for the decision tree. If the registry is found in either collection the location
// is cached and the hint is irrelevant. If no registry exists yet, optInHint (or the connection-
// wide flag) determines the cached target. Already-cached targets are not overwritten.
func (cc *CouchbaseCluster) SetBucketBootstrapTargetHint(ctx context.Context, bucketName string, optInHint bool) error {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}
	if _, cached := cc.bucketBootstrapTargets.Load(bucketName); cached {
		return nil
	}
	b, teardown, err := cc.getBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	defer teardown()
	target, found, err := cc.probeRegistryLocation(b)
	if err != nil {
		return err
	}
	if !found {
		if optInHint || cc.useSystemMetadataCollection {
			target = bucketTargetSystemMobile
		} else {
			target = bucketTargetDefault
		}
	}
	cc.bucketBootstrapTargets.LoadOrStore(bucketName, target)
	return nil
}

// probeRegistryLocation checks both collections for an existing _sync:registry doc. Returns
// (target, found): when found, target identifies the collection; when not found, target is
// undefined and the caller picks based on its own policy (cluster flag or per-DB hint).
// A "collection not found" probe error on _system._mobile is treated as "not present".
func (cc *CouchbaseCluster) probeRegistryLocation(b *gocb.Bucket) (target bucketBootstrapTarget, found bool, err error) {
	systemCol := b.Scope(SystemScope).Collection(SystemCollectionMobile)
	existsInSystem, sysErr := cc.configPersistence.keyExists(systemCol, SGRegistryKey)
	if sysErr == nil && existsInSystem {
		return bucketTargetSystemMobile, true, nil
	}
	defaultCol := b.DefaultCollection()
	existsInDefault, defErr := cc.configPersistence.keyExists(defaultCol, SGRegistryKey)
	if defErr != nil {
		return 0, false, defErr
	}
	if existsInDefault {
		return bucketTargetDefault, true, nil
	}
	return 0, false, nil
}

// shouldFallback returns true if a primary read returned a not-found-style error and the cluster
// has a fallback collection configured (i.e. useSystemMetadataCollection is enabled).
func (cc *CouchbaseCluster) shouldFallback(err error, fallback *gocb.Collection) bool {
	return fallback != nil && IsDocNotFoundError(err)
}

// SetMigrationComplete marks bootstrap-metadata migration as finished; subsequent reads stop
// falling back to _default._default. Safe to call concurrently with in-flight operations.
func (cc *CouchbaseCluster) SetMigrationComplete() {
	cc.migrationComplete.Store(true)
}

// RefreshBucketBootstrapTarget reads the bucket's metadata-migration status doc; if bootstrap.state
// is complete it updates the per-bucket cache to _system._mobile via noteBucketFallbackHit. ErrNotFound
// (no migration ever started here) and any other transient error are returned to the caller for
// telemetry but never escalate — this is best-effort cache convergence, not a correctness path.
func (cc *CouchbaseCluster) RefreshBucketBootstrapTarget(ctx context.Context, bucket string) error {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}
	status, _, err := cc.GetMetadataMigrationStatus(ctx, bucket)
	if err != nil {
		return err
	}
	if status.Bootstrap.State == MigrationStateComplete {
		cc.noteBucketFallbackHit(bucket)
	}
	return nil
}

// loadConfigWithFallback resolves which of primary/fallback currently holds docID and returns its
// value+CAS along with the resolved collection. Callers should pass the returned collection back
// in subsequent writes so the CAS stays valid. Re-call on CAS mismatch to handle the doc being
// migrated between collections mid-operation. On a successful fallback hit the bucket's cached
// bootstrap target is updated (via noteBucketFallbackHit) so subsequent ops route correctly.
func (cc *CouchbaseCluster) loadConfigWithFallback(ctx context.Context, bucketName string, primary, fallback *gocb.Collection, docID string) (collection *gocb.Collection, value []byte, cas gocb.Cas, err error) {
	value, cas, err = cc.configPersistence.loadRawConfig(ctx, primary, docID)
	if cc.shouldFallback(err, fallback) {
		value, cas, err = cc.configPersistence.loadRawConfig(ctx, fallback, docID)
		if err == nil {
			cc.noteBucketFallbackHit(bucketName)
			return fallback, value, cas, nil
		}
	}
	return primary, value, cas, err
}

func (cc *CouchbaseCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr any) (cas uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)

	if err != nil {
		return 0, err
	}

	defer teardown()

	primary, fallback := cc.metadataCollections(b)
	cas, err = cc.configPersistence.loadConfig(ctx, primary, docID, valuePtr)
	if cc.shouldFallback(err, fallback) {
		cas, err = cc.configPersistence.loadConfig(ctx, fallback, docID, valuePtr)
		if err == nil {
			cc.noteBucketFallbackHit(location)
		}
	}
	SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
	return cas, err
}

func (cc *CouchbaseCluster) InsertMetadataDocument(ctx context.Context, location, key string, value any) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	primary, fallback := cc.metadataCollections(b)
	// If a fallback exists and already holds the doc, surface ErrAlreadyExists rather than
	// silently creating a duplicate in primary that diverges from the legacy copy. A transient
	// fallback-read error must be propagated rather than swallowed, otherwise a TOCTOU race could
	// hide an existing legacy copy and we'd create exactly the divergence we're guarding against.
	if fallback != nil {
		exists, existsErr := cc.configPersistence.keyExists(fallback, key)
		if existsErr != nil {
			return 0, existsErr
		}
		if exists {
			cc.noteBucketFallbackHit(location)
			return 0, ErrAlreadyExists
		}
	}
	return cc.configPersistence.insertConfig(primary, key, value)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch.
// When useSystemMetadataCollection is enabled and the supplied CAS came from a fallback read
// (i.e. the document still lives in _default._default), the write is applied against the
// fallback collection so the CAS stays valid. Migration to the primary collection is left to a
// future explicit migration step.
func (cc *CouchbaseCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value any) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}
	if cas == 0 {
		return 0, RedactErrorf("CAS for %q in bucket %q must be non-zero to call WriteMetadataDocument, to add a new document use InsertMetadataDocument", MD(docID), MD(location))
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	rawDocument, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	primary, fallback := cc.metadataCollections(b)
	casOut, err := cc.configPersistence.replaceRawConfig(primary, docID, rawDocument, gocb.Cas(cas))
	if cc.shouldFallback(err, fallback) {
		// Doc is not in primary. Caller's CAS must be from a previous fallback read; replay the
		// write against the fallback collection so CAS semantics line up.
		casOut, err = cc.configPersistence.replaceRawConfig(fallback, docID, rawDocument, gocb.Cas(cas))
		if err == nil {
			cc.noteBucketFallbackHit(location)
		}
	}
	return uint64(casOut), err
}

func (cc *CouchbaseCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {

	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	primary, fallback := cc.metadataCollections(b)
	casOut, err := cc.configPersistence.touchConfigRollback(primary, docID, property, value, gocb.Cas(cas))
	if cc.shouldFallback(err, fallback) {
		casOut, err = cc.configPersistence.touchConfigRollback(fallback, docID, property, value, gocb.Cas(cas))
		if err == nil {
			cc.noteBucketFallbackHit(location)
		}
	}
	return uint64(casOut), err

}

func (cc *CouchbaseCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) (err error) {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return err
	}
	defer teardown()

	primary, fallback := cc.metadataCollections(b)
	_, removeErr := cc.configPersistence.removeRawConfig(primary, key, gocb.Cas(cas))
	if cc.shouldFallback(removeErr, fallback) {
		_, removeErr = cc.configPersistence.removeRawConfig(fallback, key, gocb.Cas(cas))
		if removeErr == nil {
			cc.noteBucketFallbackHit(location)
		}
	}
	return removeErr
}

// UpdateMetadataDocument retries on CAS mismatch.
// When useSystemMetadataCollection is enabled the read-modify-write loop re-resolves the owning
// collection (primary or fallback) on every retry, so a doc that migrates between collections
// concurrently with this Update is picked up on the next iteration instead of returning ErrNotFound.
func (cc *CouchbaseCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	primary, fallback := cc.metadataCollections(b)

	collection, bucketValue, cas, err := cc.loadConfigWithFallback(ctx, location, primary, fallback, docID)
	if err != nil {
		return 0, err
	}

	for {
		newConfig, err := updateCallback(bucketValue, uint64(cas))
		if err != nil {
			return 0, err
		}

		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := cc.configPersistence.removeRawConfig(collection, docID, cas)
			if err != nil {
				if errors.Is(err, gocb.ErrCasMismatch) {
					collection, bucketValue, cas, err = cc.loadConfigWithFallback(ctx, location, primary, fallback, docID)
					if err != nil {
						return 0, err
					}
					continue
				}
				return 0, err
			}
			return uint64(removeCasOut), nil
		}

		replaceCfgCasOut, err := cc.configPersistence.replaceRawConfig(collection, docID, newConfig, cas)
		if err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				collection, bucketValue, cas, err = cc.loadConfigWithFallback(ctx, location, primary, fallback, docID)
				if err != nil {
					return 0, err
				}
				continue
			}
			return 0, err
		}

		return uint64(replaceCfgCasOut), nil
	}

}

// KeyExists checks whether a key exists in the bootstrap metadata collection for the specified bucket.
// When useSystemMetadataCollection is enabled, both the primary (_system._mobile) and fallback
// (_default._default) collections are checked, mirroring read semantics of GetMetadataDocument.
func (cc *CouchbaseCluster) KeyExists(ctx context.Context, location, docID string) (exists bool, err error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)

	if err != nil {
		return false, err
	}

	defer teardown()

	primary, fallback := cc.metadataCollections(b)
	exists, err = cc.configPersistence.keyExists(primary, docID)
	if err != nil || exists || fallback == nil {
		return exists, err
	}
	exists, err = cc.configPersistence.keyExists(fallback, docID)
	if err == nil && exists {
		cc.noteBucketFallbackHit(location)
	}
	return exists, err
}

// GetDocument fetches a document from the default collection.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (cc *CouchbaseCluster) GetDocument(ctx context.Context, bucketName, docID string, rv any) (exists bool, err error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, bucketName)
	if err != nil {
		return false, err
	}

	defer teardown()

	getOptions := &gocb.GetOptions{
		Transcoder: NewSGJSONTranscoder(),
	}
	getResult, err := b.DefaultCollection().Get(docID, getOptions)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return false, nil
		}
		return false, err
	}
	err = getResult.Content(rv)
	return true, err
}

// GetRawDocument fetches a document from the default collection as raw bytes.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (cc *CouchbaseCluster) GetRawDocument(ctx context.Context, bucketName, docID string) (value []byte, exists bool, err error) {
	if cc == nil {
		return nil, false, errors.New("nil CouchbaseCluster")
	}
	b, teardown, err := cc.getBucket(ctx, bucketName)
	if err != nil {
		return nil, false, err
	}

	defer teardown()
	getOptions := &gocb.GetOptions{
		Transcoder: NewSGRawTranscoder(),
	}
	getResult, err := b.DefaultCollection().Get(docID, getOptions)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	err = getResult.Content(&value)
	return value, true, err
}

// systemMobileCollection returns the _system._mobile collection for the supplied bucket. Used by
// the metadata-migration status doc path, which must bypass the dual-collection wrapper.
func (cc *CouchbaseCluster) systemMobileCollection(b *gocb.Bucket) *gocb.Collection {
	return b.Scope(SystemScope).Collection(SystemCollectionMobile)
}

// GetMetadataMigrationStatus reads the status doc directly from _system._mobile.
func (cc *CouchbaseCluster) GetMetadataMigrationStatus(ctx context.Context, bucket string) (*MetadataMigrationStatus, uint64, error) {
	if cc == nil {
		return nil, 0, errors.New("nil CouchbaseCluster")
	}
	b, teardown, err := cc.getBucket(ctx, bucket)
	if err != nil {
		return nil, 0, err
	}
	defer teardown()

	res, err := cc.systemMobileCollection(b).Get(MetadataMigrationStatusDocID, &gocb.GetOptions{Transcoder: NewSGJSONTranscoder()})
	if err != nil {
		if IsDocNotFoundError(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	status := &MetadataMigrationStatus{}
	if err := res.Content(status); err != nil {
		return nil, 0, err
	}
	return status, uint64(res.Cas()), nil
}

// InsertMetadataMigrationStatus stamps the initial status doc in _system._mobile. Concurrent
// stamp attempts from peer SG nodes lose the CAS race and receive ErrAlreadyExists.
func (cc *CouchbaseCluster) InsertMetadataMigrationStatus(ctx context.Context, bucket string, status *MetadataMigrationStatus) (uint64, error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}
	if status == nil {
		return 0, errors.New("nil MetadataMigrationStatus")
	}
	b, teardown, err := cc.getBucket(ctx, bucket)
	if err != nil {
		return 0, err
	}
	defer teardown()

	res, err := cc.systemMobileCollection(b).Insert(MetadataMigrationStatusDocID, status, &gocb.InsertOptions{Transcoder: NewSGJSONTranscoder()})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentExists) {
			return 0, ErrAlreadyExists
		}
		return 0, err
	}
	return uint64(res.Cas()), nil
}

// UpdateMetadataMigrationStatus runs the callback inside a CAS-safe read-modify-write loop. If the
// doc is absent the callback receives a freshly seeded status (created via NewMetadataMigrationStatus)
// and a successful return triggers an Insert; otherwise a Replace is issued with the observed CAS.
func (cc *CouchbaseCluster) UpdateMetadataMigrationStatus(ctx context.Context, bucket string, callback func(*MetadataMigrationStatus) error) (uint64, error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}
	b, teardown, err := cc.getBucket(ctx, bucket)
	if err != nil {
		return 0, err
	}
	defer teardown()
	col := cc.systemMobileCollection(b)

	for {
		var (
			status  = &MetadataMigrationStatus{}
			cas     gocb.Cas
			isFresh bool
		)
		res, getErr := col.Get(MetadataMigrationStatusDocID, &gocb.GetOptions{Transcoder: NewSGJSONTranscoder()})
		if getErr != nil {
			if !IsDocNotFoundError(getErr) {
				return 0, getErr
			}
			status = NewMetadataMigrationStatus()
			isFresh = true
		} else {
			if err := res.Content(status); err != nil {
				return 0, err
			}
			cas = res.Cas()
		}
		if err := callback(status); err != nil {
			return 0, err
		}
		if isFresh {
			ins, err := col.Insert(MetadataMigrationStatusDocID, status, &gocb.InsertOptions{Transcoder: NewSGJSONTranscoder()})
			if err != nil {
				if errors.Is(err, gocb.ErrDocumentExists) {
					// peer SG won the race; retry the read-modify-write against the real doc
					continue
				}
				return 0, err
			}
			return uint64(ins.Cas()), nil
		}
		repl, err := col.Replace(MetadataMigrationStatusDocID, status, &gocb.ReplaceOptions{Cas: cas, Transcoder: NewSGJSONTranscoder()})
		if err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				continue
			}
			return 0, err
		}
		return uint64(repl.Cas()), nil
	}
}

// MigrateBootstrapDocs is the bucket-level copy step. For each supplied docID, reads the raw body
// from _default._default and CAS-inserts it into _system._mobile; if primary already has the doc
// the fallback copy is left for the verify-and-delete sweep below. Once every doc is in primary
// the fallback copy is removed with the originally observed CAS — a CAS mismatch means a write
// slipped in mid-migration, which we log and skip (the caller is responsible for re-running).
func (cc *CouchbaseCluster) MigrateBootstrapDocs(ctx context.Context, bucket string, docIDs []string) error {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}
	b, teardown, err := cc.getBucket(ctx, bucket)
	if err != nil {
		return err
	}
	defer teardown()

	primary := cc.systemMobileCollection(b)
	fallback := b.DefaultCollection()
	raw := &gocb.GetOptions{Transcoder: NewSGRawTranscoder()}

	for _, docID := range docIDs {
		fallbackRes, err := fallback.Get(docID, raw)
		if err != nil {
			if IsDocNotFoundError(err) {
				continue
			}
			return fmt.Errorf("read fallback %q during bootstrap migration: %w", docID, err)
		}
		var body []byte
		if err := fallbackRes.Content(&body); err != nil {
			return fmt.Errorf("decode fallback %q: %w", docID, err)
		}
		fallbackCas := fallbackRes.Cas()

		// Use the JSON transcoder for both sides so the destination preserves JSONType flags
		// even when the source datatype is masked behind the raw read.
		_, insertErr := primary.Insert(docID, json.RawMessage(body), &gocb.InsertOptions{Transcoder: gocb.NewRawJSONTranscoder()})
		if insertErr != nil && !errors.Is(insertErr, gocb.ErrDocumentExists) {
			return fmt.Errorf("insert primary %q during bootstrap migration: %w", docID, insertErr)
		}

		if _, err := fallback.Remove(docID, &gocb.RemoveOptions{Cas: fallbackCas}); err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
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
	cc.bucketBootstrapTargets.Store(bucket, bucketTargetSystemMobile)
	return nil
}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (cc *CouchbaseCluster) Close() {

	cc.cachedBucketConnections.closeAll()

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()

	if cc.cachedClusterConnection != nil {
		_ = cc.cachedClusterConnection.Close(nil)
		cc.cachedClusterConnection = nil
	}
}

func (cc *CouchbaseCluster) getBucket(ctx context.Context, bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {

	if cc.bucketConnectionMode != CachedClusterConnections {
		b, teardownFn, err = cc.connectToBucket(ctx, bucketName)
		if err != nil {
			return nil, nil, err
		}
		cc.ensureBucketBootstrapTargetCached(b)
		return b, teardownFn, nil
	}

	teardownFn = func() {
		cc.cachedBucketConnections.teardown(bucketName)
	}
	cc.cachedBucketConnections.lock.Lock()
	defer cc.cachedBucketConnections.lock.Unlock()
	bucket := cc.cachedBucketConnections._get(bucketName)
	if bucket != nil {
		cc.ensureBucketBootstrapTargetCached(bucket.bucket)
		return bucket.bucket, teardownFn, nil
	}

	// cached bucket not found, connect and add
	newBucket, bucketCloseFn, err := cc.connectToBucket(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}
	cc.cachedBucketConnections._set(bucketName, &cachedBucket{
		bucket:        newBucket,
		bucketCloseFn: bucketCloseFn,
		refcount:      1,
	})
	cc.ensureBucketBootstrapTargetCached(newBucket)
	return newBucket, teardownFn, nil
}

// ensureBucketBootstrapTargetCached freezes the bucket's bootstrap target IF an existing
// _sync:registry is found in either collection — a definitive location we should stick to.
// When no registry exists yet the decision is left uncached so a subsequent per-DB opt-in (via
// SetBucketBootstrapTargetHint) can still claim system._mobile; metadataCollections will fall
// back to the connection-wide flag in the meantime. Best-effort: probe errors are swallowed.
func (cc *CouchbaseCluster) ensureBucketBootstrapTargetCached(b *gocb.Bucket) {
	if _, cached := cc.bucketBootstrapTargets.Load(b.Name()); cached {
		return
	}
	target, found, err := cc.probeRegistryLocation(b)
	if err != nil || !found {
		return
	}
	cc.bucketBootstrapTargets.LoadOrStore(b.Name(), target)
}

func (cc *CouchbaseCluster) GetClusterConnectionForBucket(ctx context.Context, bucketName string) (connection *gocb.Cluster, teardownFn func(), err error) {
	if bucketAuth, set := cc.perBucketAuth[bucketName]; set {
		connection, err = cc.connect(bucketAuth)
	} else if cc.forcePerBucketAuth {
		return nil, nil, fmt.Errorf("unable to get bucket %q since credentials are not defined in bucket_credentials", MD(bucketName).Redact())
	} else {
		connection, err = cc.connect(nil)
	}
	if err != nil {
		return nil, nil, err
	}

	teardownFn = func() {
		err := connection.Close(&gocb.ClusterCloseOptions{})
		if err != nil {
			WarnfCtx(ctx, "Failed to close cluster connection: %v", err)
		}
	}
	return connection, teardownFn, nil
}

// connectToBucket establishes a new connection to a bucket, and returns the bucket after waiting for it to be ready.
func (cc *CouchbaseCluster) connectToBucket(ctx context.Context, bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {
	connection, teardownFn, err := cc.GetClusterConnectionForBucket(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}

	b = connection.Bucket(bucketName)
	err = b.WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		RetryStrategy: gocbRetryStrategy(cc.useGOCBFastFailRetry),
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	})
	if err != nil {
		teardownFn()

		if errors.Is(err, gocb.ErrAuthenticationFailure) {
			return nil, nil, ErrAuthError
		}

		// In best-effort retry mode a missing/unreachable bucket surfaces as a timeout rather than an
		// auth failure. Classify it as a connection error so callers translate it to a 502 instead of a
		// generic 500, mirroring the per-database connection path (see db.connectToBucketErrorHandling).
		if !cc.useGOCBFastFailRetry {
			return nil, nil, HTTPErrorf(http.StatusBadGateway,
				"Unable to connect to Couchbase Server. Please ensure it is running and reachable, and that bucket %q exists. Error: %s", MD(bucketName).Redact(), err)
		}
		return nil, nil, err
	}

	return b, teardownFn, nil
}

type PerBucketCredentialsConfig map[string]*CredentialsConfig

type CredentialsConfig struct {
	Username string `json:"username,omitempty"       help:"Username for authenticating to the bucket"`
	Password string `json:"password,omitempty"       help:"Password for authenticating to the bucket"`
	CredentialsConfigX509
}

type CredentialsConfigX509 struct {
	X509CertPath string `json:"x509_cert_path,omitempty" help:"Cert path (public key) for X.509 bucket auth"`
	X509KeyPath  string `json:"x509_key_path,omitempty"  help:"Key path (private key) for X.509 bucket auth"`
}

// ConfigMerge applies non-empty fields from b onto non-empty fields on a
func ConfigMerge(a, b any) error {
	return mergo.Merge(a, b, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)
}

// mergoNilTransformer is a mergo.Transformers implementation that treats non-nil zero values as non-empty when merging.
type mergoNilTransformer struct{}

var _ mergo.Transformers = &mergoNilTransformer{}

func (t *mergoNilTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ.Kind() == reflect.Pointer {
		if typ.Elem().Kind() == reflect.Struct {
			// skip nilTransformer for structs, to allow recursion
			return nil
		}
		return func(dst, src reflect.Value) error {
			if dst.CanSet() && !src.IsNil() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}
