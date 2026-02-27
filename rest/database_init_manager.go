// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// DatabaseInitManager coordinates InitializeDatabase requests across multiple callers and
// databases.  At most one worker per database may be active at a time.  If the required initialization
// (based on the databaseConfig) changes for a database while a worker is already active, that worker is
// cancelled and a new one created.  Currently this is based solely on the set of collections in the config, and
// their computed index sets.
// DatabaseInitManager is only responsible for asynchronous execution of the initialization processing - it
// is not intended maintain initialization status, or the success/failure of historic executions. The expectation
// is that evaluation of whether a database has been initialized is relatively inexpensive and is the responsibility
// of the code in databaseInitWork.Run.
type DatabaseInitManager struct {

	// Set of active DatabaseInitWorkers.  Workers are removed from the set on completion.
	workers     map[string]*DatabaseInitWorker
	workersLock sync.Mutex

	// initializeIndexesFunc is defined for testability only.
	initializeIndexesFunc InitializeIndexesFunc

	// testCollectionStatusUpdateCallback is defined for testability only.
	// Invoked after collection initialization is complete for each collection
	testCollectionStatusUpdateCallback CollectionCallbackFunc

	// testDatabaseCompleteCallback is defined for testability only.
	// Invoked after worker completes, but before worker is removed from workers set
	testDatabaseCompleteCallback func(databaseName string) // Callback for testability only
}

// CollectionCallbackFunc is called when the initialization has completed for each collection on the database.
type CollectionCallbackFunc func(dbName string, scName base.ScopeAndCollectionName, status db.CollectionIndexStatus)

type InitializeIndexesFunc func(context.Context, base.N1QLStore, db.InitializeIndexOptions) error

// CollectionInitData defines the set of collections being created (by ScopeAneCollectionName), and the set of
// indexes required for each collection.
type CollectionInitData map[base.ScopeAndCollectionName]db.CollectionIndexesType

func (m *DatabaseInitManager) InitializeDatabaseWithStatusCallback(ctx context.Context, startupConfig *StartupConfig, dbConfig *DatabaseConfig, statusCallback CollectionCallbackFunc, useLegacySyncDocsIndex bool) (doneChan chan error, err error) {
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	if m.workers == nil {
		m.workers = make(map[string]*DatabaseInitWorker)
	}
	base.InfofCtx(ctx, base.KeyAll, "Initializing database %s ...",
		base.MD(dbConfig.Name))
	dbInitWorker, ok := m.workers[dbConfig.Name]

	collectionSet := buildCollectionIndexData(dbConfig)
	if ok {
		// If worker exists for the database and the collection sets match, add watcher to the existing worker
		if dbInitWorker.collectionsEqual(collectionSet) {
			// make sure the worker isn't in the process of shutting down
			if dbInitWorker.ctx.Err() == nil {
				base.InfofCtx(ctx, base.KeyAll, "Found existing database initialization for database %s ...",
					base.MD(dbConfig.Name))
				doneChan := dbInitWorker.addWatcher()
				return doneChan, nil
			}
			base.DebugfCtx(ctx, base.KeyAll, "Existing database initialization for database %s has been cancelled, starting new initialization ... ctx=%s", base.MD(dbConfig.Name), context.Cause(dbInitWorker.ctx))
		} else {
			// For a mismatch in collections, stop and remove the existing worker, then continue through to creation of new worker
			dbInitWorker.Stop("Database requested to be initialized with different collections than existing initialization, canceling existing initialization.")
		}
		delete(m.workers, dbConfig.Name)
	}

	opts := bootstrapConnectionOptsConfigs(startupConfig, dbConfig.DbConfig)
	opts.bucketConnectionMode = base.PerUseClusterConnections

	base.InfofCtx(ctx, base.KeyAll, "Starting new initialization for database %s ...",
		base.MD(dbConfig.Name))

	couchbaseCluster, err := createBootstrapConnectionWithOpts(ctx, opts)
	if err != nil {
		return nil, err
	}

	bucketName := dbConfig.Name
	if dbConfig.Bucket != nil {
		bucketName = *dbConfig.Bucket
	}

	cc, ok := couchbaseCluster.(*base.CouchbaseCluster)
	if !ok {
		return nil, fmt.Errorf("DatabaseInitManager requires gocb.Cluster connection - had %T", couchbaseCluster)
	}

	connection, closeClusterConnection, err := cc.GetClusterConnectionForBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	// Initialize ClusterN1QLStore for the bucket.  Scope and collection name are set per-operation
	n1qlStore, err := base.NewClusterOnlyN1QLStore(connection, bucketName, "", "")
	if err != nil {
		return nil, err
	}

	indexOptions := m.buildIndexOptions(dbConfig, useLegacySyncDocsIndex)

	// allow the test callback to be overridden by the caller if desired
	if statusCallback == nil {
		statusCallback = m.testCollectionStatusUpdateCallback
	}

	initializeIndexesFunc := db.InitializeIndexes
	if m.initializeIndexesFunc != nil {
		initializeIndexesFunc = m.initializeIndexesFunc
	}
	// Create new worker and add this caller as a watcher
	worker := NewDatabaseInitWorker(context.WithoutCancel(ctx), dbConfig.Name, n1qlStore, collectionSet, indexOptions, statusCallback, initializeIndexesFunc)
	m.workers[dbConfig.Name] = worker
	doneChan = worker.addWatcher()

	// Start a goroutine to perform the initialization
	go func() {
		defer closeClusterConnection()
		defer couchbaseCluster.Close()
		// worker.Run blocks until completion, and returns any error on doneChan.
		worker.Run()
		if m.testDatabaseCompleteCallback != nil {
			m.testDatabaseCompleteCallback(dbConfig.Name)
		}
		// On success, remove worker
		m.workersLock.Lock()
		delete(m.workers, dbConfig.Name)
		m.workersLock.Unlock()
	}()
	return doneChan, nil
}

// InitializeDatabase will establish a new cluster connection using the provided server config.  Establishes a new
// cluster-only N1QLStore based on the startup config to perform initialization.
func (m *DatabaseInitManager) InitializeDatabase(ctx context.Context, startupConfig *StartupConfig, dbConfig *DatabaseConfig, useLegacySyncDocsIndex bool) (doneChan chan error, err error) {
	return m.InitializeDatabaseWithStatusCallback(ctx, startupConfig, dbConfig, nil, useLegacySyncDocsIndex)
}

func (m *DatabaseInitManager) HasActiveInitialization(dbName string) bool {
	if m == nil {
		return false
	}
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	_, ok := m.workers[dbName]
	return ok
}

func (m *DatabaseInitManager) buildIndexOptions(dbConfig *DatabaseConfig, useLegacySyncDocsIndex bool) db.InitializeIndexOptions {
	return db.InitializeIndexOptions{
		WaitForIndexesOnlineOption: base.WaitForIndexesInfinite,
		NumReplicas:                dbConfig.numIndexReplicas(),
		LegacySyncDocsIndex:        useLegacySyncDocsIndex,
		UseXattrs:                  dbConfig.UseXattrs(),
		NumPartitions:              dbConfig.NumIndexPartitions(),
	}
}

// Intended for test usage.  Updates to callback function aren't synchronized
func (m *DatabaseInitManager) SetTestCallbacks(collectionCallback CollectionCallbackFunc, databaseComplete func(dbName string)) {
	m.testCollectionStatusUpdateCallback = collectionCallback
	m.testDatabaseCompleteCallback = databaseComplete
}

func (m *DatabaseInitManager) SetInitializeIndexesFunc(_ testing.TB, initializeIndexesFunc InitializeIndexesFunc) {
	m.initializeIndexesFunc = initializeIndexesFunc
}

func (m *DatabaseInitManager) Cancel(dbName string, reason string) {
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	worker, ok := m.workers[dbName]
	if !ok {
		return
	}
	worker.Stop(reason)
}

// buildCollectionIndexData determines the set of indexes required for each collection in the config, including
// the metadata collection
func buildCollectionIndexData(config *DatabaseConfig) CollectionInitData {
	if len(config.Scopes) == 0 {
		return CollectionInitData{base.DefaultScopeAndCollectionName(): db.IndexesAll}
	}

	defaultScopeAndCollectionMetadataIndexes := db.IndexesMetadataOnly

	collectionInitData := make(CollectionInitData)
	for scopeName, scopeConfig := range config.Scopes {
		for collectionName := range scopeConfig.Collections {
			scName := base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
			if scName.IsDefault() {
				defaultScopeAndCollectionMetadataIndexes = db.IndexesAll
				continue
			}
			collectionInitData[scName] = db.IndexesWithoutMetadata
		}
	}

	collectionInitData[base.DefaultScopeAndCollectionName()] = defaultScopeAndCollectionMetadataIndexes

	return collectionInitData
}

// DatabaseInitWorker performs async database initialization tasks that should be performed in the background,
// independent of the database being reloaded for config changes
type DatabaseInitWorker struct {
	dbName                   string
	n1qlStore                *base.ClusterOnlyN1QLStore
	options                  DatabaseInitOptions
	ctx                      context.Context         // On close, terminates any goroutines associated with the worker
	cancelFunc               context.CancelCauseFunc // Cancel function for context, invoked if Cancel is called
	collections              CollectionInitData      // The set of collections associated with the worker, mapped by name to their index set
	collectionStatusCallback CollectionCallbackFunc  // Callback for status observability

	// Multiple goroutines (watchers) may be waiting for database initialization.  To support sending error information to
	// every goroutine, we maintain a channel for each of these watching goroutines.  On success, all channels are
	// closed.  On error, the error is sent to each channel before closing the channel
	watchers    []chan error
	watcherLock sync.Mutex // Mutex for synchronized watchers access
	completed   bool       // Set to true when processing completes, to handle watcher registration during completion.  Synchronized with watcherLock.
	lastError   error      // Set for when processing does not complete successfully.  Synchronized with watcherLock

	initializeIndexesFunc InitializeIndexesFunc // function to create indexes, for testability
}

// DatabaseInitOptions specifies the options used for database initialization
type DatabaseInitOptions struct {
	indexOptions db.InitializeIndexOptions // Options used for index initialization
}

func NewDatabaseInitWorker(ctx context.Context, dbName string, n1qlStore *base.ClusterOnlyN1QLStore, collections CollectionInitData, indexOptions db.InitializeIndexOptions, callback CollectionCallbackFunc, initializeIndexesFunc InitializeIndexesFunc) *DatabaseInitWorker {
	cancelCtx, cancelFunc := context.WithCancelCause(ctx)
	return &DatabaseInitWorker{
		dbName:                   dbName,
		options:                  DatabaseInitOptions{indexOptions: indexOptions},
		ctx:                      cancelCtx,
		cancelFunc:               cancelFunc,
		collections:              collections,
		n1qlStore:                n1qlStore,
		collectionStatusCallback: callback,
		initializeIndexesFunc:    initializeIndexesFunc,
	}
}

func (w *DatabaseInitWorker) Run() {
	// Ensure cancelFunc resources are released on normal completion
	defer func() {
		if w.cancelFunc != nil {
			w.cancelFunc(errors.New("database initialization finished normally"))
		}
	}()

	if w.collectionStatusCallback != nil {
		for scName := range w.collections {
			w.collectionStatusCallback(w.dbName, scName, db.CollectionIndexStatusQueued)
		}
	}

	var indexErr error
	for scName, indexSet := range w.collections {
		if w.collectionStatusCallback != nil {
			w.collectionStatusCallback(w.dbName, scName, db.CollectionIndexStatusInProgress)
		}

		// Add the index set to the common indexOptions
		collectionIndexOptions := w.options.indexOptions
		collectionIndexOptions.MetadataIndexes = indexSet

		// Set the scope and collection name on the cluster n1ql store for use by initializeIndexes
		w.n1qlStore.SetScopeAndCollection(scName)
		keyspaceCtx := base.KeyspaceLogCtx(w.ctx, w.n1qlStore.BucketName(), scName.ScopeName(), scName.CollectionName())
		indexErr = w.initializeIndexesFunc(keyspaceCtx, w.n1qlStore, collectionIndexOptions)
		if w.collectionStatusCallback != nil {
			if indexErr != nil {
				w.collectionStatusCallback(w.dbName, scName, db.CollectionIndexStatusError)
				break
			}
			w.collectionStatusCallback(w.dbName, scName, db.CollectionIndexStatusReady)
		}

		// Check for context cancellation after each collection is processed - if cancelled, return cancellation error
		// to all watchers end exit
		if err := w.ctx.Err(); err != nil {
			indexErr = fmt.Errorf("Database initialization cancelled: %w", err)
			break
		}
	}

	// On completion (success or error), notify watchers
	w.watcherLock.Lock()
	defer w.watcherLock.Unlock()
	w.lastError = indexErr
	for _, doneChan := range w.watchers {
		if indexErr != nil {
			doneChan <- indexErr
		}
		close(doneChan)
	}
	w.completed = true
}

// Adds a watcher for the current worker.  Creates a new notification channel for completion and adds
// to watcher set.
func (w *DatabaseInitWorker) addWatcher() (doneChan chan error) {
	w.watcherLock.Lock()
	defer w.watcherLock.Unlock()
	if w.completed {
		// If the worker has completed while we acquired the lock, return any error and close channel
		doneChan = make(chan error, 1)
		if w.lastError != nil {
			doneChan <- w.lastError
		}
		close(doneChan)
		return doneChan
	}
	doneChan = make(chan error, 1)
	w.watchers = append(w.watchers, doneChan)
	return doneChan
}

// Compare collections checks whether the provided CollectionInitData matches the set being
// initialized by the worker.
func (w *DatabaseInitWorker) collectionsEqual(newCollectionSet CollectionInitData) bool {

	if len(newCollectionSet) != len(w.collections) {
		return false
	}

	for name, indexType := range newCollectionSet {
		currentType, ok := w.collections[name]
		if !ok || currentType != indexType {
			return false
		}
	}
	return true
}

// Stop cancels the context, which will terminate initialization after the current collection being processed
func (w *DatabaseInitWorker) Stop(reason string) {
	w.cancelFunc(errors.New(reason))
}
