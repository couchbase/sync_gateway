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
	"sync"

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

	// collectionCompleteCallback is defined for testability only.
	// Invoked after collection initialization is complete for each collection
	collectionCompleteCallback collectionCallbackFunc

	// databaseCompleteCallback is defined for testability only.
	// Invoked after worker completes, but before worker is removed from workers set
	databaseCompleteCallback func(databaseName string) // Callback for testability only
}

type collectionCallbackFunc func(dbName, collectionName string)

// CollectionInitData defines the set of collections being created (by ScopeAneCollectionName), and the set of
// indexes required for each collection.
type CollectionInitData map[base.ScopeAndCollectionName]db.CollectionIndexesType

// Initializes the database.  Will establish a new cluster connection using the provided server config.  Establishes a new
// cluster-only N1QLStore based on the startup config to perform initialization.
func (m *DatabaseInitManager) InitializeDatabase(ctx context.Context, startupConfig *StartupConfig, dbConfig *DatabaseConfig) (doneChan chan error, err error) {
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	if m.workers == nil {
		m.workers = make(map[string]*DatabaseInitWorker)
	}
	base.InfofCtx(ctx, base.KeyAll, "Initializing database %s ...",
		base.MD(dbConfig.Name))
	dbInitWorker, ok := m.workers[dbConfig.Name]
	collectionSet := m.buildCollectionIndexData(dbConfig)
	if ok {
		// If worker exists for the database and the collection sets match, add watcher to the existing worker
		if dbInitWorker.collectionsEqual(collectionSet) {
			base.InfofCtx(ctx, base.KeyAll, "Found existing database initialization for database %s ...",
				base.MD(dbConfig.Name))
			doneChan, err := dbInitWorker.addWatcher()
			return doneChan, err
		}
		// For a mismatch in collections, stop and remove the existing worker, then continue through to creation of new worker
		dbInitWorker.Stop()
		delete(m.workers, dbConfig.Name)
	}

	base.InfofCtx(ctx, base.KeyAll, "Starting new async initialization for database %s ...",
		base.MD(dbConfig.Name))
	couchbaseCluster, err := CreateCouchbaseClusterFromStartupConfig(startupConfig, base.PerUseClusterConnections)
	if err != nil {
		return nil, err
	}

	bucketName := dbConfig.Name
	if dbConfig.Bucket != nil {
		bucketName = *dbConfig.Bucket
	}

	// Initialize ClusterN1QLStore for the bucket.  Scope and collection name are set
	// per-operation
	n1qlStore, err := couchbaseCluster.GetClusterN1QLStore(bucketName, "", "")
	if err != nil {
		return nil, err
	}

	indexOptions := m.BuildIndexOptions(startupConfig, dbConfig)

	// Create new worker and add this caller as a watcher
	worker := NewDatabaseInitWorker(ctx, dbConfig.Name, n1qlStore, collectionSet, indexOptions, m.collectionCompleteCallback)
	m.workers[dbConfig.Name] = worker
	doneChan, err = worker.addWatcher()
	if err != nil {
		return nil, err
	}

	// Start a goroutine to perform the initialization
	go func() {
		defer couchbaseCluster.Close()
		// worker.Run blocks until completion, and returns any error on doneChan.
		worker.Run()
		if m.databaseCompleteCallback != nil {
			m.databaseCompleteCallback(dbConfig.Name)
		}
		// On success, remove worker
		m.workersLock.Lock()
		delete(m.workers, dbConfig.Name)
		m.workersLock.Unlock()
	}()
	return doneChan, nil
}

func (m *DatabaseInitManager) HasActiveInitialization(dbName string) bool {
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	_, ok := m.workers[dbName]
	return ok
}

func (m *DatabaseInitManager) BuildIndexOptions(startupConfig *StartupConfig, dbConfig *DatabaseConfig) db.InitializeIndexOptions {
	numReplicas := DefaultNumIndexReplicas
	if dbConfig.NumIndexReplicas != nil {
		numReplicas = *dbConfig.NumIndexReplicas
	}
	return db.InitializeIndexOptions{
		FailFast:    false,
		NumReplicas: numReplicas,
		Serverless:  startupConfig.IsServerless(),
		UseXattrs:   dbConfig.UseXattrs(),
	}
}

// Intended for test usage.  Updates to callback function aren't synchronized
func (m *DatabaseInitManager) SetCallbacks(collectionComplete collectionCallbackFunc, databaseComplete func(dbName string)) {
	m.collectionCompleteCallback = collectionComplete
	m.databaseCompleteCallback = databaseComplete
}

func (m *DatabaseInitManager) Cancel(dbName string) {
	m.workersLock.Lock()
	defer m.workersLock.Unlock()
	worker, ok := m.workers[dbName]
	if !ok {
		return
	}
	worker.Stop()
}

// buildCollectionIndexData determines the set of indexes required for each collection in the config, including
// the metadata collection
func (m *DatabaseInitManager) buildCollectionIndexData(config *DatabaseConfig) CollectionInitData {
	collectionInitData := make(CollectionInitData, 0)
	if len(config.Scopes) > 0 {
		hasDefaultCollection := false
		for scopeName, scopeConfig := range config.Scopes {
			for collectionName, _ := range scopeConfig.Collections {
				metadataIndexOption := db.IndexesWithoutMetadata
				if base.IsDefaultCollection(scopeName, collectionName) {
					hasDefaultCollection = true
					metadataIndexOption = db.IndexesAll
				}
				scName := base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
				collectionInitData[scName] = metadataIndexOption
			}
		}
		if !hasDefaultCollection {
			collectionInitData[base.DefaultScopeAndCollectionName()] = db.IndexesMetadataOnly
		}
	} else {
		collectionInitData[base.DefaultScopeAndCollectionName()] = db.IndexesAll
	}
	return collectionInitData
}

// DatabaseInitWorker performs async database initialization tasks that should be performed in the background,
// independent of the database being reloaded for config changes
type DatabaseInitWorker struct {
	dbName                     string
	n1qlStore                  *base.ClusterOnlyN1QLStore
	options                    DatabaseInitOptions
	ctx                        context.Context        // On close, terminates any goroutines associated with the worker
	cancelFunc                 context.CancelFunc     // Cancel function for context, invoked if Cancel is called
	collections                CollectionInitData     // The set of collections associated with the worker, mapped by name to their index set
	collectionCompleteCallback collectionCallbackFunc // Callback for testability

	// Multiple goroutines (watchers) may be waiting for database initialization.  To support sending error information to
	// every goroutine, we maintain a channel for each of these watching goroutines.  On success, all channels are
	// closed.  On error, the error is sent to each channel before closing the channel
	watchers    []chan error
	watcherLock sync.Mutex // Mutex for synchronized watchers access
	completed   bool       // Set to true when processing completes, to handle watcher registration during completion.  Synchronized with watcherLock.
	lastError   error      // Set for when processing does not complete successfully.  Synchronized with watcherLock
}

// DatabaseInitOptions specifies the options used for database initialization
type DatabaseInitOptions struct {
	indexOptions db.InitializeIndexOptions // Options used for index initialization
}

func NewDatabaseInitWorker(ctx context.Context, dbName string, n1qlStore *base.ClusterOnlyN1QLStore, collections CollectionInitData, indexOptions db.InitializeIndexOptions, callback collectionCallbackFunc) *DatabaseInitWorker {
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	return &DatabaseInitWorker{
		dbName:                     dbName,
		options:                    DatabaseInitOptions{indexOptions: indexOptions},
		ctx:                        cancelCtx,
		cancelFunc:                 cancelFunc,
		collections:                collections,
		n1qlStore:                  n1qlStore,
		collectionCompleteCallback: callback,
	}
}

func (w *DatabaseInitWorker) Run() {

	// Ensure cancelFunc resources are released on normal completion
	defer func() {
		if w.cancelFunc != nil {
			w.cancelFunc()
		}
	}()

	// TODO: CBG-2838 refactor initialize indexes to reduce number of system:indexes calls
	var indexErr error
	for scName, indexSet := range w.collections {
		// Add the index set to the common indexOptions
		collectionIndexOptions := w.options.indexOptions
		collectionIndexOptions.MetadataIndexes = indexSet

		// TODO: CBG-2838 Refactor InitializeIndexes API to move scope, collection to parameters on system:indexes calls
		// Set the scope and collection name on the cluster n1ql store for use by initializeIndexes
		w.n1qlStore.SetScopeAndCollection(scName)
		indexErr = db.InitializeIndexes(w.ctx, w.n1qlStore, collectionIndexOptions)
		if indexErr != nil {
			break
		}

		// Check for context cancellation after each collection is processed - if cancelled, return cancellation error
		// to all watchers end exit
		select {
		case <-w.ctx.Done():
			indexErr = errors.New("Database initialization cancelled")
		default:
		}
		if indexErr != nil {
			break
		}

		if w.collectionCompleteCallback != nil {
			w.collectionCompleteCallback(w.dbName, scName.CollectionName())
		}
	}

	// On completion (success or error), notify watchers
	w.watcherLock.Lock()
	w.lastError = indexErr
	for _, doneChan := range w.watchers {
		if indexErr != nil {
			doneChan <- indexErr
		}
		close(doneChan)
	}
	w.completed = true
	w.watcherLock.Unlock()
}

// Adds a watcher for the current worker.  Creates a new notification channel for completion and adds
// to watcher set.
func (w *DatabaseInitWorker) addWatcher() (doneChan chan error, err error) {
	w.watcherLock.Lock()
	defer w.watcherLock.Unlock()
	if w.completed {
		// If the worker has completed while we acquired the lock, return any error and close channel
		doneChan = make(chan error, 1)
		if w.lastError != nil {
			doneChan <- w.lastError
		}
		close(doneChan)
		return doneChan, nil
	}
	doneChan = make(chan error, 1)
	w.watchers = append(w.watchers, doneChan)
	return doneChan, nil
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
func (w *DatabaseInitWorker) Stop() {
	w.cancelFunc()
}
