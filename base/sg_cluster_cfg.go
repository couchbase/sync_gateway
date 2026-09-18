/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
)

const SGCbgtMetadataVersion = "5.5.0" // cbgt metadata version, matching 3.0 clients

// cfgSGDataStore is the subset of DataStore CfgSG needs, so it can run against either a real
// bucket or cfgMemoryStorage.
type cfgSGDataStore interface {
	Get(ctx context.Context, k string, rv any) (cas uint64, err error)
	WriteCas(ctx context.Context, k string, exp uint32, cas uint64, v any, opt sgbucket.WriteOptions) (casOut uint64, err error)
	Remove(ctx context.Context, k string, cas uint64) (casOut uint64, err error)
}

// CfgSG is used to manage shared information between Sync Gateway nodes.
// It implements cbgt.Cfg for use with cbgt, but can be used for to manage
// any shared data.  It uses Sync Gateway's existing
// bucket as a keystore, and existing caching feed for change notifications.
type CfgSG struct {
	datastore     cfgSGDataStore
	ctx           context.Context
	cancelFn      context.CancelCauseFunc           // unblocks pending FireEvent/Refresh sends on Stop
	subscriptions map[string][]chan<- cbgt.CfgEvent // Keyed by key
	lock          sync.Mutex                        // mutex for subscriptions
	keyPrefix     string                            // Config doc key prefix
	nodePoller    *cfgNodePoller
}

type CfgEventNotifyFunc func(docID string, cas uint64, err error)

var ErrCfgCasError = &cbgt.CfgCASError{}

// newCfgSGBase constructs a CfgSG.
func newCfgSGBase(ctx context.Context) *CfgSG {
	cancelCtx, cancelFn := context.WithCancelCause(ctx)
	return &CfgSG{
		ctx:           cancelCtx,
		cancelFn:      cancelFn,
		subscriptions: make(map[string][]chan<- cbgt.CfgEvent),
	}
}

// newCfgSG returns a bucket-backed CfgSG, polling for changes at pollInterval if useNodePoller is true.
func newCfgSG(ctx context.Context, datastore sgbucket.DataStore, keyPrefix string, useNodePoller bool, pollInterval time.Duration) (*CfgSG, error) {
	ctx = CorrelationIDLogCtx(ctx, MD(datastore.GetName()).Redact()+"-cfgSG")
	c := newCfgSGBase(ctx)
	c.datastore = datastore
	c.keyPrefix = keyPrefix

	if useNodePoller {
		c.nodePoller = newCfgNodePoller(c.ctx, datastore, c.FireEvent, pollInterval)
	}

	return c, nil
}

// NewCfgSG returns a Cfg implementation that reads/writes its entries
// from/to a couchbase datastore. All document names will start with keyPrefix.
// If useNodePoller is true, then any document changes are received by node polling with the specified pollInterval.
// If useNodePoller is not true, then the caller needs to register event changes itself by calling FireEvent.
//
// The caching feed implements FireEvent calls by looking for document changes starting with keyPrefix and calling FireEvent.
func NewCfgSG(ctx context.Context, datastore sgbucket.DataStore, keyPrefix string, useNodePoller bool) (*CfgSG, error) {
	return newCfgSG(ctx, datastore, keyPrefix, useNodePoller, DefaultHeartbeatPollInterval)
}

// NewCbgtCfgMem returns a cfgMemoryStorage-backed Cfg - no keyPrefix, no node poller. CE
// replacement for cbgt.NewCfgMem.
func NewCbgtCfgMem(ctx context.Context) (*CfgSG, error) {
	ctx = CorrelationIDLogCtx(ctx, "cfgSG-mem")
	c := newCfgSGBase(ctx)
	c.datastore = newCfgMemoryStorage(c.FireEvent)
	return c, nil
}

// Stop unblocks pending FireEvent/Refresh sends (cbgt.Cfg has no Unsubscribe) and stops the node
// poller, if any.
func (c *CfgSG) Stop() {
	c.cancelFn(errors.New("cfg_sg: stopped"))
}

func (c *CfgSG) sgCfgBucketKey(cfgKey string) string {
	return c.keyPrefix + cfgKey
}

func (c *CfgSG) Get(cfgKey string, cas uint64) (
	[]byte, uint64, error) {

	if cfgKey == cbgt.VERSION_KEY {
		return []byte(SGCbgtMetadataVersion), cas, nil
	}

	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: Get, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	var value []byte
	casOut, err := c.datastore.Get(c.ctx, bucketKey, &value)
	if err != nil && !IsDocNotFoundError(err) {
		InfofCtx(c.ctx, KeyCluster, "cfg_sg: Get, key: %s, cas: %d, err: %v", cfgKey, cas, err)
		return nil, 0, err
	}

	if cas != 0 && casOut != cas {
		InfofCtx(c.ctx, KeyCluster, "cfg_sg: Get, CasError key: %s, cas: %d", cfgKey, cas)
		return nil, 0, ErrCfgCasError
	}

	return value, casOut, nil
}

func (c *CfgSG) Set(cfgKey string, val []byte, cas uint64) (uint64, error) {

	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: Set, key: %s, cas: %d", cfgKey, cas)
	if strings.HasPrefix(cfgKey, ":") {
		return 0, fmt.Errorf("cfg_sg: key cannot start with a colon")
	}

	bucketKey := c.sgCfgBucketKey(cfgKey)
	casOut, err := c.datastore.WriteCas(c.ctx, bucketKey, 0, cas, val, 0)

	if IsCasMismatch(err) {
		InfofCtx(c.ctx, KeyCluster, "cfg_sg: Set, ErrKeyExists key: %s, cas: %d", cfgKey, cas)
		return 0, ErrCfgCasError
	} else if err != nil {
		InfofCtx(c.ctx, KeyCluster, "cfg_sg: Set Error key: %s, cas: %d err:%s", cfgKey, cas, err)
		return 0, err
	}

	return casOut, nil
}

func (c *CfgSG) Del(cfgKey string, cas uint64) error {

	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: Del, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	_, err := c.datastore.Remove(c.ctx, bucketKey, cas)
	if IsCasMismatch(err) {
		return ErrCfgCasError
	} else if err != nil && !IsDocNotFoundError(err) {
		return err
	}

	return err
}

func (c *CfgSG) Subscribe(cfgKey string, ch chan cbgt.CfgEvent) error {

	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: Subscribe, key: %s", cfgKey)
	c.lock.Lock()
	defer c.lock.Unlock()
	a, exists := c.subscriptions[cfgKey]
	if !exists || a == nil {
		a = make([]chan<- cbgt.CfgEvent, 0)
	}
	c.subscriptions[cfgKey] = append(a, ch)

	if c.nodePoller == nil {
		return nil
	}
	return c.nodePoller.Register(c.sgCfgBucketKey(cfgKey))
}

func (c *CfgSG) FireEvent(docID string, cas uint64, err error) {
	cfgKey := strings.TrimPrefix(docID, c.keyPrefix)
	c.lock.Lock()
	defer c.lock.Unlock()
	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: FireEvent, key: %s, cas %d", cfgKey, cas)
	for _, ch := range c.subscriptions[cfgKey] {
		go func(ch chan<- cbgt.CfgEvent) {
			select {
			case ch <- cbgt.CfgEvent{Key: cfgKey, CAS: cas, Error: err}:
			case <-c.ctx.Done():
			}
		}(ch)
	}
}

func (c *CfgSG) Refresh() error {

	DebugfCtx(c.ctx, KeyCluster, "cfg_sg: Refresh")
	c.lock.Lock()
	defer c.lock.Unlock()
	for cfgKey, cs := range c.subscriptions {
		event := cbgt.CfgEvent{Key: cfgKey}
		for _, ch := range cs {
			go func(ch chan<- cbgt.CfgEvent, event cbgt.CfgEvent) {
				select {
				case ch <- event:
				case <-c.ctx.Done():
				}
			}(ch, event)
		}
	}
	return nil
}

// cfgMemoryStorageEntry is a single stored value and its CAS, as tracked by cfgMemoryStorage.
type cfgMemoryStorageEntry struct {
	cas uint64
	val []byte
}

// cfgMemoryStorage stands in for a bucket-backed DataStore; WriteCas/Remove fire events
// themselves since there's no caching feed to notice the write. Its CAS/entry semantics are
// based on cbgt.CfgMem, the in-memory cbgt.Cfg implementation this type replaces for CE.
type cfgMemoryStorage struct {
	lock      sync.Mutex
	entries   map[string]*cfgMemoryStorageEntry
	casNext   uint64
	fireEvent CfgEventNotifyFunc
}

// newCfgMemoryStorage returns an empty cfgMemoryStorage that calls fireEvent on every WriteCas/Remove.
func newCfgMemoryStorage(fireEvent CfgEventNotifyFunc) *cfgMemoryStorage {
	return &cfgMemoryStorage{
		entries:   make(map[string]*cfgMemoryStorageEntry),
		casNext:   1,
		fireEvent: fireEvent,
	}
}

// Get copies the stored value for k into rv, a *[]byte, if rv is non-nil.
func (m *cfgMemoryStorage) Get(_ context.Context, k string, rv any) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry, exists := m.entries[k]
	if !exists {
		return 0, sgbucket.MissingError{Key: k}
	}

	if rv != nil {
		out, ok := rv.(*[]byte)
		if !ok {
			return 0, fmt.Errorf("cfgMemoryStorage: Get requires a *[]byte, got %T", rv)
		}
		val := make([]byte, len(entry.val))
		copy(val, entry.val)
		*out = val
	}
	return entry.cas, nil
}

// WriteCas stores v under k if cas matches the current entry's CAS (or the entry doesn't exist and cas is 0), then fires an event.
func (m *cfgMemoryStorage) WriteCas(_ context.Context, k string, _ uint32, cas uint64, v any, _ sgbucket.WriteOptions) (uint64, error) {
	val, ok := v.([]byte)
	if !ok {
		return 0, fmt.Errorf("cfgMemoryStorage: WriteCas requires a []byte value, got %T", v)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	prevEntry, exists := m.entries[k]
	switch {
	case cas == cbgt.CFG_CAS_FORCE:
	case !exists:
		if cas != 0 {
			return 0, sgbucket.CasMismatchErr{Expected: cas, Actual: 0}
		}
	case cas == 0 || cas != prevEntry.cas:
		return 0, sgbucket.CasMismatchErr{Expected: cas, Actual: prevEntry.cas}
	}

	newVal := make([]byte, len(val))
	copy(newVal, val)
	entry := &cfgMemoryStorageEntry{cas: m.casNext, val: newVal}
	m.entries[k] = entry
	m.casNext++
	casOut := entry.cas

	// fireEvent only spawns goroutines and returns immediately, so firing under m.lock is fine
	if m.fireEvent != nil {
		m.fireEvent(k, casOut, nil)
	}
	return casOut, nil
}

// Remove deletes the entry for k if cas matches (or cas is 0), then fires an event.
func (m *cfgMemoryStorage) Remove(_ context.Context, k string, cas uint64) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry, exists := m.entries[k]
	if !exists {
		return 0, sgbucket.MissingError{Key: k}
	}
	if cas != 0 && cas != entry.cas {
		return 0, sgbucket.CasMismatchErr{Expected: cas, Actual: entry.cas}
	}
	delete(m.entries, k)

	if m.fireEvent != nil {
		m.fireEvent(k, 0, nil)
	}
	return 0, nil
}
