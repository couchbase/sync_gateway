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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
)

const SGCbgtMetadataVersion = "5.5.0" // cbgt metadata version, matching 3.0 clients

// CfgSG is used to manage shared information between Sync Gateway nodes.
// It implements cbgt.Cfg for use with cbgt, but can be used for to manage
// any shared data.  It uses Sync Gateway's existing
// bucket as a keystore, and existing caching feed for change notifications.
type CfgSG struct {
	datastore     DataStore
	loggingCtx    context.Context
	subscriptions map[string][]chan<- cbgt.CfgEvent // Keyed by key
	lock          sync.Mutex                        // mutex for subscriptions
	keyPrefix     string                            // Config doc key prefix
	nodePoller    *cfgNodePoller
}

type CfgEventNotifyFunc func(docID string, cas uint64, err error)

var ErrCfgCasError = &cbgt.CfgCASError{}

func newCfgSG(ctx context.Context, datastore sgbucket.DataStore, keyPrefix string, useNodePoller bool, pollInterval time.Duration) (*CfgSG, error) {

	cfgContextID := MD(datastore.GetName()).Redact() + "-cfgSG"
	// should this inherit DB context?
	loggingCtx := LogContextWith(ctx, &LogContext{CorrelationID: cfgContextID})

	c := &CfgSG{
		datastore:     datastore,
		loggingCtx:    loggingCtx,
		subscriptions: make(map[string][]chan<- cbgt.CfgEvent),
		keyPrefix:     keyPrefix,
	}

	if useNodePoller {
		c.nodePoller = newCfgNodePoller(loggingCtx, datastore, c.FireEvent, pollInterval)
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

func (c *CfgSG) sgCfgBucketKey(cfgKey string) string {
	return c.keyPrefix + cfgKey
}

func (c *CfgSG) Get(cfgKey string, cas uint64) (
	[]byte, uint64, error) {
	return c.get(c.datastore, cfgKey, cas)
}

// get is Get against the given datastore, which is c.datastore or a wrapper around it.
func (c *CfgSG) get(datastore DataStore, cfgKey string, cas uint64) ([]byte, uint64, error) {
	if cfgKey == cbgt.VERSION_KEY {
		return []byte(SGCbgtMetadataVersion), cas, nil
	}

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Get, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	var value []byte
	casOut, err := datastore.Get(c.loggingCtx, bucketKey, &value)
	if err != nil && !IsDocNotFoundError(err) {
		InfofCtx(c.loggingCtx, KeyCluster, "cfg_sg: Get, key: %s, cas: %d, err: %v", cfgKey, cas, err)
		return nil, 0, err
	}

	if cas != 0 && casOut != cas {
		InfofCtx(c.loggingCtx, KeyCluster, "cfg_sg: Get, CasError key: %s, cas: %d", cfgKey, cas)
		return nil, 0, ErrCfgCasError
	}

	return value, casOut, nil
}

func (c *CfgSG) Set(cfgKey string, val []byte, cas uint64) (uint64, error) {
	return c.set(c.datastore, cfgKey, val, cas)
}

// set is Set against the given datastore, which is c.datastore or a wrapper around it.
func (c *CfgSG) set(datastore DataStore, cfgKey string, val []byte, cas uint64) (uint64, error) {
	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Set, key: %s, cas: %d", cfgKey, cas)
	if strings.HasPrefix(cfgKey, ":") {
		return 0, fmt.Errorf("cfg_sg: key cannot start with a colon")
	}

	bucketKey := c.sgCfgBucketKey(cfgKey)
	casOut, err := datastore.WriteCas(c.loggingCtx, bucketKey, 0, cas, val, 0)

	if IsCasMismatch(err) {
		InfofCtx(c.loggingCtx, KeyCluster, "cfg_sg: Set, ErrKeyExists key: %s, cas: %d", cfgKey, cas)
		return 0, ErrCfgCasError
	} else if err != nil {
		InfofCtx(c.loggingCtx, KeyCluster, "cfg_sg: Set Error key: %s, cas: %d err:%s", cfgKey, cas, err)
		return 0, err
	}

	return casOut, nil
}

func (c *CfgSG) Del(cfgKey string, cas uint64) error {
	return c.del(c.datastore, cfgKey, cas)
}

// del is Del against the given datastore, which is c.datastore or a wrapper around it.
func (c *CfgSG) del(datastore DataStore, cfgKey string, cas uint64) error {
	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Del, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	_, err := datastore.Remove(c.loggingCtx, bucketKey, cas)
	if IsCasMismatch(err) {
		return ErrCfgCasError
	} else if err != nil && !IsDocNotFoundError(err) {
		return err
	}

	return err
}

func (c *CfgSG) Subscribe(cfgKey string, ch chan cbgt.CfgEvent) error {

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Subscribe, key: %s", cfgKey)
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
	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: FireEvent, key: %s, cas %d", cfgKey, cas)
	for _, ch := range c.subscriptions[cfgKey] {
		go func(ch chan<- cbgt.CfgEvent) {
			ch <- cbgt.CfgEvent{
				Key: cfgKey, CAS: cas, Error: err,
			}
		}(ch)
	}
	c.lock.Unlock()
}

func (c *CfgSG) Refresh() error {

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Refresh")
	c.lock.Lock()
	for cfgKey, cs := range c.subscriptions {
		event := cbgt.CfgEvent{Key: cfgKey}
		for _, ch := range cs {
			go func(ch chan<- cbgt.CfgEvent, event cbgt.CfgEvent) {
				ch <- event
			}(ch, event)
		}
	}
	c.lock.Unlock()
	return nil
}

// kvOpStatCfg is a CfgSG whose KV operations are added to a stat. cbgt and SG Replicate read their config through the
// cbgt.Cfg interface, which has no way to report KV operations, so a caller that needs them counted wraps the Cfg.
type kvOpStatCfg struct {
	*CfgSG
	datastore DataStore // the CfgSG's datastore, wrapped to add its KV operations to the stat
}

// NewKVOpStatCfg wraps cfg so that every KV operation made through it is added to stat. Returns cfg unchanged when stat
// is nil, or when cfg is not a CfgSG, since a Cfg held in memory makes no KV operations.
func NewKVOpStatCfg(cfg cbgt.Cfg, stat *SgwIntStat) cbgt.Cfg {
	sgCfg, ok := cfg.(*CfgSG)
	if !ok || stat == nil {
		return cfg
	}
	return &kvOpStatCfg{CfgSG: sgCfg, datastore: NewKVOpStatDataStore(sgCfg.datastore, stat)}
}

func (c *kvOpStatCfg) Get(cfgKey string, cas uint64) ([]byte, uint64, error) {
	return c.CfgSG.get(c.datastore, cfgKey, cas)
}

func (c *kvOpStatCfg) Set(cfgKey string, val []byte, cas uint64) (uint64, error) {
	return c.CfgSG.set(c.datastore, cfgKey, val, cas)
}

func (c *kvOpStatCfg) Del(cfgKey string, cas uint64) error {
	return c.CfgSG.del(c.datastore, cfgKey, cas)
}
