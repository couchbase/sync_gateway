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

	"github.com/couchbase/cbgt"
)

// CfgSG is used to manage shared information between Sync Gateway nodes.
// It implements cbgt.Cfg for use with cbgt, but can be used for to manage
// any shared data.  It uses Sync Gateway's existing
// bucket as a keystore, and existing caching feed for change notifications.
//
type CfgSG struct {
	bucket        Bucket
	loggingCtx    context.Context
	subscriptions map[string][]chan<- cbgt.CfgEvent // Keyed by key
	lock          sync.Mutex                        // mutex for subscriptions
	keyPrefix     string                            // Config doc key prefix
}

type CfgEventNotifyFunc func(docID string, cas uint64, err error)

var ErrCfgCasError = &cbgt.CfgCASError{}

// NewCfgSG returns a Cfg implementation that reads/writes its entries
// from/to a couchbase bucket, using DCP streams to subscribe to
// changes.
//
// urlStr: single URL or multiple URLs delimited by ';'
// bucket: couchbase bucket name
func NewCfgSG(bucket Bucket, groupID string) (*CfgSG, error) {

	cfgContextID := MD(bucket.GetName()).Redact() + "-cfgSG"
	loggingCtx := context.WithValue(context.Background(), LogContextKey{},
		LogContext{CorrelationID: cfgContextID},
	)

	c := &CfgSG{
		bucket:        bucket,
		loggingCtx:    loggingCtx,
		subscriptions: make(map[string][]chan<- cbgt.CfgEvent),
		keyPrefix:     SGCfgPrefixWithGroupID(groupID),
	}
	return c, nil
}

func (c *CfgSG) sgCfgBucketKey(cfgKey string) string {
	return c.keyPrefix + cfgKey
}

func (c *CfgSG) Get(cfgKey string, cas uint64) (
	[]byte, uint64, error) {

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Get, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	var value []byte
	casOut, err := c.bucket.Get(bucketKey, &value)
	if err != nil && !IsKeyNotFoundError(c.bucket, err) {
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

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Set, key: %s, cas: %d", cfgKey, cas)
	if strings.HasPrefix(cfgKey, ":") {
		return 0, fmt.Errorf("cfg_sg: key cannot start with a colon")
	}

	bucketKey := c.sgCfgBucketKey(cfgKey)

	casOut, err := c.bucket.WriteCas(bucketKey, 0, 0, cas, val, 0)

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

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Del, key: %s, cas: %d", cfgKey, cas)
	bucketKey := c.sgCfgBucketKey(cfgKey)
	_, err := c.bucket.Remove(bucketKey, cas)
	if IsCasMismatch(err) {
		return ErrCfgCasError
	} else if err != nil && !IsKeyNotFoundError(c.bucket, err) {
		return err
	}

	return err
}

func (c *CfgSG) Subscribe(cfgKey string, ch chan cbgt.CfgEvent) error {

	DebugfCtx(c.loggingCtx, KeyCluster, "cfg_sg: Subscribe, key: %s", cfgKey)
	c.lock.Lock()
	a, exists := c.subscriptions[cfgKey]
	if !exists || a == nil {
		a = make([]chan<- cbgt.CfgEvent, 0)
	}
	c.subscriptions[cfgKey] = append(a, ch)

	c.lock.Unlock()

	return nil
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
