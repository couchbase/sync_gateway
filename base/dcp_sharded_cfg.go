package base

import (
	"context"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/gocb"
)

// CfgSG is an implementation of Cfg that uses Sync Gateway's existing
// bucket connection, and existing caching feed to get change notifications.
//
type CfgSG struct {
	bucket        *gocb.Bucket
	loggingCtx    context.Context
	subscriptions map[string][]chan<- cbgt.CfgEvent // Keyed by key
	lock          sync.Mutex                        // mutex for subscriptions
}

// NewCfgCB returns a Cfg implementation that reads/writes its entries
// from/to a couchbase bucket, using DCP streams to subscribe to
// changes.
//
// urlStr: single URL or multiple URLs delimited by ';'
// bucket: couchbase bucket name
func NewCfgSG(bucket *gocb.Bucket) (*CfgSG, error) {

	cfgContextID := MD(bucket.Name()).Redact() + "-" + DCPImportFeedID
	loggingCtx := context.WithValue(context.Background(), LogContextKey{},
		LogContext{CorrelationID: cfgContextID},
	)

	c := &CfgSG{
		bucket:        bucket,
		loggingCtx:    loggingCtx,
		subscriptions: make(map[string][]chan<- cbgt.CfgEvent),
	}
	return c, nil
}

func sgCfgBucketKey(cfgKey string) string {
	return SGCfgPrefix + cfgKey
}

func (c *CfgSG) Get(cfgKey string, cas uint64) (
	[]byte, uint64, error) {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Get, key: %s, cas: %d", cfgKey, cas)
	bucketKey := sgCfgBucketKey(cfgKey)
	var value []byte
	casOut, err := c.bucket.Get(bucketKey, &value)
	if err != nil && err != gocb.ErrKeyNotFound {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Get, key: %s, cas: %d, err: %v", cfgKey, cas, err)
		return nil, 0, err
	}

	if cas != 0 && uint64(casOut) != cas {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Get, CasError key: %s, cas: %d", cfgKey, cas)
		return nil, 0, &cbgt.CfgCASError{}
	}

	return value, uint64(casOut), nil
}

func (c *CfgSG) Set(cfgKey string, val []byte, cas uint64) (uint64, error) {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set, key: %s, cas: %d", cfgKey, cas)
	var casOut gocb.Cas
	var err error
	bucketKey := sgCfgBucketKey(cfgKey)
	if cas == 0 {
		casOut, err = c.bucket.Insert(bucketKey, val, 0)
	} else {
		casOut, err = c.bucket.Replace(bucketKey, val, gocb.Cas(cas), 0)
	}

	if err == gocb.ErrKeyExists {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set, ErrKeyExists key: %s, cas: %d", cfgKey, cas)
		return 0, &cbgt.CfgCASError{}
	} else if err != nil {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set Error key: %s, cas: %d err:%s", cfgKey, cas, err)
		return 0, err
	}

	c.FireEvent(cfgKey, uint64(casOut), nil)
	return uint64(casOut), nil
}

func (c *CfgSG) Del(cfgKey string, cas uint64) error {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Del, key: %s, cas: %d", cfgKey, cas)
	bucketKey := sgCfgBucketKey(cfgKey)
	casOut, err := c.bucket.Remove(bucketKey, gocb.Cas(cas))
	if err != nil && err != gocb.ErrKeyNotFound {
		return err
	}

	c.FireEvent(cfgKey, uint64(casOut), nil)
	return err
}

func (c *CfgSG) Subscribe(cfgKey string, ch chan cbgt.CfgEvent) error {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Subscribe, key: %s", cfgKey)
	c.lock.Lock()
	a, exists := c.subscriptions[cfgKey]
	if !exists || a == nil {
		a = make([]chan<- cbgt.CfgEvent, 0)
	}
	c.subscriptions[cfgKey] = append(a, ch)

	c.lock.Unlock()

	return nil
}

func (c *CfgSG) FireEvent(cfgKey string, cas uint64, err error) {
	c.lock.Lock()
	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: FireEvent, key: %s, cas %d", cfgKey, cas)
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

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Refresh")
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
