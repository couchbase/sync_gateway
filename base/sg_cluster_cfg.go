package base

import (
	"context"
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
}

var ErrCfgCasError = &cbgt.CfgCASError{}

// NewCfgSG returns a Cfg implementation that reads/writes its entries
// from/to a couchbase bucket, using DCP streams to subscribe to
// changes.
//
// urlStr: single URL or multiple URLs delimited by ';'
// bucket: couchbase bucket name
func NewCfgSG(bucket Bucket) (*CfgSG, error) {

	cfgContextID := MD(bucket.GetName()).Redact() + "-cfgSG"
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
	if err != nil && !IsKeyNotFoundError(c.bucket, err) {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Get, key: %s, cas: %d, err: %v", cfgKey, cas, err)
		return nil, 0, err
	}

	if cas != 0 && casOut != cas {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Get, CasError key: %s, cas: %d", cfgKey, cas)
		return nil, 0, ErrCfgCasError
	}

	return value, casOut, nil
}

func (c *CfgSG) Set(cfgKey string, val []byte, cas uint64) (uint64, error) {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set, key: %s, cas: %d", cfgKey, cas)
	var err error
	bucketKey := sgCfgBucketKey(cfgKey)

	casOut, err := c.bucket.WriteCas(bucketKey, 0, 0, cas, val, 0)

	if IsCasMismatch(err) {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set, ErrKeyExists key: %s, cas: %d", cfgKey, cas)
		return 0, ErrCfgCasError
	} else if err != nil {
		InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Set Error key: %s, cas: %d err:%s", cfgKey, cas, err)
		return 0, err
	}

	c.FireEvent(cfgKey, casOut, nil)
	return casOut, nil
}

func (c *CfgSG) Del(cfgKey string, cas uint64) error {

	InfofCtx(c.loggingCtx, KeyDCP, "cfg_sg: Del, key: %s, cas: %d", cfgKey, cas)
	bucketKey := sgCfgBucketKey(cfgKey)
	casOut, err := c.bucket.Remove(bucketKey, cas)
	if IsCasMismatch(err) {
		return ErrCfgCasError
	} else if err != nil && !IsKeyNotFoundError(c.bucket, err) {
		return err
	}

	c.FireEvent(cfgKey, casOut, nil)
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
