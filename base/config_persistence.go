package base

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

// ConfigPersistence manages the underlying storage of database config documents in the bucket.
// Implementations support using either document body or xattr for storage
type ConfigPersistence interface {
	// Operations for interacting with raw config ([]byte).  gocb.Cas values represent document cas,
	// cfgCas represent the cas value associated with the last mutation, and may not match document CAS
	loadRawConfig(c *gocb.Collection, key string) ([]byte, gocb.Cas, error)
	removeRawConfig(c *gocb.Collection, key string, cas gocb.Cas) (gocb.Cas, error)
	replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (casOut gocb.Cas, cfgCas uint64, err error)

	// Operations for interacting with marshalled config. cfgCas represents the cas value
	// associated with the last config mutation, and may not match document CAS
	loadConfig(c *gocb.Collection, key string, valuePtr interface{}) (cfgCas uint64, err error)
	insertConfig(c *gocb.Collection, key string, value interface{}) (cfgCas uint64, err error)
}

// System xattr persistence
type XattrBootstrapPersistence struct {
}

const cfgXattrKey = "_sync"
const cfgXattrConfigPath = cfgXattrKey + ".config"
const cfgXattrCasPath = cfgXattrKey + ".cas"
const cfgXattrBody = `{"cfgVersion": 1}`

func (xbp *XattrBootstrapPersistence) insertConfig(c *gocb.Collection, key string, value interface{}) (cas uint64, err error) {

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(cfgXattrConfigPath, value, UpsertSpecXattr),
		gocb.UpsertSpec(cfgXattrCasPath, gocb.MutationMacroCAS, UpsertSpecXattr),
		gocb.ReplaceSpec("", json.RawMessage(cfgXattrBody), nil),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsInsert,
	}
	result, mutateErr := c.MutateIn(key, mutateOps, options)
	if isKVError(mutateErr, memd.StatusKeyExists) {
		return 0, ErrAlreadyExists
	}
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil

}

func (xbp *XattrBootstrapPersistence) loadRawConfig(c *gocb.Collection, key string) ([]byte, gocb.Cas, error) {

	var rawValue []byte
	cas, err := xbp.loadConfig(c, key, &rawValue)
	return rawValue, gocb.Cas(cas), err

}

func (xbp *XattrBootstrapPersistence) removeRawConfig(c *gocb.Collection, key string, cas gocb.Cas) (gocb.Cas, error) {

	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec(cfgXattrKey, RemoveSpecXattr),
		gocb.RemoveSpec("", nil),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Cas:           cas,
	}
	result, mutateErr := c.MutateIn(key, mutateOps, options)
	if mutateErr == nil {
		return result.Cas(), nil
	}

	// StatusKeyNotFound returned if document doesn't exist
	if errors.Is(mutateErr, gocbcore.ErrDocumentNotFound) {
		return 0, ErrNotFound
	}

	// StatusSubDocBadMulti returned if xattr doesn't exist
	if isKVError(mutateErr, memd.StatusSubDocBadMulti) {
		return 0, ErrNotFound
	}
	return 0, mutateErr

}

func (xbp *XattrBootstrapPersistence) replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (gocb.Cas, uint64, error) {
	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(cfgXattrConfigPath, bytesToRawMessage(value), UpsertSpecXattr),
		gocb.UpsertSpec(cfgXattrCasPath, gocb.MutationMacroCAS, UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Cas:           cas,
	}
	result, mutateErr := c.MutateIn(key, mutateOps, options)
	if mutateErr != nil {
		return 0, 0, mutateErr
	}
	return result.Cas(), uint64(result.Cas()), nil
}

// loadConfig returns the cas associated with the last cfg change (xattr._sync.cas)
func (xbp *XattrBootstrapPersistence) loadConfig(c *gocb.Collection, key string, valuePtr interface{}) (cas uint64, err error) {

	ops := []gocb.LookupInSpec{
		gocb.GetSpec(cfgXattrConfigPath, GetSpecXattr),
		gocb.GetSpec(cfgXattrCasPath, GetSpecXattr),
	}
	lookupOpts := &gocb.LookupInOptions{
		Timeout: time.Second * 10,
	}
	lookupOpts.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	res, lookupErr := c.LookupIn(key, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		xattrContErr := res.ContentAt(0, valuePtr)
		if xattrContErr != nil {
			Debugf(KeyCRUD, "No xattr config found for key=%s, path=%s: %v", key, cfgXattrConfigPath, xattrContErr)
			return 0, ErrNotFound
		}

		var strCas string
		xattrCasErr := res.ContentAt(1, &strCas)
		if xattrCasErr != nil {
			Debugf(KeyCRUD, "No xattr cas found for key=%s, path=%s: %v", key, cfgXattrCasPath, xattrContErr)
			return 0, ErrNotFound
		}
		cfgCas := HexCasToUint64(strCas)
		return cfgCas, nil
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		Debugf(KeyCRUD, "No config document found for key=%s", key)
		return 0, ErrNotFound
	} else {
		return 0, lookupErr
	}
}

// Document Body persistence stores config in the document body.
// cfgCas is just document cas
type DocumentBootstrapPersistence struct {
}

func (dbp *DocumentBootstrapPersistence) loadRawConfig(c *gocb.Collection, key string) ([]byte, gocb.Cas, error) {
	res, err := c.Get(key, &gocb.GetOptions{
		Transcoder: gocb.NewRawJSONTranscoder(),
	})
	if err != nil {
		return nil, 0, err
	}

	var bucketValue []byte
	err = res.Content(&bucketValue)
	if err != nil {
		return nil, 0, err
	}

	return bucketValue, res.Cas(), err
}

func (dbp *DocumentBootstrapPersistence) removeRawConfig(c *gocb.Collection, key string, cas gocb.Cas) (gocb.Cas, error) {
	deleteRes, err := c.Remove(key, &gocb.RemoveOptions{Cas: cas})
	if err != nil {
		return 0, err
	}
	return deleteRes.Cas(), err
}

func (dbp *DocumentBootstrapPersistence) replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (gocb.Cas, uint64, error) {
	replaceRes, err := c.Replace(key, value, &gocb.ReplaceOptions{Transcoder: gocb.NewRawJSONTranscoder(), Cas: cas})
	if err != nil {
		return 0, 0, err
	}

	// For DocumentBootstrapPersistence, cfgCas always equals doc.cas
	return replaceRes.Cas(), uint64(replaceRes.Cas()), nil
}

func (dbp *DocumentBootstrapPersistence) loadConfig(c *gocb.Collection, key string, valuePtr interface{}) (cas uint64, err error) {

	res, err := c.Get(key, &gocb.GetOptions{
		Timeout:       time.Second * 10,
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
	})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	err = res.Content(valuePtr)
	if err != nil {
		return 0, err
	}

	return uint64(res.Cas()), nil
}

func (dbp *DocumentBootstrapPersistence) insertConfig(c *gocb.Collection, key string, value interface{}) (cas uint64, err error) {
	res, err := c.Insert(key, value, nil)
	if err != nil {
		if isKVError(err, memd.StatusKeyExists) {
			return 0, ErrAlreadyExists
		}
		return 0, err
	}

	return uint64(res.Cas()), nil
}
