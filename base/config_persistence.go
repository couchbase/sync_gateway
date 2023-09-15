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
	loadRawConfig(ctx context.Context, c *gocb.Collection, key string) ([]byte, gocb.Cas, error)
	removeRawConfig(c *gocb.Collection, key string, cas gocb.Cas) (gocb.Cas, error)
	replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (casOut gocb.Cas, err error)

	// Operations for interacting with marshalled config. cfgCas represents the cas value
	// associated with the last config mutation, and may not match document CAS
	loadConfig(ctx context.Context, c *gocb.Collection, key string, valuePtr interface{}) (cfgCas uint64, err error)
	insertConfig(c *gocb.Collection, key string, value interface{}) (cfgCas uint64, err error)

	// touchConfigRollback sets the specific property to the specified string value via a subdoc operation.
	// Used to change to the cas value during rollback, to guard against races with slow updates
	touchConfigRollback(c *gocb.Collection, key string, property string, value string, cas gocb.Cas) (casOut gocb.Cas, err error)

	// keyExists checks whether the specified key exists in the collection
	keyExists(c *gocb.Collection, key string) (found bool, err error)
}

var _ ConfigPersistence = &XattrBootstrapPersistence{}
var _ ConfigPersistence = &DocumentBootstrapPersistence{}

// System xattr persistence
type XattrBootstrapPersistence struct {
	CommonBootstrapPersistence
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

func (xbp *XattrBootstrapPersistence) touchConfigRollback(c *gocb.Collection, key, property, value string, cas gocb.Cas) (casOut gocb.Cas, err error) {
	xattrProperty := cfgXattrKey + "." + property
	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrProperty, value, UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Cas:           cas,
	}
	result, mutateErr := c.MutateIn(key, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas(), nil
}

// loadRawConfig returns the config and document cas (not cfgCas).  Does not restore deleted documents,
// to avoid cas collisions with concurrent updates
func (xbp *XattrBootstrapPersistence) loadRawConfig(ctx context.Context, c *gocb.Collection, key string) ([]byte, gocb.Cas, error) {

	var rawValue []byte
	ops := []gocb.LookupInSpec{
		gocb.GetSpec(cfgXattrConfigPath, GetSpecXattr),
	}
	lookupOpts := &gocb.LookupInOptions{
		Timeout: time.Second * 10,
	}
	lookupOpts.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	res, lookupErr := c.LookupIn(key, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		// config
		xattrContErr := res.ContentAt(0, &rawValue)
		if xattrContErr != nil {
			DebugfCtx(ctx, KeyCRUD, "No xattr config found for key=%s, path=%s: %v", key, cfgXattrConfigPath, xattrContErr)
			return rawValue, 0, ErrNotFound
		}
		return rawValue, res.Cas(), nil
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		DebugfCtx(ctx, KeyCRUD, "No config document found for key=%s", key)
		return rawValue, 0, ErrNotFound
	} else {
		return rawValue, 0, lookupErr
	}
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

func (xbp *XattrBootstrapPersistence) replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (gocb.Cas, error) {
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
		return 0, mutateErr
	}
	return result.Cas(), nil
}

// loadConfig returns the cas associated with the last cfg change (xattr._sync.cas).  If a deleted document body is
// detected, recreates the document to avoid metadata purge
func (xbp *XattrBootstrapPersistence) loadConfig(ctx context.Context, c *gocb.Collection, key string, valuePtr interface{}) (cas uint64, err error) {

	ops := []gocb.LookupInSpec{
		gocb.GetSpec(cfgXattrConfigPath, GetSpecXattr),
		gocb.GetSpec(cfgXattrCasPath, GetSpecXattr),
		gocb.GetSpec("", &gocb.GetSpecOptions{}),
	}
	lookupOpts := &gocb.LookupInOptions{
		Timeout: time.Second * 10,
	}
	lookupOpts.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	res, lookupErr := c.LookupIn(key, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		// config
		xattrContErr := res.ContentAt(0, valuePtr)
		if xattrContErr != nil {
			DebugfCtx(ctx, KeyCRUD, "No xattr config found for key=%s, path=%s: %v", key, cfgXattrConfigPath, xattrContErr)
			return 0, ErrNotFound
		}

		// cas
		var strCas string
		xattrCasErr := res.ContentAt(1, &strCas)
		if xattrCasErr != nil {
			DebugfCtx(ctx, KeyCRUD, "No xattr cas found for key=%s, path=%s: %v", key, cfgXattrCasPath, xattrContErr)
			return 0, ErrNotFound
		}
		cfgCas := HexCasToUint64(strCas)

		// deleted document check - if deleted, restore
		var body map[string]interface{}
		bodyErr := res.ContentAt(2, &body)
		if bodyErr != nil {
			restoreErr := xbp.restoreDocumentBody(c, key, valuePtr, strCas)
			if restoreErr != nil {
				WarnfCtx(ctx, "Error attempting to restore unexpected deletion of config: %v", restoreErr)
			}
		}
		return cfgCas, nil
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		DebugfCtx(ctx, KeyCRUD, "No config document found for key=%s", key)
		return 0, ErrNotFound
	} else {
		return 0, lookupErr
	}
}

// Restore a deleted document's body.  Rewrites metadata, but preserves previous cfgCas
func (xbp *XattrBootstrapPersistence) restoreDocumentBody(c *gocb.Collection, key string, value interface{}, cfgCas string) error {
	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(cfgXattrConfigPath, value, UpsertSpecXattr),
		gocb.UpsertSpec(cfgXattrCasPath, cfgCas, UpsertSpecXattr),
		gocb.ReplaceSpec("", json.RawMessage(cfgXattrBody), nil),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsInsert,
	}
	_, mutateErr := c.MutateIn(key, mutateOps, options)
	if isKVError(mutateErr, memd.StatusKeyExists) {
		return ErrAlreadyExists
	}
	if mutateErr != nil {
		return mutateErr
	}
	return nil
}

// Document Body persistence stores config in the document body.
// cfgCas is just document cas
type DocumentBootstrapPersistence struct {
	CommonBootstrapPersistence
}

func (dbp *DocumentBootstrapPersistence) loadRawConfig(_ context.Context, c *gocb.Collection, key string) ([]byte, gocb.Cas, error) {
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

func (dbp *DocumentBootstrapPersistence) replaceRawConfig(c *gocb.Collection, key string, value []byte, cas gocb.Cas) (gocb.Cas, error) {
	replaceRes, err := c.Replace(key, value, &gocb.ReplaceOptions{Transcoder: gocb.NewRawJSONTranscoder(), Cas: cas})
	if err != nil {
		return 0, err
	}

	// For DocumentBootstrapPersistence, cfgCas always equals doc.cas
	return replaceRes.Cas(), nil
}

func (dbp *DocumentBootstrapPersistence) loadConfig(_ context.Context, c *gocb.Collection, key string, valuePtr interface{}) (cas uint64, err error) {

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

func (dbp *DocumentBootstrapPersistence) touchConfigRollback(c *gocb.Collection, key, property, value string, cas gocb.Cas) (casOut gocb.Cas, err error) {
	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(property, value, UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Cas:           cas,
	}
	result, mutateErr := c.MutateIn(key, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas(), nil
}

// Common operations that don't depend on storage format
type CommonBootstrapPersistence struct {
}

// Check whether the specified key exists.  Ignores format of stored data
func (cbp *CommonBootstrapPersistence) keyExists(c *gocb.Collection, key string) (bool, error) {
	res, err := c.Exists(key, nil)
	if err != nil {
		return false, err
	}
	return res.Exists(), nil
}
