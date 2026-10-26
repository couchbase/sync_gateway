/*
Copyright 2026-Present Couchbase, Inc.
Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import "context"

var _ BootstrapConnection = &LeakyBootstrapConnection{}

// LeakyBootstrapConnection is a wrapper around a BootstrapConnection to support forced errors. For testing use only.
type LeakyBootstrapConnection struct {
	BootstrapConnection
	config LeakyBootstrapConnectionConfig
}

// LeakyBootstrapConnectionConfig configures the hooks of a LeakyBootstrapConnection.
type LeakyBootstrapConnectionConfig struct {
	// BeforeDeleteMetadataDocument runs before DeleteMetadataDocument, to simulate a concurrent writer.
	BeforeDeleteMetadataDocument func(bucket, key string)
	// AfterDeleteMetadataDocument runs after DeleteMetadataDocument, to simulate a concurrent writer.
	AfterDeleteMetadataDocument func(bucket, key string)
	// KVOpCallback runs after every method that makes KV operations, so tests can see which KV work a node does.
	// kvOps is the number of KV operations the method reported, or 1 for a method that does not report a count. key
	// is empty for a method that is not about one document.
	KVOpCallback func(op, bucket, key string, kvOps int)
}

// NewLeakyBootstrapConnection creates a wrapper around a BootstrapConnection to support forced errors.
func NewLeakyBootstrapConnection(conn BootstrapConnection, config LeakyBootstrapConnectionConfig) *LeakyBootstrapConnection {
	return &LeakyBootstrapConnection{
		BootstrapConnection: conn,
		config:              config,
	}
}

func (c *LeakyBootstrapConnection) kvOp(op, bucket, key string, kvOps int) {
	if c.config.KVOpCallback != nil {
		c.config.KVOpCallback(op, bucket, key, kvOps)
	}
}

func (c *LeakyBootstrapConnection) GetMetadataDocument(ctx context.Context, bucket, key string, valuePtr any) (cas uint64, kvOps int, err error) {
	cas, kvOps, err = c.BootstrapConnection.GetMetadataDocument(ctx, bucket, key, valuePtr)
	c.kvOp("GetMetadataDocument", bucket, key, kvOps)
	return cas, kvOps, err
}

func (c *LeakyBootstrapConnection) InsertMetadataDocument(ctx context.Context, bucket, key string, value any) (newCAS uint64, kvOps int, err error) {
	newCAS, kvOps, err = c.BootstrapConnection.InsertMetadataDocument(ctx, bucket, key, value)
	c.kvOp("InsertMetadataDocument", bucket, key, kvOps)
	return newCAS, kvOps, err
}

func (c *LeakyBootstrapConnection) DeleteMetadataDocument(ctx context.Context, bucket, key string, cas uint64) error {
	if c.config.BeforeDeleteMetadataDocument != nil {
		c.config.BeforeDeleteMetadataDocument(bucket, key)
	}
	err := c.BootstrapConnection.DeleteMetadataDocument(ctx, bucket, key, cas)
	c.kvOp("DeleteMetadataDocument", bucket, key, 1)
	if c.config.AfterDeleteMetadataDocument != nil {
		c.config.AfterDeleteMetadataDocument(bucket, key)
	}
	return err
}

func (c *LeakyBootstrapConnection) UpdateMetadataDocument(ctx context.Context, bucket, key string, updateCallback func(rawBucketConfig []byte, rawBucketConfigCas uint64) (updatedConfig []byte, err error)) (newCAS uint64, err error) {
	newCAS, err = c.BootstrapConnection.UpdateMetadataDocument(ctx, bucket, key, updateCallback)
	c.kvOp("UpdateMetadataDocument", bucket, key, 1)
	return newCAS, err
}

func (c *LeakyBootstrapConnection) WriteMetadataDocument(ctx context.Context, bucket, key string, cas uint64, valuePtr any) (casOut uint64, kvOps int, err error) {
	casOut, kvOps, err = c.BootstrapConnection.WriteMetadataDocument(ctx, bucket, key, cas, valuePtr)
	c.kvOp("WriteMetadataDocument", bucket, key, kvOps)
	return casOut, kvOps, err
}

func (c *LeakyBootstrapConnection) TouchMetadataDocument(ctx context.Context, bucket, key string, property string, value string, cas uint64) (casOut uint64, kvOps int, err error) {
	casOut, kvOps, err = c.BootstrapConnection.TouchMetadataDocument(ctx, bucket, key, property, value, cas)
	c.kvOp("TouchMetadataDocument", bucket, key, kvOps)
	return casOut, kvOps, err
}

func (c *LeakyBootstrapConnection) KeyExists(ctx context.Context, bucket, key string) (exists bool, err error) {
	exists, err = c.BootstrapConnection.KeyExists(ctx, bucket, key)
	c.kvOp("KeyExists", bucket, key, 1)
	return exists, err
}

func (c *LeakyBootstrapConnection) GetDocument(ctx context.Context, bucket, docID string, rv any) (exists bool, err error) {
	exists, err = c.BootstrapConnection.GetDocument(ctx, bucket, docID, rv)
	c.kvOp("GetDocument", bucket, docID, 1)
	return exists, err
}

func (c *LeakyBootstrapConnection) GetRawDocument(ctx context.Context, bucket, docID string) (value []byte, exists bool, err error) {
	value, exists, err = c.BootstrapConnection.GetRawDocument(ctx, bucket, docID)
	c.kvOp("GetRawDocument", bucket, docID, 1)
	return value, exists, err
}

func (c *LeakyBootstrapConnection) GetMetadataMigrationStatus(ctx context.Context, bucket string) (status *MetadataMigrationStatus, cas uint64, kvOps int, err error) {
	status, cas, kvOps, err = c.BootstrapConnection.GetMetadataMigrationStatus(ctx, bucket)
	c.kvOp("GetMetadataMigrationStatus", bucket, MetadataMigrationStatusDocID, kvOps)
	return status, cas, kvOps, err
}

func (c *LeakyBootstrapConnection) InsertMetadataMigrationStatus(ctx context.Context, bucket string, status *MetadataMigrationStatus) (cas uint64, err error) {
	cas, err = c.BootstrapConnection.InsertMetadataMigrationStatus(ctx, bucket, status)
	c.kvOp("InsertMetadataMigrationStatus", bucket, MetadataMigrationStatusDocID, 1)
	return cas, err
}

func (c *LeakyBootstrapConnection) UpdateMetadataMigrationStatus(ctx context.Context, bucket string, callback func(*MetadataMigrationStatus) error) (newCAS uint64, err error) {
	newCAS, err = c.BootstrapConnection.UpdateMetadataMigrationStatus(ctx, bucket, callback)
	c.kvOp("UpdateMetadataMigrationStatus", bucket, MetadataMigrationStatusDocID, 1)
	return newCAS, err
}

func (c *LeakyBootstrapConnection) MigrateBootstrapDocs(ctx context.Context, bucket string, docIDs []string) error {
	err := c.BootstrapConnection.MigrateBootstrapDocs(ctx, bucket, docIDs)
	c.kvOp("MigrateBootstrapDocs", bucket, "", 1)
	return err
}

func (c *LeakyBootstrapConnection) RefreshBucketBootstrapTarget(ctx context.Context, bucket string) (kvOps int, err error) {
	kvOps, err = c.BootstrapConnection.RefreshBucketBootstrapTarget(ctx, bucket)
	c.kvOp("RefreshBucketBootstrapTarget", bucket, MetadataMigrationStatusDocID, kvOps)
	return kvOps, err
}

func (c *LeakyBootstrapConnection) SetBucketBootstrapTargetHint(ctx context.Context, bucket string, optInHint bool) error {
	err := c.BootstrapConnection.SetBucketBootstrapTargetHint(ctx, bucket, optInHint)
	c.kvOp("SetBucketBootstrapTargetHint", bucket, SGRegistryKey, 1)
	return err
}

func (c *LeakyBootstrapConnection) BucketBootstrapTargetIsSystemMobile(ctx context.Context, bucket string, optInHint bool) (bool, error) {
	isSystemMobile, err := c.BootstrapConnection.BucketBootstrapTargetIsSystemMobile(ctx, bucket, optInHint)
	c.kvOp("BucketBootstrapTargetIsSystemMobile", bucket, SGRegistryKey, 1)
	return isSystemMobile, err
}
