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
}

// NewLeakyBootstrapConnection creates a wrapper around a BootstrapConnection to support forced errors.
func NewLeakyBootstrapConnection(conn BootstrapConnection, config LeakyBootstrapConnectionConfig) *LeakyBootstrapConnection {
	return &LeakyBootstrapConnection{
		BootstrapConnection: conn,
		config:              config,
	}
}

func (c *LeakyBootstrapConnection) DeleteMetadataDocument(ctx context.Context, bucket, key string, cas uint64) error {
	if c.config.BeforeDeleteMetadataDocument != nil {
		c.config.BeforeDeleteMetadataDocument(bucket, key)
	}
	err := c.BootstrapConnection.DeleteMetadataDocument(ctx, bucket, key, cas)
	if c.config.AfterDeleteMetadataDocument != nil {
		c.config.AfterDeleteMetadataDocument(bucket, key)
	}
	return err
}
