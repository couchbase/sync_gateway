//go:build cb_sg_enterprise
// +build cb_sg_enterprise

/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

const (
	productEditionEnterprise = true
	productEditionShortName  = "EE"

	DefaultAutoImport = true // Whether Sync Gateway should auto-import docs, if not specified in the config
)
