// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"github.com/couchbase/cbgt"
)

// NewCbgtCfgMem runs cbgt.NewCfgMem and sets the matching version number we expect for Sync Gateway.
func NewCbgtCfgMem() (*cbgt.CfgMem, error) {
	cfg := cbgt.NewCfgMem()
	_, err := cfg.Set(cbgt.VERSION_KEY, []byte(SGCbgtMetadataVersion), 0)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
