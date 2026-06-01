//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
)

// =====================================================================
// InvalidatePrincipals Background Manager
// =====================================================================

// InvalidatePrincipalsProcess implements the principal invalidation and syncInfo update steps that follow a resync.
type InvalidatePrincipalsProcess struct {
	db                  *DatabaseContext
	regenerateSequences bool
	hasAllCollections   bool
	docsChanged         int64
	collectionIDs       []uint32
}

var _ BackgroundManagerProcessI = &InvalidatePrincipalsProcess{}

// newInvalidatePrincipalsManager returns a local BackgroundManager that runs the principal invalidation process
// with the given parameters.
func newInvalidatePrincipalsManager(db *DatabaseContext, regenerateSequences bool, hasAllCollections bool, docsChanged int64, collectionIDs []uint32) *BackgroundManager {
	return &BackgroundManager{
		name: "invalidate_principals",
		Process: &InvalidatePrincipalsProcess{
			db:                  db,
			regenerateSequences: regenerateSequences,
			hasAllCollections:   hasAllCollections,
			docsChanged:         docsChanged,
			collectionIDs:       collectionIDs,
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (p *InvalidatePrincipalsProcess) Init(_ context.Context, _ map[string]any, _ []byte) error {
	return nil
}

func (p *InvalidatePrincipalsProcess) Run(ctx context.Context, _ map[string]any, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if terminator.IsClosed() {
		return nil
	}
	if err := invalidatePrincipals(ctx, p.db, p.regenerateSequences, p.hasAllCollections, p.docsChanged); err != nil {
		return err
	}
	if terminator.IsClosed() {
		return nil
	}
	if p.regenerateSequences {
		updateSyncInfo(ctx, p.db, p.collectionIDs)
	}
	return nil
}

func (p *InvalidatePrincipalsProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) ([]byte, []byte, error) {
	out, err := base.JSONMarshal(status)
	return out, nil, err
}

func (p *InvalidatePrincipalsProcess) SetProcessStatus(_ context.Context, _, _ []byte) {}

func (p *InvalidatePrincipalsProcess) ResetStatus() {}
