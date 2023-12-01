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

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
)

// Fills in the GoCB UpsertOptions based on the passed in SG Bucket UpsertOptions
func fillUpsertOptions(ctx context.Context, goCBUpsertOptions *gocb.UpsertOptions, upsertOptions *sgbucket.UpsertOptions) {
	if upsertOptions == nil {
		return
	}
	goCBUpsertOptions.PreserveExpiry = upsertOptions.PreserveExpiry
	if goCBUpsertOptions.Expiry != 0 && upsertOptions.PreserveExpiry {
		InfofCtx(ctx, KeyCRUD, "Expiry set on gocb.UpsertOptions, but sgbucket.UpsertOptions.PreserveExpiry is false. Force setting PreserveExpiry to false to allow write to proceed.")
		goCBUpsertOptions.PreserveExpiry = false

	}
}

// Fills in the GoCB MutateInOptions based on the passed in MutateInOptions
func fillMutateInOptions(ctx context.Context, goCBMutateInOptions *gocb.MutateInOptions, mutateInOptions *sgbucket.MutateInOptions) {
	if mutateInOptions == nil {
		return
	}
	goCBMutateInOptions.PreserveExpiry = mutateInOptions.PreserveExpiry
	if goCBMutateInOptions.Expiry != 0 && mutateInOptions.PreserveExpiry {
		InfofCtx(ctx, KeyCRUD, "Expiry set on gocb.MutateInOptions, but sgbucket.MutateInOptions.PreserveExpiry is false. Force setting PreserveExpiry to false to allow write to proceed.")
		goCBMutateInOptions.PreserveExpiry = false

	}

}
