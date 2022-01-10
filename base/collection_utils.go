package base

import (
	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
)

// Fills in the GoCB UpsertOptions based on the passed in SG Bucket UpsertOptions
func fillUpsertOptions(goCBUpsertOptions *gocb.UpsertOptions, upsertOptions *sgbucket.UpsertOptions) {
	if upsertOptions == nil {
		return
	}
	goCBUpsertOptions.PreserveExpiry = upsertOptions.PreserveExpiry
}
