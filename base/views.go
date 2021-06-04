/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

const (
	ViewQueryParamStale         = "stale"
	ViewQueryParamReduce        = "reduce"
	ViewQueryParamStartKey      = "startkey"
	ViewQueryParamEndKey        = "endkey"
	ViewQueryParamInclusiveEnd  = "inclusive_end"
	ViewQueryParamLimit         = "limit"
	ViewQueryParamIncludeDocs   = "include_docs" // Ignored -- see https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
	ViewQueryParamDescending    = "descending"
	ViewQueryParamGroup         = "group"
	ViewQueryParamSkip          = "skip"
	ViewQueryParamGroupLevel    = "group_level"
	ViewQueryParamStartKeyDocId = "startkey_docid"
	ViewQueryParamEndKeyDocId   = "endkey_docid"
	ViewQueryParamKey           = "key"
	ViewQueryParamKeys          = "keys"
)
