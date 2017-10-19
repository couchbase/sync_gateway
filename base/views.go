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
