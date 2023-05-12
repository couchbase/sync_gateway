/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import "github.com/couchbase/sync_gateway/documents"

// Declare aliases for some common types & constants that were moved to the `documents` package,
// to reduce the amount of code touched throughout the project:

type AttachmentsMeta = documents.AttachmentsMeta
type AttachmentStorageMeta = documents.AttachmentStorageMeta
type Body = documents.Body
type ChannelSetEntry = documents.ChannelSetEntry
type DocAttachment = documents.DocAttachment
type Document = documents.Document
type DocumentRevision = documents.DocumentRevision
type DocumentUnmarshalLevel = documents.DocumentUnmarshalLevel
type IDAndRev = documents.IDAndRev
type Revisions = documents.Revisions
type RevInfo = documents.RevInfo
type RevKey = documents.RevKey
type RevTree = documents.RevTree
type SyncData = documents.SyncData
type UserAccessMap = documents.UserAccessMap

const (
	AttVersion1 = documents.AttVersion1
	AttVersion2 = documents.AttVersion2

	BodyDeleted     = documents.BodyDeleted
	BodyRev         = documents.BodyRev
	BodyId          = documents.BodyId
	BodyRevisions   = documents.BodyRevisions
	BodyAttachments = documents.BodyAttachments
	BodyExpiry      = documents.BodyExpiry
	BodyRemoved     = documents.BodyRemoved

	DocUnmarshalAll  = documents.DocUnmarshalAll
	DocUnmarshalSync = documents.DocUnmarshalSync

	RevCacheIncludeDelta = documents.RevCacheIncludeDelta
	RevCacheOmitDelta    = documents.RevCacheOmitDelta

	RevisionsStart = documents.RevisionsStart
	RevisionsIds   = documents.RevisionsIds
)

func ParseRevID(revid string) (int, string) { return documents.ParseRevID(revid) }
