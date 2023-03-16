/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import "github.com/couchbase/sync_gateway/document"

// Declare aliases for various types & constants that were moved to document,
// to reduce the amount of code touched throughout the project:

type AttachmentsMeta = document.AttachmentsMeta
type AttachmentStorageMeta = document.AttachmentStorageMeta
type Body = document.Body
type ChannelSetEntry = document.ChannelSetEntry
type DocAttachment = document.DocAttachment
type Document = document.Document
type DocumentRevision = document.DocumentRevision
type DocumentUnmarshalLevel = document.DocumentUnmarshalLevel
type IDAndRev = document.IDAndRev
type Revisions = document.Revisions
type RevInfo = document.RevInfo
type RevKey = document.RevKey
type RevTree = document.RevTree
type SyncData = document.SyncData
type UserAccessMap = document.UserAccessMap

const (
	AttVersion1 = document.AttVersion1
	AttVersion2 = document.AttVersion2

	BodyDeleted        = document.BodyDeleted
	BodyRev            = document.BodyRev
	BodyId             = document.BodyId
	BodyRevisions      = document.BodyRevisions
	BodyAttachments    = document.BodyAttachments
	BodyPurged         = document.BodyPurged
	BodyExpiry         = document.BodyExpiry
	BodyRemoved        = document.BodyRemoved
	BodyInternalPrefix = document.BodyInternalPrefix

	DocUnmarshalAll       = document.DocUnmarshalAll
	DocUnmarshalSync      = document.DocUnmarshalSync
	DocUnmarshalNoHistory = document.DocUnmarshalNoHistory
	DocUnmarshalRev       = document.DocUnmarshalRev
	DocUnmarshalCAS       = document.DocUnmarshalCAS
	DocUnmarshalNone      = document.DocUnmarshalNone

	RevCacheIncludeDelta = document.RevCacheIncludeDelta
	RevCacheOmitDelta    = document.RevCacheOmitDelta

	RevisionsStart = document.RevisionsStart
	RevisionsIds   = document.RevisionsIds
)

func ParseRevID(revid string) (int, string) { return document.ParseRevID(revid) }
