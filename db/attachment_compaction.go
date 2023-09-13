// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	CompactionIDKey = "compactID"
	MarkPhase       = "mark"
	SweepPhase      = "sweep"
	CleanupPhase    = "cleanup"
)

func attachmentCompactMarkPhase(ctx context.Context, dataStore base.DataStore, collectionID uint32, db *Database, compactionID string, terminator *base.SafeTerminator, markedAttachmentCount *base.AtomicInt) (count int64, vbUUIDs []uint64, checkpointPrefix string, err error) {
	base.InfofCtx(ctx, base.KeyAll, "Starting first phase of attachment compaction (mark phase) with compactionID: %q", compactionID)
	compactionLoggingID := "Compaction Mark: " + compactionID

	var markProcessFailureErr error

	// failProcess used when a failure is deemed as 'un-recoverable' and we need to abort the compaction process.
	failProcess := func(err error, format string, args ...interface{}) bool {
		markProcessFailureErr = err
		terminator.Close()
		base.WarnfCtx(ctx, format, args...)
		return false
	}

	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", compactionLoggingID, event.Opcode, base.UD(docID))

		// We've had an error previously so no point doing work for any remaining items
		if markProcessFailureErr != nil {
			return false
		}

		// Don't want to process raw binary docs
		// The binary check should suffice but for additional safety also check for empty bodies
		if event.DataType == base.MemcachedDataTypeRaw || len(event.Value) == 0 {
			return true
		}

		// We only want to process full docs. Not any sync docs.
		if strings.HasPrefix(docID, base.SyncDocPrefix) {
			return true
		}

		// We need to mark attachments in every leaf revision of the current doc
		// We will build up a list of attachment names which map to attachment doc IDs. Avoids doing multiple KV ops
		// when marking if multiple leaves are referencing the same attachment.
		attachmentKeys := make(map[string]string)
		attachmentData, err := getAttachmentSyncData(event.DataType, event.Value)
		if err != nil {
			return failProcess(err, "[%s] Failed to obtain required sync data from doc %s from feed. Err: %v", compactionID, base.UD(docID), err)
		}

		// Its possible a doc doesn't have sync data. If not a sync gateway doc we can skip it.
		if attachmentData == nil {
			return true
		}

		handleAttachments(attachmentKeys, docID, attachmentData.Attachments)

		// If we're in a conflict state we need to go and check and mark attachments from other leaves, not just winning
		if attachmentData.Flags&channels.Conflict != 0 {
			// Iterate over body map
			// These are strings containing conflicting bodies, need to scan these for attachments
			for _, bodyMap := range attachmentData.History.BodyMap {
				var body AttachmentsMetaMap
				err = base.JSONUnmarshal([]byte(bodyMap), &body)
				if err != nil {
					continue
				}

				handleAttachments(attachmentKeys, docID, body.Attachments)
			}

			// Iterate over body key map
			// These are strings containing IDs to documents containing conflicting bodies
			for _, bodyKey := range attachmentData.History.BodyKeyMap {
				bodyRaw, _, err := dataStore.GetRaw(bodyKey)
				if err != nil {
					if base.IsDocNotFoundError(err) {
						continue
					}
					return failProcess(err, "[%s] Unable to obtain document from %s bodyKeyMap with ID %s: %v", compactionID, base.UD(docID), base.UD(bodyKey), err)
				}

				var body AttachmentsMetaMap
				err = base.JSONUnmarshal(bodyRaw, &body)
				if err != nil {
					continue
				}

				handleAttachments(attachmentKeys, docID, body.Attachments)
			}
		}

		for attachmentName, attachmentDocID := range attachmentKeys {
			// Stamp the current compaction ID into the attachment xattr. This is performing the actual marking
			_, err = dataStore.SetXattr(attachmentDocID, getCompactionIDSubDocPath(compactionID), []byte(strconv.Itoa(int(time.Now().Unix()))))

			// If an error occurs while stamping in that ID we need to fail this process and then the entire compaction
			// process. Otherwise, an attachment could end up getting erroneously deleted in the later sweep phase.
			if err != nil {
				return failProcess(err, "[%s] Failed to mark attachment %s from doc %s with attachment docID %s. Err: %v", compactionLoggingID, base.UD(attachmentName), base.UD(docID), base.UD(attachmentDocID), err)
			}
			base.DebugfCtx(ctx, base.KeyAll, "[%s] Marked attachment %s from doc %s with attachment docID %s ; Event CAS: %s", compactionLoggingID, base.UD(attachmentName), base.UD(docID), base.UD(attachmentDocID), event.Cas)
			markedAttachmentCount.Add(1)
		}
		return true
	}

	clientOptions, err := getCompactionDCPClientOptions(collectionID, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))
	if err != nil {
		return 0, nil, "", err
	}

	base.InfofCtx(ctx, base.KeyAll, "[%s] Starting DCP feed for mark phase of attachment compaction", compactionLoggingID)

	dcpFeedKey := GenerateCompactionDCPStreamName(compactionID, MarkPhase)

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return 0, nil, "", err
	}

	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *clientOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create attachment compaction DCP client! %v", compactionLoggingID, err)
		return 0, nil, "", err
	}
	metadataKeyPrefix := dcpClient.GetMetadataKeyPrefix()

	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start attachment compaction DCP feed! %v", compactionLoggingID, err)
		_ = dcpClient.Close()
		return 0, nil, metadataKeyPrefix, err
	}
	base.DebugfCtx(ctx, base.KeyAll, "[%s] DCP feed started.", compactionLoggingID)

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "[%s] Mark phase of attachment compaction completed. Marked %d attachments", compactionLoggingID, markedAttachmentCount.Value())
		err = dcpClient.Close()
		if markProcessFailureErr != nil {
			return markedAttachmentCount.Value(), nil, metadataKeyPrefix, markProcessFailureErr
		}
	case <-terminator.Done():
		base.DebugfCtx(ctx, base.KeyAll, "[%s] Terminator closed. Stopping mark phase.", compactionLoggingID)
		err = dcpClient.Close()
		if markProcessFailureErr != nil {
			return markedAttachmentCount.Value(), nil, metadataKeyPrefix, markProcessFailureErr
		}
		if err != nil {
			return markedAttachmentCount.Value(), base.GetVBUUIDs(dcpClient.GetMetadata()), metadataKeyPrefix, err
		}

		err = <-doneChan
		if err != nil {
			return markedAttachmentCount.Value(), base.GetVBUUIDs(dcpClient.GetMetadata()), metadataKeyPrefix, err
		}

		base.InfofCtx(ctx, base.KeyAll, "[%s] Mark phase of attachment compaction was terminated. Marked %d attachments", compactionLoggingID, markedAttachmentCount.Value())
	}

	return markedAttachmentCount.Value(), base.GetVBUUIDs(dcpClient.GetMetadata()), metadataKeyPrefix, err
}

// AttachmentsMetaMap struct is a very minimal struct to unmarshal into when getting attachments from bodies
type AttachmentsMetaMap struct {
	Attachments map[string]AttachmentsMeta `json:"_attachments"`
}

// AttachmentCompactionData struct to unmarshal a document sync data into in order to process attachments during mark
// phase. Contains only what is necessary
type AttachmentCompactionData struct {
	Attachments map[string]AttachmentsMeta `json:"attachments"`
	Flags       uint8                      `json:"flags"`
	History     struct {
		BodyMap    map[string]string `json:"bodymap"`
		BodyKeyMap map[string]string `json:"BodyKeyMap"`
	} `json:"history"`
}

// getAttachmentSyncData takes the data type and data from the DCP feed and will return a AttachmentCompactionData
// struct containing data needed to process attachments on a document.
func getAttachmentSyncData(dataType uint8, data []byte) (*AttachmentCompactionData, error) {
	var attachmentData *AttachmentCompactionData
	var documentBody []byte

	if dataType&base.MemcachedDataTypeXattr != 0 {
		body, xattr, _, err := parseXattrStreamData(base.SyncXattrName, "", data)
		if err != nil {
			if errors.Is(err, base.ErrXattrNotFound) {
				return nil, nil
			}
			return nil, err
		}

		err = base.JSONUnmarshal(xattr, &attachmentData)
		if err != nil {
			return nil, err
		}
		documentBody = body

	} else {
		type AttachmentDataSync struct {
			AttachmentData AttachmentCompactionData `json:"_sync"`
		}
		var attachmentDataSync AttachmentDataSync
		err := base.JSONUnmarshal(data, &attachmentDataSync)
		if err != nil {
			return nil, err
		}

		documentBody = data
		attachmentData = &attachmentDataSync.AttachmentData
	}

	// If we've not yet found any attachments have a last effort attempt to grab it from the body for pre-2.5 documents
	if len(attachmentData.Attachments) == 0 {
		attachmentMetaMap, err := checkForInlineAttachments(documentBody)
		if err != nil {
			return nil, err
		}
		if attachmentMetaMap != nil {
			attachmentData.Attachments = attachmentMetaMap.Attachments
		}
	}

	return attachmentData, nil
}

// checkForInlineAttachments will scan a body for "_attachments" for pre-2.5 attachments and will return any attachments
// found
func checkForInlineAttachments(body []byte) (*AttachmentsMetaMap, error) {
	if bytes.Contains(body, []byte(BodyAttachments)) {
		var attachmentBody AttachmentsMetaMap
		err := base.JSONUnmarshal(body, &attachmentBody)
		if err != nil {
			return nil, err
		}
		return &attachmentBody, nil
	}

	return nil, nil
}

// handleAttachments will iterate over the provided attachments and add any attachment doc IDs to the provided map
// Doesn't require an error return as if we fail at any point in here the attachment is either not a v1 attachment, or
// is unreadable which is likely unrecoverable.
func handleAttachments(attachmentKeyMap map[string]string, docKey string, attachmentsMap map[string]AttachmentsMeta) {
	for attName, attachmentMeta := range attachmentsMap {
		attMetaMap := attachmentMeta

		attVer, ok := GetAttachmentVersion(attMetaMap)
		if !ok {
			continue
		}

		if attVer != AttVersion1 {
			continue
		}

		digest, ok := attMetaMap["digest"]
		if !ok {
			continue
		}

		attKey := MakeAttachmentKey(AttVersion1, docKey, digest.(string))
		attachmentKeyMap[attName] = attKey
	}
}

func attachmentCompactSweepPhase(ctx context.Context, dataStore base.DataStore, collectionID uint32, db *Database, compactionID string, vbUUIDs []uint64, dryRun bool, terminator *base.SafeTerminator, purgedAttachmentCount *base.AtomicInt) (int64, error) {
	base.InfofCtx(ctx, base.KeyAll, "Starting second phase of attachment compaction (sweep phase) with compactionID: %q", compactionID)
	compactionLoggingID := "Compaction Sweep: " + compactionID

	// Iterate over v1 attachments and if not marked with supplied compactionID we can purge the attachments.
	// In the event of an error we can return but continue - Worst case is an attachment which should be deleted won't
	// be deleted.
	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", compactionLoggingID, event.Opcode, base.UD(docID))

		// We only want to look over v1 attachment docs, skip otherwise
		if !strings.HasPrefix(docID, base.AttPrefix) {
			return true
		}

		// If the data contains an xattr then the attachment likely has a compaction ID, need to check this value
		if event.DataType&base.MemcachedDataTypeXattr != 0 {
			_, xattr, _, err := parseXattrStreamData(base.AttachmentCompactionXattrName, "", event.Value)
			if err != nil && !errors.Is(err, base.ErrXattrNotFound) {
				base.WarnfCtx(ctx, "[%s] Unexpected error occurred attempting to parse attachment xattr: %v", compactionLoggingID, err)
				return true
			}

			// If the document did indeed have an xattr then check the compactID. If it is the same as the current
			// running compaction ID we don't want to purge this doc and can continue to the next doc.
			if xattr != nil {
				var syncData map[string]interface{}
				err = base.JSONUnmarshal(xattr, &syncData)
				if err != nil {
					base.WarnfCtx(ctx, "[%s] Failed to unmarshal xattr data: %v", compactionLoggingID, err)
					return true
				}

				compactIDSync, compactIDSyncPresent := syncData[CompactionIDKey]
				if _, compactionIDPresent := compactIDSync.(map[string]interface{})[compactionID]; compactIDSyncPresent && compactionIDPresent {
					return true
				}
			}
		}

		// If we've reached this point the current v1 attachment being processed either:
		// - Has no compactionID set in its xattr
		// - Has a compactionID set in its xattr but it is from a previous run and therefore is not equal to the passed
		// in compactionID
		// Therefore, we want to purge the doc (unless running as dryRun mode)
		if !dryRun {
			base.TracefCtx(ctx, base.KeyAll, "[%s] Purging attachment %s", compactionLoggingID, base.UD(docID))
			_, err := dataStore.Remove(docID, event.Cas)
			if err != nil {
				base.WarnfCtx(ctx, "[%s] Unable to purge attachment %s: %v", compactionLoggingID, base.UD(docID), err)
				return true
			}
			base.DebugfCtx(ctx, base.KeyAll, "[%s] Purged attachment %s", compactionLoggingID, base.UD(docID))
			db.DbStats.Database().NumAttachmentsCompacted.Add(1)
		} else {
			base.DebugfCtx(ctx, base.KeyAll, "[%s] Would have purged attachment %s (not purged, running with dry run)", compactionLoggingID, base.UD(docID))
		}

		purgedAttachmentCount.Add(1)
		return true
	}

	clientOptions, err := getCompactionDCPClientOptions(collectionID, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))
	if err != nil {
		return 0, err
	}
	clientOptions.InitialMetadata = base.BuildDCPMetadataSliceFromVBUUIDs(vbUUIDs)

	dcpFeedKey := GenerateCompactionDCPStreamName(compactionID, SweepPhase)

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return 0, err
	}

	base.InfofCtx(ctx, base.KeyAll, "[%s] Starting DCP feed %q for sweep phase of attachment compaction", compactionLoggingID, dcpFeedKey)
	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *clientOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create attachment compaction DCP client! %v", compactionLoggingID, err)
		return 0, err
	}

	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start attachment compaction DCP feed! %v", compactionLoggingID, err)
		_ = dcpClient.Close()
		return 0, err
	}
	base.DebugfCtx(ctx, base.KeyAll, "[%s] DCP client started.", compactionLoggingID)

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "[%s] Sweep phase of attachment compaction completed. Deleted %d attachments", compactionLoggingID, purgedAttachmentCount.Value())
		err = dcpClient.Close()
	case <-terminator.Done():
		base.DebugfCtx(ctx, base.KeyAll, "[%s] Terminator closed. Ending sweep phase.", compactionLoggingID)
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment compaction DCP client! %v", compactionLoggingID, err)
			return purgedAttachmentCount.Value(), err
		}

		err = <-doneChan
		if err != nil {
			return purgedAttachmentCount.Value(), err
		}

		base.InfofCtx(ctx, base.KeyAll, "[%s] Sweep phase of attachment compaction was terminated. Deleted %d attachments", compactionLoggingID, purgedAttachmentCount.Value())
	}

	return purgedAttachmentCount.Value(), err
}

func attachmentCompactCleanupPhase(ctx context.Context, dataStore base.DataStore, collectionID uint32, db *Database, compactionID string, vbUUIDs []uint64, terminator *base.SafeTerminator) (string, error) {
	base.InfofCtx(ctx, base.KeyAll, "Starting third phase of attachment compaction (cleanup phase) with compactionID: %q", compactionID)
	compactionLoggingID := "Compaction Cleanup: " + compactionID

	callback := func(event sgbucket.FeedEvent) bool {

		docID := string(event.Key)

		if !strings.HasPrefix(docID, base.AttPrefix) {
			return true
		}

		if event.DataType&base.MemcachedDataTypeXattr == 0 {
			return true
		}

		_, xattr, _, err := parseXattrStreamData(base.AttachmentCompactionXattrName, "", event.Value)
		if err != nil && !errors.Is(err, base.ErrXattrNotFound) {
			base.WarnfCtx(ctx, "[%s] Unexpected error occurred attempting to parse attachment xattr: %v", compactionLoggingID, err)
			return true
		}

		if xattr != nil {
			// TODO: Struct map
			var attachmentCompactionMetadata map[string]map[string]interface{}
			err = base.JSONUnmarshal(xattr, &attachmentCompactionMetadata)
			if err != nil {
				base.WarnfCtx(ctx, "[%s] Failed to unmarshal attachment compaction xattr: %v", compactionLoggingID, err)
				return true
			}

			// Get compactID map containing all compactIDs on the document, if one is not present for some reason we can
			// skip this
			compactIDSyncMap, compactIDSyncPresent := attachmentCompactionMetadata[CompactionIDKey]
			if !compactIDSyncPresent {
				return true
			}

			// Build up a set of compactionIDs that we can remove from the xattr. We always add the current
			// compaction ID as we're now done with it. Also check if any other compaction IDs are present. If any are
			// older than 30 days we can remove them.
			toDeleteCompactIDPaths := []string{getCompactionIDSubDocPath(compactionID)}
			for compactID, compactIDTimestampI := range compactIDSyncMap {
				if compactID == compactionID {
					continue
				}

				compactIDTimestampFloat, ok := compactIDTimestampI.(float64)
				if !ok {
					continue
				}

				compactIDTimestamp := time.Unix(int64(compactIDTimestampFloat), 0)
				diff := time.Now().UTC().Sub(compactIDTimestamp.UTC())
				if diff > time.Hour*24*30 {
					toDeleteCompactIDPaths = append(toDeleteCompactIDPaths, getCompactionIDSubDocPath(compactID))
				}
			}

			// If all the current compact IDs are to be deleted we can remove the entire attachment compaction xattr.
			// Note that if this operation fails with a cas mismatch we will fall through to the following per ID
			// delete. This can occur if another compact process ends up mutating / deleting the xattr.
			if len(compactIDSyncMap) == len(toDeleteCompactIDPaths) {
				err = dataStore.RemoveXattr(docID, base.AttachmentCompactionXattrName, event.Cas)
				if err == nil {
					return true
				}
				if err != nil && !base.IsCasMismatch(err) {
					base.WarnfCtx(ctx, "[%s] Failed to remove compaction ID xattr for doc %s: %v", compactionLoggingID, base.UD(docID), err)
					return true
				}

			}

			// If we only want to remove select compact IDs delete each one through a subdoc operation
			err = dataStore.DeleteXattrs(docID, toDeleteCompactIDPaths...)
			if err != nil && !errors.Is(err, base.ErrXattrNotFound) {
				base.WarnfCtx(ctx, "[%s] Failed to delete compaction IDs %s for doc %s: %v", compactionLoggingID, strings.Join(toDeleteCompactIDPaths, ","), base.UD(docID), err)
				return true
			}
		}

		return true
	}

	clientOptions, err := getCompactionDCPClientOptions(collectionID, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))
	if err != nil {
		return "", err
	}
	clientOptions.InitialMetadata = base.BuildDCPMetadataSliceFromVBUUIDs(vbUUIDs)

	base.InfofCtx(ctx, base.KeyAll, "[%s] Starting DCP feed for cleanup phase of attachment compaction", compactionLoggingID)

	dcpFeedKey := GenerateCompactionDCPStreamName(compactionID, CleanupPhase)

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return "", err
	}

	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *clientOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create attachment compaction DCP client! %v", compactionLoggingID, err)
		return "", err
	}
	metadataKeyPrefix := dcpClient.GetMetadataKeyPrefix()

	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start attachment compaction DCP feed! %v", compactionLoggingID, err)
		// simplify close in CBG-2234
		_ = dcpClient.Close()
		return metadataKeyPrefix, err
	}

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "[%s] Cleanup phase of attachment compaction completed", compactionLoggingID)
		// simplify close in CBG-2234
		err = dcpClient.Close()
	case <-terminator.Done():
		// simplify close in CBG-2234
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment compaction DCP client! %v", compactionLoggingID, err)
			return metadataKeyPrefix, err
		}

		err = <-doneChan
		if err != nil {
			return metadataKeyPrefix, err
		}

		base.InfofCtx(ctx, base.KeyAll, "[%s] Cleanup phase of attachment compaction was terminated", compactionLoggingID)
	}

	return metadataKeyPrefix, err
}

// getCompactionIDSubDocPath is just a tiny helper func that just concatenates the subdoc path we're using to store
// compactionIDs
func getCompactionIDSubDocPath(compactionID string) string {
	return base.AttachmentCompactionXattrName + "." + CompactionIDKey + "." + compactionID
}

// getCompactionDCPClientOptions returns the default set of DCPClientOptions suitable for attachment compaction
func getCompactionDCPClientOptions(collectionID uint32, groupID string, prefix string) (*base.DCPClientOptions, error) {
	clientOptions := &base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    true,
		MetadataStoreType: base.DCPMetadataStoreCS,
		GroupID:           groupID,
		CollectionIDs:     []uint32{collectionID},
		CheckpointPrefix:  prefix,
	}
	return clientOptions, nil

}

func GenerateCompactionDCPStreamName(compactionID, compactionAction string) string {
	return fmt.Sprintf(
		"sg-%v:att_compaction:%v_%v",
		base.ProductAPIVersion,
		compactionID,
		compactionAction,
	)
}
