package db

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const CompactionIDKey = "compactID"

func Mark(db *Database, compactionID string, terminator chan struct{}, markedAttachmentCallback func(markedAttachments *int)) (int, error) {
	base.InfofCtx(db.Ctx, base.KeyAll, "Starting first phase of attachment compaction (mark phase) with compactionID: %q", compactionID)
	compactionLoggingID := "Compaction Mark: " + compactionID

	var markProcessFailureErr error
	var attachmentsMarked int

	// failProcess used when a failure is deemed as 'un-recoverable' and we need to abort the compaction process.
	failProcess := func(err error, format string, args ...interface{}) bool {
		markProcessFailureErr = err
		close(terminator)
		base.WarnfCtx(db.Ctx, format, args)
		return false
	}

	callback := func(event sgbucket.FeedEvent) bool {
		defer markedAttachmentCallback(&attachmentsMarked)

		docID := string(event.Key)

		// We've had an error previously so no point doing work for any remaining items
		if markProcessFailureErr != nil {
			return false
		}

		// We only want to process full docs. Not any sync docs.
		if strings.HasPrefix(docID, base.SyncPrefix) {
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
				bodyRaw, _, err := db.Bucket.GetRaw(bodyKey)
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
			xattrValue := []byte(`{"` + CompactionIDKey + `": "` + compactionID + `"}`)
			_, err = db.Bucket.SetXattr(attachmentDocID, base.AttachmentCompactionXattrName, xattrValue)

			// If an error occurs while stamping in that ID we need to fail this process and then the entire compaction
			// process. Otherwise, an attachment could end up getting erroneously deleted in the later sweep phase.
			if err != nil {
				return failProcess(err, "[%s] Failed to mark attachment %s from doc %s with attachment docID %s. Err: %v", compactionLoggingID, base.UD(attachmentName), base.UD(docID), base.UD(attachmentDocID), err)
			}

			base.DebugfCtx(db.Ctx, base.KeyAll, "[%s] Marked attachment %s from doc %s with attachment docID %s", compactionLoggingID, base.UD(attachmentName), base.UD(docID), base.UD(attachmentDocID))
			attachmentsMarked++
		}
		return true
	}

	cbStore, ok := base.AsCouchbaseStore(db.Bucket)
	if !ok {
		return 0, fmt.Errorf("bucket is not a Couchbase Store")
	}

	clientOptions := base.DCPClientOptions{
		OneShot: true,
	}

	base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Starting DCP feed for mark phase of attachment compaction", compactionLoggingID)
	dcpClient, err := base.NewDCPClient(compactionID, callback, clientOptions, cbStore)
	if err != nil {
		return 0, err
	}

	doneChan, err := dcpClient.Start()
	if err != nil {
		_ = dcpClient.Close()
		return 0, err
	}

	select {
	case <-doneChan:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Mark phase of attachment compaction completed. Marked %d attachments", compactionLoggingID, attachmentsMarked)
	case <-terminator:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Mark phase of attachment compaction was terminated. Marked %d attachments", compactionLoggingID, attachmentsMarked)
	}

	if markProcessFailureErr != nil {
		_ = dcpClient.Close()
		return attachmentsMarked, markProcessFailureErr
	}

	return attachmentsMarked, dcpClient.Close()
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

func Sweep(db *Database, compactionID string, terminator chan struct{}, purgedAttachmentCallback func(purgedAttachments *int)) (int, error) {
	base.InfofCtx(db.Ctx, base.KeyAll, "Starting second phase of attachment compaction (sweep phase) with compactionID: %q", compactionID)
	compactionLoggingID := "Compaction Sweep: " + compactionID

	var attachmentsDeleted int

	// Iterate over v1 attachments and if not marked with supplied compactionID we can purge the attachments.
	// In the event of an error we can return but continue - Worst case is an attachment which should be deleted won't
	// be deleted.
	callback := func(event sgbucket.FeedEvent) bool {
		defer purgedAttachmentCallback(&attachmentsDeleted)

		docID := string(event.Key)

		// We only want to look over v1 attachment docs, skip otherwise
		if !strings.HasPrefix(docID, base.AttPrefix) {
			return true
		}

		// If the data contains an xattr then the attachment likely has a compaction ID, need to check this value
		if event.DataType&base.MemcachedDataTypeXattr != 0 {
			_, xattr, _, err := parseXattrStreamData(base.AttachmentCompactionXattrName, "", event.Value)
			if err != nil && !errors.Is(err, base.ErrXattrNotFound) {
				base.WarnfCtx(db.Ctx, "[%s] Unexpected error occurred attempting to parse attachment xattr: %v", compactionLoggingID, err)
				return true
			}

			// If the document did indeed have an xattr then check the compactID. If it is the same as the current
			// running compaction ID we don't want to purge this doc and can continue to the next doc.
			if xattr != nil {
				var syncData map[string]interface{}
				err = base.JSONUnmarshal(xattr, &syncData)
				if err != nil {
					base.WarnfCtx(db.Ctx, "[%s] Failed to unmarshal xattr data: %v", compactionLoggingID, err)
					return true
				}

				docCompactID, ok := syncData[CompactionIDKey]
				if ok && docCompactID == compactionID {
					return true
				}
			}
		}

		// If we've reached this point the current v1 attachment being processed either:
		// - Has no compactionID set in its xattr
		// - Has a compactionID set in its xattr but it is from a previous run and therefore is not equal to the passed
		// in compactionID
		// Therefore, we want to purge the doc
		_, err := db.Bucket.Remove(docID, event.Cas)
		if err != nil {
			base.WarnfCtx(db.Ctx, "[%s] Unable to purge attachment %s: %v", compactionLoggingID, base.UD(docID), err)
			return true
		}

		base.DebugfCtx(db.Ctx, base.KeyAll, "[%s] Purged attachment %s", compactionLoggingID, base.UD(docID))
		attachmentsDeleted++

		return true
	}

	cbStore, ok := base.AsCouchbaseStore(db.Bucket)
	if !ok {
		return 0, fmt.Errorf("bucket is not a Couchbase Store")
	}

	clientOptions := base.DCPClientOptions{
		OneShot: true,
	}

	base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Starting DCP feed for sweep phase of attachment compaction", compactionLoggingID)
	dcpClient, err := base.NewDCPClient(compactionID, callback, clientOptions, cbStore)
	if err != nil {
		return 0, err
	}

	doneChan, err := dcpClient.Start()
	if err != nil {
		_ = dcpClient.Close()
		return 0, err
	}

	select {
	case <-doneChan:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Sweep phase of attachment compaction completed. Deleted %d attachments", compactionLoggingID, attachmentsDeleted)
	case <-terminator:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Sweep phase of attachment compaction was terminated. Deleted %d attachments", compactionLoggingID, attachmentsDeleted)
	}

	return attachmentsDeleted, dcpClient.Close()
}
