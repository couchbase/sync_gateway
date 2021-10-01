package db

import (
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

const CompactionIDKey = "compactID"

func Mark(db *Database, compactionID string, terminator chan bool) (int, error) {
	base.InfofCtx(db.Ctx, base.KeyAll, "Starting first phase of attachment compaction (mark phase) with compactionID: %q", compactionID)
	compactionLoggingID := "compaction: " + compactionID

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
		// We've had an error previously so no point doing work for any remaining items
		if markProcessFailureErr != nil {
			return false
		}

		// We only want to process full docs. Not any sync docs.
		if strings.HasPrefix(string(event.Key), base.SyncPrefix) {
			return true
		}

		// Attempt to unmarshal the feed data into a document
		doc, err := UnmarshalDocumentFromFeed(string(event.Key), event.Cas, event.Value, event.DataType, "")
		if err != nil {
			return failProcess(err, "[%s] Failed to unmarshal doc %s from feed. Err: %v", compactionLoggingID, base.UD(string(event.Key)), err)
		}

		// Any doc written by SGW should have this value set. Not having this means we can skip this doc as it has not
		// got sync data
		if doc.Cas == uint64(0) {
			return true
		}

		// We need to mark attachments in every leaf revision of the current doc
		// We will build up a list of attachment names which map to attachment doc IDs. Avoids doing multiple KV ops
		// when marking if multiple leaves are referencing the same attachment.
		attachmentKeys := make(map[string]string)
		for _, leafRevision := range doc.History.GetLeaves() {
			_, _, attachmentMeta, err := db.getRevision(doc, leafRevision)
			if err != nil {
				return failProcess(err, "[%s] Failed to get doc %s revision %s. Err: %v", compactionLoggingID, base.UD(string(event.Key)), base.UD(leafRevision), err)
			}

			// Iterate over the attachments
			for attID, attMeta := range attachmentMeta {
				attMetaMap, ok := attMeta.(map[string]interface{})
				if !ok {
					continue
				}

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
				attKey := MakeAttachmentKey(AttVersion1, string(event.Key), digest.(string))
				attachmentKeys[attID] = attKey
			}
		}

		for attachmentName, attachmentDocID := range attachmentKeys {
			// Stamp the current compaction ID into the attachment xattr. This is performing the actual marking
			xattrValue := []byte(`{"` + CompactionIDKey + `": "` + compactionID + `"}`)
			_, err = db.Bucket.SetXattr(attachmentDocID, base.SyncXattrName, xattrValue)

			// If an error occurs while stamping in that ID we need to fail this process and then the entire compaction
			// process. Otherwise an attachment could end up getting erroneously deleted in the later sweep phase.
			if err != nil {
				return failProcess(err, "[%s] Failed to mark attachment %s from doc %s with attachment docID %s. Err: %v", compactionLoggingID, base.UD(attachmentName), base.UD(string(event.Key)), base.UD(attachmentDocID), err)
			}

			base.DebugfCtx(db.Ctx, base.KeyAll, "[%s] Marked attachment %s from doc %s with attachment docID %s", compactionLoggingID, base.UD(attachmentName), base.UD(string(event.Key)), base.UD(attachmentDocID))
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
	defer dcpClient.Close()

	doneChan, err := dcpClient.Start()
	if err != nil {
		return 0, err
	}

	select {
	case <-doneChan:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Mark phase of attachment compaction completed. Marked %d attachments", compactionLoggingID, attachmentsMarked)
	case <-terminator:
		base.InfofCtx(db.Ctx, base.KeyAll, "[%s] Mark phase of attachment compaction was terminated. Marked %d attachments", compactionLoggingID, attachmentsMarked)
	}

	return attachmentsMarked, markProcessFailureErr
}
