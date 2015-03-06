package db

import (
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/snej/zdelta-go"
)

// How long to cache precomputed deltas
var DeltaCacheExpirationTime = 5 * time.Minute

// How many bytes shorter than the target a delta needs to be, to be worth using
var MinDeltaSavings = 100

// Looks up a cached delta between two attachments given their keys (digests).
// If the delta is not worth using (not enough space savings), returns an empty array.
// If no delta is cached, returns nil.
func (db *Database) getCachedAttachmentZDelta(srcKey, dstKey AttachmentKey) []byte {
	return db._getCachedZDelta("att", srcKey.EncodedString(), dstKey.EncodedString())
}

// Computes & caches the delta between two attachments given their data.
// If the delta is not worth using (not enough space savings), returns an empty array.
func (db *Database) generateAttachmentZDelta(src, dst []byte, srcKey, dstKey AttachmentKey) []byte {
	return db._generateZDelta(src, dst, "att", srcKey.EncodedString(), dstKey.EncodedString())
}

// INTERNAL:

func (db *Database) _getCachedZDelta(idType, srcID, dstID string) []byte {
	key := _keyForCachedZDelta(idType, srcID, dstID)
	delta, _ := db.Bucket.GetRaw(key)
	if delta != nil {
		base.LogTo("Delta", "Reused cached zdelta %s %s --> %s",
			idType, srcID, dstID)
	}
	return delta
}

func (db *Database) _generateZDelta(src, dst []byte, idType, srcID, dstID string) []byte {
	delta, _ := zdelta.CreateDelta(src, dst)
	if delta == nil {
		return nil
	}
	base.LogTo("Delta", "Computed zdelta %s %s --> %s: saved %d bytes",
		idType, srcID, dstID, int64(len(dst))-int64(len(delta)))
	if len(delta)+MinDeltaSavings > len(dst) {
		delta = []byte{} // not worth using
	}
	// Cache the computed delta:
	key := _keyForCachedZDelta(idType, srcID, dstID)
	db.Bucket.SetRaw(key, int(DeltaCacheExpirationTime.Seconds()), delta)
	return delta
}

func _keyForCachedZDelta(idType, srcID, dstID string) string {
	return fmt.Sprintf("_sync:zd:%s:%d:%s:%s", idType, len(srcID), srcID, dstID)
}
