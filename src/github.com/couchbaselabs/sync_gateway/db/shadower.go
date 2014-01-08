package db

import (
	"encoding/json"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/walrus"

	"github.com/couchbaselabs/sync_gateway/base"
)

// Bidirectional sync with an external Couchbase bucket.
// Watches the bucket's tap feed and applies changes to the matching managed document.
// Accepts local change notifications and makes equivalent changes to the external bucket.
// See: https://github.com/couchbase/sync_gateway/wiki/Bucket-Shadowing
type Shadower struct {
	context *DatabaseContext // Database
	bucket  base.Bucket      // External bucket we sync with
	tapFeed base.TapFeed     // Observes changes to bucket
}

// Creates a new Shadower.
func NewShadower(context *DatabaseContext, bucket base.Bucket) (*Shadower, error) {
	tapFeed, err := bucket.StartTapFeed(walrus.TapArguments{Backfill: 0})
	if err != nil {
		return nil, err
	}
	s := &Shadower{context: context, bucket: bucket, tapFeed: tapFeed}
	go s.readTapFeed()
	return s, nil
}

// Stops a Shadower. (Safe to call on a nil receiver)
func (s *Shadower) Stop() {
	if s != nil && s.tapFeed != nil {
		s.tapFeed.Close()
	}
}

func (s *Shadower) CurrentCheckpoint() uint64 {
	return 0 // TODO: Current tap protocol doesn't provide any value we can return here :(
}

// Main loop that pulls changes from the external bucket. (Runs in its own goroutine.)
func (s *Shadower) readTapFeed() {
	vbucketsFilling := 0
	for event := range s.tapFeed.Events() {
		switch event.Opcode {
		case walrus.TapBeginBackfill:
			if vbucketsFilling == 0 {
				base.LogTo("Shadow", "Reading history of external bucket")
			}
			vbucketsFilling++
			//base.LogTo("Shadow", "Reading history of external bucket")
		case walrus.TapMutation, walrus.TapDeletion:
			if event.Expiry == 0 {
				isDeletion := event.Opcode == walrus.TapDeletion
				err := s.pullDocument(string(event.Key), event.Value, isDeletion, event.Sequence, event.Flags)
				if err != nil {
					base.Warn("Error applying change from external bucket: %v", err)
				}
			}
		case walrus.TapEndBackfill:
			if vbucketsFilling--; vbucketsFilling == 0 {
				base.LogTo("Shadow", "Caught up with history of external bucket")
			}
		}
	}
	base.LogTo("Shadow", "End of tap feed(?)")
}

// Gets an external document and applies it as a new revision to the managed document.
func (s *Shadower) pullDocument(key string, value []byte, isDeletion bool, cas uint64, flags uint32) error {
	var body Body
	if isDeletion {
		body = Body{"_deleted": true}
	} else {
		if err := json.Unmarshal(value, &body); err != nil {
			return err
		}
	}

	db, _ := CreateDatabase(s.context)
	_, err := db.updateDoc(key, false, func(doc *document) (Body, error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
		if doc.UpstreamCAS != nil && *doc.UpstreamCAS == cas {
			return nil, couchbase.UpdateCancel // we already have this doc revision
		}
		base.LogTo("Shadow+", "Pulling %q, CAS=%x ... have UpstreamRev=%q, UpstreamCAS=%x", key, cas, doc.UpstreamRev, doc.UpstreamCAS)
		// Make the prior pulled revision (if any) the parent:
		parentRev := doc.UpstreamRev
		generation, _ := parseRevID(parentRev)
		newRev := createRevID(generation+1, parentRev, body)
		doc.UpstreamRev = newRev
		doc.UpstreamCAS = &cas
		body["_rev"] = newRev
		if doc.History[newRev] == nil {
			doc.History.addRevision(RevInfo{ID: newRev, Parent: parentRev, Deleted: isDeletion})
			base.LogTo("Shadow", "Pulling %q, CAS=%x --> rev %q", key, cas, newRev)
		} else {
			// We already have this rev; but don't cancel, because we do need to update the
			// doc's UpstreamRev/UpstreamCAS fields.
			base.LogTo("Shadow+", "Not pulling %q, CAS=%x (echo of rev %q)", key, cas, newRev)
		}
		return body, nil
	})
	if err == couchbase.UpdateCancel {
		err = nil
	}
	return err
}

// Saves a new local revision to the external bucket.
func (s *Shadower) PushRevision(doc *document) {
	if doc.newestRevID() == doc.UpstreamRev {
		return // This revision was pulled from the external bucket, so don't push it back!
	}

	var err error
	if doc.Deleted {
		base.LogTo("Shadow", "Pushing %q, rev %q [deletion]", doc.ID, doc.CurrentRev)
		err = s.bucket.Delete(doc.ID)
	} else {
		base.LogTo("Shadow", "Pushing %q, rev %q", doc.ID, doc.CurrentRev)
		body := doc.getRevision(doc.CurrentRev)
		if body == nil {
			base.Warn("Can't get rev %q.%q to push to external bucket", doc.ID, doc.CurrentRev)
			return
		}
		err = s.bucket.Set(doc.ID, 0, body)
	}
	if err != nil {
		base.Warn("Error pushing rev of %q to external bucket: %v", doc.ID, err)
	}
}
