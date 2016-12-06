package db

import (
	"encoding/json"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/couchbase/go-couchbase"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Bidirectional sync with an external Couchbase bucket.
// Watches the bucket's tap feed and applies changes to the matching managed document.
// Accepts local change notifications and makes equivalent changes to the external bucket.
// See: https://github.com/couchbase/sync_gateway/wiki/Bucket-Shadowing
type Shadower struct {
	context              *DatabaseContext // Database
	bucket               base.Bucket      // External bucket we sync with
	tapFeed              base.TapFeed     // Observes changes to bucket
	docIDPattern         *regexp.Regexp   // Optional regex that key/doc IDs must match
	pullCount, pushCount uint64           // Used for testing
}

// Creates a new Shadower.
func NewShadower(context *DatabaseContext, bucket base.Bucket, docIDPattern *regexp.Regexp) (*Shadower, error) {
	tapFeed, err := bucket.StartTapFeed(sgbucket.TapArguments{Backfill: 0, Notify: func(bucket string, err error) {
		context.TakeDbOffline("Lost shadower TAP Feed")
	}})
	if err != nil {
		return nil, err
	}
	s := &Shadower{context: context, bucket: bucket, tapFeed: tapFeed, docIDPattern: docIDPattern}
	go s.readTapFeed()
	return s, nil
}

// Stops a Shadower. (Safe to call on a nil receiver)
func (s *Shadower) Stop() {
	if s != nil && s.tapFeed != nil {
		s.tapFeed.Close()
	}
}

func (s *Shadower) docIDMatches(docID string) bool {
	if s.docIDPattern != nil {
		match := s.docIDPattern.FindStringIndex(docID)
		if match == nil || match[0] != 0 || match[1] != len(docID) {
			return false
		}
	}
	return !strings.HasPrefix(docID, KSyncKeyPrefix)
}

// Main loop that pulls changes from the external bucket. (Runs in its own goroutine.)
func (s *Shadower) readTapFeed() {
	vbucketsFilling := 0
	for event := range s.tapFeed.Events() {
		switch event.Opcode {
		case sgbucket.TapBeginBackfill:
			if vbucketsFilling == 0 {
				base.LogTo("Shadow", "Reading history of external bucket")
			}
			vbucketsFilling++
			//base.LogTo("Shadow", "Reading history of external bucket")
		case sgbucket.TapMutation, sgbucket.TapDeletion:
			key := string(event.Key)
			if !s.docIDMatches(key) {
				break
			}
			isDeletion := event.Opcode == sgbucket.TapDeletion
			if !isDeletion && event.Expiry > 0 {
				break // ignore ephemeral documents
			}
			err := s.pullDocument(key, event.Value, isDeletion, event.Sequence, event.Flags)
			if err != nil {
				base.Warn("Error applying change %q from external bucket: %v", key, err)
			}
			atomic.AddUint64(&s.pullCount, 1)
		case sgbucket.TapEndBackfill:
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
			base.LogTo("Shadow", "Doc %q is not JSON; skipping", key)
			return nil
		}
	}

	db, _ := CreateDatabase(s.context)
	expiry, err := body.getExpiry()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}
	_, err = db.updateDoc(key, false, expiry, func(doc *document) (Body, AttachmentData, error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
		if doc.UpstreamCAS != nil && *doc.UpstreamCAS == cas {
			return nil, nil, couchbase.UpdateCancel // we already have this doc revision
		}
		base.LogTo("Shadow+", "Pulling %q, CAS=%x ... have UpstreamRev=%q, UpstreamCAS=%x", key, cas, doc.UpstreamRev, doc.UpstreamCAS)

		// Compare this body to the current revision body to see if it's an echo:
		parentRev := doc.UpstreamRev
		newRev := doc.CurrentRev
		if !reflect.DeepEqual(body, doc.getRevision(newRev)) {
			// Nope, it's not. Assign it a new rev ID
			generation, _ := ParseRevID(parentRev)
			newRev = createRevID(generation+1, parentRev, body)
		}
		doc.UpstreamRev = newRev
		doc.UpstreamCAS = &cas
		body["_rev"] = newRev
		if doc.History[newRev] == nil {
			// It's a new rev, so add it to the history:
			if parentRev != "" && !doc.History.contains(parentRev) {
				// parent rev does not exist in the doc history
				// set parentRev to "", this will create a  new conflicting
				//branch in the revtree
				base.Warn("Shadow: Adding revision as conflict branch, parent id %q is missing", parentRev)
				parentRev = ""
			}
			doc.History.addRevision(RevInfo{ID: newRev, Parent: parentRev, Deleted: isDeletion})
			base.LogTo("Shadow", "Pulling %q, CAS=%x --> rev %q", key, cas, newRev)
		} else {
			// We already have this rev; but don't cancel, because we do need to update the
			// doc's UpstreamRev/UpstreamCAS fields.
			base.LogTo("Shadow+", "Not pulling %q, CAS=%x (echo of rev %q)", key, cas, newRev)
		}
		return body, nil, nil
	})
	if err == couchbase.UpdateCancel {
		err = nil
	}
	return err
}

// Saves a new local revision to the external bucket.
func (s *Shadower) PushRevision(doc *document) {
	defer func() { atomic.AddUint64(&s.pushCount, 1) }()
	if !s.docIDMatches(doc.ID) {
		return
	} else if doc.newestRevID() == doc.UpstreamRev {
		return // This revision was pulled from the external bucket, so don't push it back!
	}

	var err error
	if doc.Flags&channels.Deleted != 0 {
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
