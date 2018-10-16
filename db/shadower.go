package db

import (
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"log"
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

	tapFeed, err := bucket.StartTapFeed(sgbucket.FeedArguments{Backfill: 0, Notify: func(bucket string, err error) {
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
		case sgbucket.FeedOpBeginBackfill:
			if vbucketsFilling == 0 {
				base.Infof(base.KeyShadow, "Reading history of external bucket")
			}
			vbucketsFilling++
			// base.Infof(base.KeyShadow, "Reading history of external bucket")
		case sgbucket.FeedOpMutation, sgbucket.FeedOpDeletion:
			key := string(event.Key)
			if !s.docIDMatches(key) {
				break
			}
			isDeletion := event.Opcode == sgbucket.FeedOpDeletion
			if !isDeletion && event.Expiry > 0 {
				break // ignore ephemeral documents
			}
			err := s.pullDocument(key, event.Value, isDeletion, event.Cas, event.Flags)
			if err != nil {
				base.Warnf(base.KeyAll, "Error applying change %q from external bucket: %v", base.UD(key), err)
			}
			atomic.AddUint64(&s.pullCount, 1)
		case sgbucket.FeedOpEndBackfill:
			if vbucketsFilling--; vbucketsFilling == 0 {
				base.Infof(base.KeyShadow, "Caught up with history of external bucket")
			}
		}
	}
	base.Infof(base.KeyShadow, "End of tap feed(?)")
}

// Gets an external document and applies it as a new revision to the managed document.
func (s *Shadower) pullDocument(key string, value []byte, isDeletion bool, cas uint64, flags uint32) error {
	var body Body
	if isDeletion {
		body = Body{BodyDeleted: true}
	} else {
		if err := body.Unmarshal(value); err != nil {
			base.Infof(base.KeyShadow, "Doc %q is not JSON; skipping", base.UD(key))
			return nil
		}
	}

	db, _ := CreateDatabase(s.context)
	expiry, _, err := body.getExpiry()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}

	_, err = db.updateDoc(key, false, expiry, func(doc *document) (resultBody Body, resultAttachmentData AttachmentData, updatedExpiry *uint32, resultErr error) {
		// (Be careful: this block can be invoked multiple times if there are races!)
		if doc.UpstreamCAS != nil && *doc.UpstreamCAS == cas {
			return nil, nil, nil, base.ErrUpdateCancel // we already have this doc revision
		}
		base.Debugf(base.KeyShadow, "Pulling %q, CAS=%x ... have UpstreamRev=%q, UpstreamCAS=%x", base.UD(key), cas, doc.UpstreamRev, doc.UpstreamCAS)

		// Compare this body to the current revision body to see if it's an echo:
		parentRev := doc.UpstreamRev
		newRev := doc.CurrentRev
		docBody := doc.getRevisionBody(newRev, s.context.RevisionBodyLoader)
		if !reflect.DeepEqual(body, docBody) {
			// Nope, it's not. Assign it a new rev ID
			log.Printf("Not equal.  body: %+v  %T", body, body["foo"])
			log.Printf("Not equal.  docBody: %+v %T", docBody, docBody["foo"])
			generation, _ := ParseRevID(parentRev)
			newRev = createRevID(generation+1, parentRev, body)
		}
		doc.UpstreamRev = newRev
		doc.UpstreamCAS = &cas
		body[BodyRev] = newRev
		if doc.History[newRev] == nil {
			// It's a new rev, so add it to the history:
			if parentRev != "" && !doc.History.contains(parentRev) {
				// parent rev does not exist in the doc history
				// set parentRev to "", this will create a  new conflicting
				//branch in the revtree
				base.Warnf(base.KeyAll, "Shadow: Adding revision as conflict branch, parent id %q is missing", parentRev)
				parentRev = ""
			}
			if err = doc.History.addRevision(doc.ID, RevInfo{ID: newRev, Parent: parentRev, Deleted: isDeletion}); err != nil {
				return nil, nil, nil, err
			}
			base.Infof(base.KeyShadow, "Pulling %q, CAS=%x --> rev %q", base.UD(key), cas, newRev)
		} else {
			// We already have this rev; but don't cancel, because we do need to update the
			// doc's UpstreamRev/UpstreamCAS fields.
			base.Debugf(base.KeyShadow, "Not pulling %q, CAS=%x (echo of rev %q)", base.UD(key), cas, newRev)
		}
		return body, nil, nil, nil
	})
	if err == base.ErrUpdateCancel {
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
		base.Infof(base.KeyShadow, "Pushing %q, rev %q [deletion]", base.UD(doc.ID), doc.CurrentRev)
		err = s.bucket.Delete(doc.ID)
	} else {
		base.Infof(base.KeyShadow, "Pushing %q, rev %q", base.UD(doc.ID), doc.CurrentRev)
		body := doc.getRevisionBody(doc.CurrentRev, s.context.RevisionBodyLoader)
		if body == nil {
			base.Warnf(base.KeyAll, "Can't get rev %q.%q to push to external bucket", base.UD(doc.ID), doc.CurrentRev)
			return
		}
		err = s.bucket.Set(doc.ID, 0, body)
	}
	if err != nil {
		base.Warnf(base.KeyAll, "Error pushing rev of %q to external bucket: %v", base.UD(doc.ID), err)
	}
}
