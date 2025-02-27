/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"iter"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type BlipTesterClientOpts struct {
	ClientDeltas                  bool // Support deltas on the client side
	Username                      string
	Channels                      []string
	SendRevocations               bool
	SupportedBLIPProtocols        []string
	SkipCollectionsInitialization bool

	// a deltaSrc rev ID for which to reject a delta
	rejectDeltasForSrcRev string

	// changesEntryCallback is a callback function invoked for each changes entry being received (pull)
	changesEntryCallback func(docID, revID string)

	// optional Origin header
	origin *string

	// sendReplacementRevs opts into the replacement rev behaviour in the event that we do not find the requested one.
	sendReplacementRevs bool

	revsLimit *int // defaults to 20

}

// defaultBlipTesterClientRevsLimit is the number of revisions sent as history when the client replicates - older revisions are not sent, and may not be stored.
const defaultBlipTesterClientRevsLimit = 20

// BlipTesterClient is a fully fledged client to emulate CBL behaviour on both push and pull replications through methods on this type.
type BlipTesterClient struct {
	BlipTesterClientOpts

	id              uint32 // unique ID for the client
	rt              *RestTester
	pullReplication *BlipTesterReplicator // SG -> CBL replications
	pushReplication *BlipTesterReplicator // CBL -> SG replications

	collectionClients        []*BlipTesterCollectionClient
	nonCollectionAwareClient *BlipTesterCollectionClient
}

// getProposeChangesForSeq returns a proposeChangeBatch entry for a document, if there is a document existing at this sequence.
func (btcc *BlipTesterCollectionClient) getProposeChangesForSeq(seq clientSeq) (*proposeChangeBatchEntry, bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._seqStore[seq]
	if !ok {
		return nil, false
	}
	return doc._proposeChangesEntryForDoc(), true
}

// OneShotChangesSince is an iterator that yields client sequence and document pairs that are newer than the given since value.
func (btcc *BlipTesterCollectionClient) OneShotChangesSince(ctx context.Context, since clientSeq) iter.Seq2[clientSeq, *proposeChangeBatchEntry] {
	return func(yield func(clientSeq, *proposeChangeBatchEntry) bool) {
		btcc.seqLock.Lock()
		seqLast := btcc._seqLast
		for btcc._seqLast <= since {
			if ctx.Err() != nil {
				btcc.seqLock.Unlock()
				return
			}
			// block until new seq
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotChangesSince: since=%d, _seqLast=%d - waiting for new sequence", since, btcc._seqLast)
			btcc._seqCond.Wait()
			// Check to see if we were woken because of Close()
			if ctx.Err() != nil {
				btcc.seqLock.Unlock()
				return
			}
			seqLast = btcc._seqLast
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotChangesSince: since=%d, _seqLast=%d - woke up", since, btcc._seqLast)
		}
		btcc.seqLock.Unlock()
		base.DebugfCtx(ctx, base.KeySGTest, "OneShotChangesSince: since=%d, _seqLast=%d - iterating", since, seqLast)
		for seq := since; seq <= seqLast; seq++ {
			change, ok := btcc.getProposeChangesForSeq(seq)
			// filter non-latest entries in cases where we haven't pruned _seqStore
			if !ok {
				continue
			}
			if !yield(seq, change) {
				base.DebugfCtx(ctx, base.KeySGTest, "OneShotChangesSince: since=%d, _seqLast=%d - stopping iteration", since, seqLast)
				return
			}
		}
		base.DebugfCtx(ctx, base.KeySGTest, "OneShotChangesSince: since=%d, _seqLast=%d - done", since, seqLast)
	}
}

// changesSince returns a channel which will yield proposed versions of changes that are the given since value.
// The channel will be closed when the iteration is finished. In the case of a continuous iteration, the channel will remain open until the context is cancelled.
func (btcc *BlipTesterCollectionClient) changesSince(ctx context.Context, since clientSeq, continuous bool) chan *proposeChangeBatchEntry {
	ch := make(chan *proposeChangeBatchEntry)
	btcc.goroutineWg.Add(1)
	go func() {
		defer btcc.goroutineWg.Done()
		sinceVal := since
		defer close(ch)
		for {
			if ctx.Err() != nil {
				return
			}
			base.DebugfCtx(ctx, base.KeySGTest, "changesSince: sinceVal=%d", sinceVal)
			for _, change := range btcc.OneShotChangesSince(ctx, sinceVal) {
				select {
				case <-ctx.Done():
					return
				case ch <- change:
					base.DebugfCtx(ctx, base.KeySGTest, "sent doc %q to changes feed", change.docID)
					sinceVal = change.seq
				}
			}
			if !continuous {
				base.DebugfCtx(ctx, base.KeySGTest, "opts.Continuous=false, breaking changes loop")
				break
			}
		}
	}()
	return ch
}

type clientSeq uint64

// clientDocRev represents a revision of a document stored on this client, including any metadata associated with this specific revision.
type clientDocRev struct {
	clientSeq clientSeq
	version   DocVersion
	body      []byte
	isDelete  bool
	message   *blip.Message // rev or norev message associated with this revision when replicated
}

// clientDoc represents a document stored on the client - it may also contain older versions of the document.
type clientDoc struct {
	id                   string                     // doc ID
	_latestSeq           clientSeq                  // Latest sequence number we have for the doc - the active rev
	_latestServerVersion DocVersion                 // Latest version we know the server had (via push or a pull)
	_revisionsBySeq      map[clientSeq]clientDocRev // Full history of doc from client POV
	_seqsByVersions      map[DocVersion]clientSeq   // Lookup from version into revisionsBySeq
}

// newClientDocument creates a local copy of a document
func newClientDocument(docID string, newClientSeq clientSeq, rev *clientDocRev) *clientDoc {
	doc := &clientDoc{
		id:              docID,
		_latestSeq:      newClientSeq,
		_revisionsBySeq: make(map[clientSeq]clientDocRev),
		_seqsByVersions: make(map[DocVersion]clientSeq),
	}
	if rev != nil {
		doc._revisionsBySeq[rev.clientSeq] = *rev
		doc._seqsByVersions[rev.version] = rev.clientSeq
	}
	return doc
}

// _docRevSeqsNewestToOldest returns a list of sequences associated with this document, ordered newest to oldest. Calling this function requires holding BlipTesterCollectionClient.seqLock as read lock.
func (cd *clientDoc) _docRevSeqsNewestToOldest() []clientSeq {
	seqs := make([]clientSeq, 0, len(cd._revisionsBySeq))
	for _, rev := range cd._revisionsBySeq {
		seqs = append(seqs, rev.clientSeq)
	}
	slices.Sort(seqs)    // oldest to newest
	slices.Reverse(seqs) // newest to oldest
	return seqs
}

// _latestRev returns the latest revision of the document. Calling this function requires holding BlipTesterCollectionClient.seqLock as read lock.
func (cd *clientDoc) _latestRev(tb testing.TB) *clientDocRev {
	rev, ok := cd._revisionsBySeq[cd._latestSeq]
	require.True(tb, ok, "latestSeq %d not found in revisionsBySeq", cd._latestSeq)
	return &rev
}

// _addNewRev adds a new revision to the document. Calling this function requires holding BlipTesterCollectionClient.seqLock as write lock.
func (cd *clientDoc) _addNewRev(rev clientDocRev) {
	cd._latestSeq = rev.clientSeq
	cd._revisionsBySeq[rev.clientSeq] = rev
	cd._seqsByVersions[rev.version] = rev.clientSeq
}

// _revisionBySeq returns the revision associated with the given sequence number. Calling this function requires holding BlipTesterCollectionClient.seqLock as read lock.
func (cd *clientDoc) _revisionBySeq(tb testing.TB, seq clientSeq) *clientDocRev {
	rev, ok := cd._revisionsBySeq[seq]
	require.True(tb, ok, "seq %d not found in revisionsBySeq", seq)
	return &rev
}

// _getRev returns the revision associated with the given version. Calling this function requires holding BlipTesterCollectionClient.seqLock as read lock.
func (cd *clientDoc) _getRev(tb testing.TB, version DocVersion) *clientDocRev {
	seq, ok := cd._seqsByVersions[version]
	require.True(tb, ok, "version %v not found in seqsByVersions", version)
	rev, ok := cd._revisionsBySeq[seq]
	require.True(tb, ok, "seq %d not found in revisionsBySeq", seq)
	return &rev
}

// _pruneVersion removes the given version from the document. Calling this function requires holding BlipTesterCollectionClient.seqLock as write lock.
func (cd *clientDoc) _pruneVersion(t testing.TB, version DocVersion) {
	seq, ok := cd._seqsByVersions[version]
	require.True(t, ok, "version %v not found in seqsByVersions", version)
	require.Less(t, seq, cd._latestSeq, "seq %d is the latest seq for doc %q, can not prune latest version", seq, cd.id)
	delete(cd._seqsByVersions, version)
	delete(cd._revisionsBySeq, seq)
}

// _proposeChangesEntryForDoc returns a proposeChangeBatchEntry representing the revision and history for the change from the last known version replicated to server. Calling this function requires holding BlipTesterCollectionClient.seqLock as read lock.
func (cd *clientDoc) _proposeChangesEntryForDoc() *proposeChangeBatchEntry {
	latestRev := cd._revisionsBySeq[cd._latestSeq]
	var revisionHistory []DocVersion
	for i, seq := range cd._docRevSeqsNewestToOldest() {
		if i == 0 {
			// skip current rev
			continue
		}
		revisionHistory = append(revisionHistory, cd._revisionsBySeq[seq].version)
	}
	return &proposeChangeBatchEntry{docID: cd.id, version: latestRev.version, history: revisionHistory, latestServerVersion: cd._latestServerVersion, seq: cd._latestSeq}
}

type BlipTesterCollectionClient struct {
	parent *BlipTesterClient

	ctx         context.Context
	ctxCancel   context.CancelFunc
	goroutineWg sync.WaitGroup

	collection    string
	collectionIdx int

	// seqLock protects all _seq fields below, including the _seqStore map
	seqLock *sync.RWMutex
	// _lastSeq is the client's latest assigned sequence number
	_seqLast clientSeq
	// _seqStore is a sparse map of (client) sequences and the corresponding document
	// entries are removed from this map when the sequence no longer represents an active document revision
	// the older revisions for a particular document can still be accessed via clientDoc.revisionsBySeq if required
	_seqStore map[clientSeq]*clientDoc
	// _seqFromDocID used to lookup entry in _seqStore by docID - not a pointer into other map for simplicity
	_seqFromDocID map[string]clientSeq
	// _seqCond is used to signal when a new sequence has been added to wake up idle "changes" loops
	_seqCond *sync.Cond

	attachmentsLock sync.RWMutex      // lock for _attachments map
	_attachments    map[string][]byte // Client's local store of _attachments - Map of digest to bytes
}

// _getClientDoc returns the clientDoc for the given docID, if it exists. Requires BlipTesterCollectionClient.seqLock read lock to be held.
func (btcc *BlipTesterCollectionClient) _getClientDoc(docID string) (*clientDoc, bool) {
	seq, ok := btcc._seqFromDocID[docID]
	if !ok {
		return nil, false
	}
	clientDoc, ok := btcc._seqStore[seq]
	require.True(btcc.TB(), ok, "docID %q found in _seqFromDocID but seq %d not in _seqStore %v", docID, seq, btcc._seqStore)
	return clientDoc, ok
}

// BlipTestClientRunner is for running the blip tester client and its associated methods in test framework
type BlipTestClientRunner struct {
	clients                         map[uint32]*BlipTesterClient // map of created BlipTesterClient's
	t                               *testing.T
	initialisedInsideRunnerCode     bool // flag to check that the BlipTesterClient is being initialised in the correct area (inside the Run() method)
	SkipVersionVectorInitialization bool // used to skip the version vector subtest
}

// BlipTesterReplicator is a BlipTester which stores a map of messages keyed by Serial Number
type BlipTesterReplicator struct {
	bt *BlipTester
	id string // Generated UUID on creation

	messagesLock sync.RWMutex                         // lock for messages map
	messages     map[blip.MessageNumber]*blip.Message // Map of blip messages keyed by message number

	replicationStats *db.BlipSyncStats // Stats of replications
}

// NewBlipTesterClientRunner creates a BlipTestClientRunner type
func NewBlipTesterClientRunner(t *testing.T) *BlipTestClientRunner {
	return &BlipTestClientRunner{
		t:       t,
		clients: make(map[uint32]*BlipTesterClient),
	}
}

// Close shuts down all the clients and clears all messages stored.
func (btr *BlipTesterReplicator) Close() {
	btr.bt.Close()
	btr.messagesLock.Lock()
	btr.messages = make(map[blip.MessageNumber]*blip.Message, 0)
	btr.messagesLock.Unlock()
}

// initHandlers sets up the blip client side handles for each message type.
func (btr *BlipTesterReplicator) initHandlers(btc *BlipTesterClient) {

	if btr.replicationStats == nil {
		btr.replicationStats = db.NewBlipSyncStats()
	}

	ctx := base.DatabaseLogCtx(base.TestCtx(btr.bt.restTester.TB()), btr.bt.restTester.GetDatabase().Name, nil)
	btr.bt.blipContext.DefaultHandler = btr.defaultHandler()
	handlers := map[string]func(*blip.Message){
		db.MessageNoRev:           btr.handleNoRev(btc),
		db.MessageGetAttachment:   btr.handleGetAttachment(btc),
		db.MessageRev:             btr.handleRev(ctx, btc),
		db.MessageProposeChanges:  btr.handleProposeChanges(btc),
		db.MessageChanges:         btr.handleChanges(btc),
		db.MessageProveAttachment: btr.handleProveAttachment(ctx, btc),
	}
	for profile, handler := range handlers {
		btr.bt.blipContext.HandlerForProfile[profile] = func(msg *blip.Message) {
			defer btr.storeMessage(msg)
			handler(msg)
		}
	}
}

// handleProveAttachment handles proveAttachment received by blip client
func (btr *BlipTesterReplicator) handleProveAttachment(ctx context.Context, btc *BlipTesterClient) func(*blip.Message) {
	return func(msg *blip.Message) {
		nonce, err := msg.Body()
		require.NoError(btr.TB(), err)
		require.NotEmpty(btr.TB(), nonce, "no nonce sent with proveAttachment")

		digest, ok := msg.Properties[db.ProveAttachmentDigest]
		require.True(btr.TB(), ok, "no digest sent with proveAttachment")

		btcc := btc.getCollectionClientFromMessage(msg)

		attData := btcc.getAttachment(digest)

		proof := db.ProveAttachment(ctx, attData, nonce)

		resp := msg.Response()
		resp.SetBody([]byte(proof))
		btr.replicationStats.ProveAttachment.Add(1)
	}

}

// handleChanges handles changes messages on the blip tester client
func (btr *BlipTesterReplicator) handleChanges(btc *BlipTesterClient) func(*blip.Message) {
	revsLimit := base.IntDefault(btc.revsLimit, defaultBlipTesterClientRevsLimit)
	return func(msg *blip.Message) {
		btcc := btc.getCollectionClientFromMessage(msg)

		// Exit early when there's nothing to do
		if msg.NoReply() {
			return
		}

		body, err := msg.Body()
		require.NoError(btr.TB(), err)

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(btr.TB(), err)

			knownRevs = make([]interface{}, len(changesReqs))
			// changesReqs == [[sequence, docID, revID, {deleted}, {size (bytes)}], ...]
		outer:
			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)

				if btc.changesEntryCallback != nil {
					btc.changesEntryCallback(docID, revID)
				}

				deletedInt := 0
				if len(changesReq) > 3 {
					castedDeleted, ok := changesReq[3].(float64)
					if ok {
						deletedInt = int(castedDeleted)
						if deletedInt&2 == 2 {
							continue
						}
					}
				}

				// Build up a list of revisions known to the client for each change
				// The first element of each revision list must be the parent revision of the change
				revList := make([]string, 0, revsLimit)
				allVersions := btcc.getAllRevisions(docID)
				if len(allVersions) == 0 {
					knownRevs[i] = []interface{}{} // sending empty array means we've not seen the doc before, but still want it
					continue
				}
				for _, version := range allVersions {
					if revID == version.RevID {
						knownRevs[i] = nil // Send back null to signal we don't need this change
						continue outer
					}

					if len(revList) < revsLimit {
						revList = append(revList, version.RevID)
					} else {
						break
					}

					knownRevs[i] = revList
				}

			}
		}

		response := msg.Response()
		if btc.ClientDeltas {
			// Enable deltas from the client side
			response.Properties["deltas"] = "true"
		}

		b, err := base.JSONMarshal(knownRevs)
		require.NoError(btr.TB(), err)

		response.SetBody(b)
	}
}

// handleProposeChanges handles proposeChanges messages on the blip tester client
func (btr *BlipTesterReplicator) handleProposeChanges(btc *BlipTesterClient) func(msg *blip.Message) {
	return func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)
	}
}

// handleRev handles rev messages on the blip tester client
func (btr *BlipTesterReplicator) handleRev(ctx context.Context, btc *BlipTesterClient) func(msg *blip.Message) {
	return func(msg *blip.Message) {
		defer btc.pullReplication.storeMessage(msg)

		btcc := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.RevMessageID]
		revID := msg.Properties[db.RevMessageRev]
		deltaSrc := msg.Properties[db.RevMessageDeltaSrc]
		replacedRev := msg.Properties[db.RevMessageReplacedRev]

		body, err := msg.Body()
		require.NoError(btr.TB(), err)

		if msg.Properties[db.RevMessageDeleted] == "1" {
			rev := revOptions{
				newVersion:                DocVersion{RevID: revID},
				body:                      body,
				isDelete:                  true,
				msg:                       msg,
				updateLatestServerVersion: true,
			}
			if replacedRev != "" {
				rev.replacedVersion = &DocVersion{RevID: replacedRev}
			}
			btcc.addRev(docID, rev)

			if !msg.NoReply() {
				response := msg.Response()
				response.SetBody([]byte(`[]`))
			}
			return
		}

		// bodyJSON is unmarshalled into when required (e.g. Delta patching, or attachment processing)
		// Before being marshalled back into bytes for storage in the test client
		var bodyJSON db.Body

		// If deltas are enabled, and we see a deltaSrc property, we'll need to patch it before storing
		if btc.ClientDeltas && deltaSrc != "" {
			if btc.rejectDeltasForSrcRev == deltaSrc {
				require.False(btr.TB(), msg.NoReply(), "expected delta rev message to be sent without noreply flag: %+v", msg)
				response := msg.Response()
				response.SetError("HTTP", http.StatusUnprocessableEntity, "test code intentionally rejected delta")
			}

			// unmarshal body to extract deltaSrc
			var delta db.Body
			err := delta.Unmarshal(body)
			require.NoError(btc.TB(), err)

			old := btcc.getBody(docID, DocVersion{RevID: deltaSrc})
			var oldMap = map[string]interface{}(old)
			err = base.Patch(&oldMap, delta)
			require.NoError(btc.TB(), err)

			bodyJSON = oldMap
		}

		// Fetch any missing attachments (if required) during this rev processing
		if bytes.Contains(body, []byte(db.BodyAttachments)) {

			// We'll need to unmarshal the body in order to do attachment processing
			if bodyJSON == nil {
				err := bodyJSON.Unmarshal(body)
				require.NoError(btr.TB(), err)
			}

			if atts, ok := bodyJSON[db.BodyAttachments]; ok {
				attsMap, ok := atts.(map[string]interface{})
				require.True(btr.TB(), ok, "atts in doc wasn't map[string]interface{}")

				var missingDigests []string
				var knownDigests []string
				btcc.attachmentsLock.RLock()
				for _, attachment := range attsMap {
					attMap, ok := attachment.(map[string]interface{})
					require.True(btr.TB(), ok, "att in doc wasn't map[string]interface{}")
					digest := attMap["digest"].(string)

					if _, found := btcc._attachments[digest]; !found {
						missingDigests = append(missingDigests, digest)
					} else if btr.bt.activeSubprotocol == db.CBMobileReplicationV2 {
						// only v2 clients care about proveAttachments
						knownDigests = append(knownDigests, digest)
					}
				}
				btcc.attachmentsLock.RUnlock()

				for _, digest := range knownDigests {
					attData := btcc.getAttachment(digest)
					nonce, proof, err := db.GenerateProofOfAttachment(ctx, attData)
					require.NoError(btr.TB(), err)

					// if we already have this attachment, _we_ should ask the peer whether _they_ have the attachment
					outrq := blip.NewRequest()
					outrq.SetProfile(db.MessageProveAttachment)
					outrq.Properties[db.ProveAttachmentDigest] = digest
					outrq.SetBody(nonce)

					btcc.sendPullMsg(outrq)

					resp := outrq.Response()
					btc.pullReplication.storeMessage(resp)
					respBody, err := resp.Body()
					require.NoError(btr.TB(), err)

					if resp.Type() == blip.ErrorType {
						// forward error from proveAttachment response into rev response
						if !msg.NoReply() {
							response := msg.Response()
							errorCode, _ := strconv.Atoi(resp.Properties["Error-Code"])
							response.SetError(resp.Properties["Error-Code"], errorCode, string(respBody))
						}
						return
					}

					if string(respBody) != proof {
						// forward error from proveAttachment response into rev response
						if !msg.NoReply() {
							response := msg.Response()
							response.SetError(resp.Properties["Error-Code"], http.StatusForbidden, fmt.Sprintf("Incorrect proof for attachment %s", digest))
						}
						return
					}
				}

				for _, digest := range missingDigests {
					outrq := blip.NewRequest()
					outrq.SetProfile(db.MessageGetAttachment)
					outrq.Properties[db.GetAttachmentDigest] = digest
					if btr.bt.activeSubprotocol >= db.CBMobileReplicationV3 {
						outrq.Properties[db.GetAttachmentID] = docID
					}

					btcc.sendPullMsg(outrq)

					resp := outrq.Response()
					btc.pullReplication.storeMessage(resp)
					respBody, err := resp.Body()
					require.NoError(btr.TB(), err)

					if resp.Type() == blip.ErrorType {
						// forward error from getAttachment response into rev response
						if !msg.NoReply() {
							response := msg.Response()
							errorCode, _ := strconv.Atoi(resp.Properties["Error-Code"])
							response.SetError(resp.Properties["Error-Code"], errorCode, string(respBody))
							return
						}
					}

					btcc.attachmentsLock.Lock()
					btcc._attachments[digest] = respBody
					btcc.attachmentsLock.Unlock()
				}
			}

		}

		if bodyJSON != nil {
			body, err = base.JSONMarshal(bodyJSON)
			require.NoError(btr.TB(), err)
		}
		rev := revOptions{
			newVersion:                DocVersion{RevID: revID},
			body:                      body,
			msg:                       msg,
			updateLatestServerVersion: true,
		}
		if replacedRev != "" {
			// store the new sequence for a replaced rev for tests waiting for this specific rev
			rev.replacedVersion = &DocVersion{RevID: replacedRev}
		}
		btcc.addRev(docID, rev)
		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}
}

// handleGetAttachment handles getAttachment messages on the blip tester client
func (btr *BlipTesterReplicator) handleGetAttachment(btc *BlipTesterClient) func(msg *blip.Message) {
	return func(msg *blip.Message) {
		digest, ok := msg.Properties[db.GetAttachmentDigest]
		require.True(btr.TB(), ok, "couldn't find digest in getAttachment message properties")

		btcc := btc.getCollectionClientFromMessage(msg)

		attachment := btcc.getAttachment(digest)

		response := msg.Response()
		response.SetBody(attachment)
		btr.replicationStats.GetAttachment.Add(1)
	}

}

// handleNoRev handles noRev messages on the blip tester client
func (btr *BlipTesterReplicator) handleNoRev(btc *BlipTesterClient) func(msg *blip.Message) {
	return func(msg *blip.Message) {
		btcc := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.NorevMessageId]
		revID := msg.Properties[db.NorevMessageRev]

		btcc.addRev(docID, revOptions{
			newVersion: DocVersion{RevID: revID},
			msg:        msg,
		})
	}

}

// defaultHandler is the default handler for the blip tester client, this will fail the test harness
func (btr *BlipTesterReplicator) defaultHandler() func(msg *blip.Message) {
	return func(msg *blip.Message) {
		require.FailNow(btr.TB(), fmt.Sprintf("Unknown profile: %s caught by client DefaultHandler - msg: %#v", msg.Profile(), msg))
	}
}

// TB returns testing.TB for the current test
func (btr *BlipTesterReplicator) TB() testing.TB {
	return btr.bt.restTester.TB()
}

// TB returns testing.TB for the current test
func (btcc *BlipTesterCollectionClient) TB() testing.TB {
	return btcc.parent.rt.TB()
}

// saveAttachment takes base64 encoded data and stores the attachment on the client.
func (btcc *BlipTesterCollectionClient) saveAttachment(base64data string) (dataLength int, digest string) {
	btcc.attachmentsLock.Lock()
	defer btcc.attachmentsLock.Unlock()

	ctx := base.DatabaseLogCtx(base.TestCtx(btcc.parent.rt.TB()), btcc.parent.rt.GetDatabase().Name, nil)

	data, err := base64.StdEncoding.DecodeString(base64data)
	require.NoError(btcc.TB(), err)

	digest = db.Sha1DigestKey(data)
	if _, found := btcc._attachments[digest]; found {
		base.InfofCtx(ctx, base.KeySync, "attachment with digest %s already exists", digest)
	} else {
		btcc._attachments[digest] = data
	}

	return len(data), digest
}

// getAttachment returns the attachment data for the given digest. The test will fail if the attachment is not found.
func (btcc *BlipTesterCollectionClient) getAttachment(digest string) (attachment []byte) {
	btcc.attachmentsLock.RLock()
	defer btcc.attachmentsLock.RUnlock()

	attachment, found := btcc._attachments[digest]
	require.True(btcc.TB(), found, "attachment with digest %s not found", digest)

	return attachment
}

// updateLastReplicatedRev stores this version as the last version replicated to Sync Gateway.
func (btcc *BlipTesterCollectionClient) updateLastReplicatedRev(docID string, version DocVersion, msg *blip.Message) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	doc._latestServerVersion = version
	rev := doc._revisionsBySeq[doc._seqsByVersions[version]]
	rev.message = msg
}

// getLastReplicatedRev returns the last version replicated to Sync Gateway for the given docID.
func (btcc *BlipTesterCollectionClient) getLastReplicatedRev(docID string) (version DocVersion, ok bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	latestServerVersion := doc._latestServerVersion
	return latestServerVersion, latestServerVersion.RevID != ""
}

func newBlipTesterReplication(tb testing.TB, id string, btc *BlipTesterClient, skipCollectionsInitialization bool) *BlipTesterReplicator {
	bt, err := NewBlipTesterFromSpecWithRT(tb, &BlipTesterSpec{
		connectingPassword:            RestTesterDefaultUserPassword,
		connectingUsername:            btc.Username,
		connectingUserChannelGrants:   btc.Channels,
		blipProtocols:                 btc.SupportedBLIPProtocols,
		skipCollectionsInitialization: skipCollectionsInitialization,
		origin:                        btc.origin,
	}, btc.rt)
	require.NoError(tb, err)

	r := &BlipTesterReplicator{
		id:       id,
		bt:       bt,
		messages: make(map[blip.MessageNumber]*blip.Message),
	}

	r.initHandlers(btc)

	return r
}

// getCollectionsForBLIP returns collections configured by a single database instance on a restTester. If only default collection exists, it will skip returning it to test "legacy" blip mode.
func getCollectionsForBLIP(_ testing.TB, rt *RestTester) []string {
	db := rt.GetDatabase()
	var collections []string
	for _, collection := range db.CollectionByID {
		if base.IsDefaultCollection(collection.ScopeName, collection.Name) {
			continue
		}
		collections = append(collections,
			strings.Join([]string{collection.ScopeName, collection.Name}, base.ScopeCollectionSeparator))
	}
	slices.Sort(collections)
	return collections
}

func (btcRunner *BlipTestClientRunner) NewBlipTesterClientOptsWithRT(rt *RestTester, opts *BlipTesterClientOpts) (client *BlipTesterClient) {
	require.True(btcRunner.TB(), btcRunner.initialisedInsideRunnerCode, "must call BlipTestClientRunner.NewBlipTesterClientRunner from inside BlipTestClientRunner.Run() method")
	if opts == nil {
		opts = &BlipTesterClientOpts{}
	}
	id, err := uuid.NewRandom()
	require.NoError(btcRunner.TB(), err)

	client = &BlipTesterClient{
		BlipTesterClientOpts: *opts,
		rt:                   rt,
		id:                   id.ID(),
	}
	btcRunner.clients[client.id] = client
	client.createBlipTesterReplications()

	return client
}

// TB returns testing.TB for the current test
func (btc *BlipTesterClient) TB() testing.TB {
	return btc.rt.TB()
}

// Close shuts down all the clients and clears all messages stored.
func (btc *BlipTesterClient) Close() {
	btc.tearDownBlipClientReplications()
	for _, collectionClient := range btc.collectionClients {
		collectionClient.Close()
	}
	if btc.nonCollectionAwareClient != nil {
		btc.nonCollectionAwareClient.Close()
	}
}

// TB returns testing.TB for the current test
func (btcRunner *BlipTestClientRunner) TB() testing.TB {
	return btcRunner.t
}

// Run is the main entry point for running the blip tester client and its associated methods in test framework and should be used instead of t.Run
func (btcRunner *BlipTestClientRunner) Run(test func(t *testing.T, SupportedBLIPProtocols []string)) {
	btcRunner.initialisedInsideRunnerCode = true
	// reset to protect against someone creating a new client after Run() is run
	defer func() { btcRunner.initialisedInsideRunnerCode = false }()
	btcRunner.t.Run("revTree", func(t *testing.T) {
		test(t, []string{db.CBMobileReplicationV3.SubprotocolString()})
	})
}

// tearDownBlipClientReplications closes the push and pull replications for the client.
func (btc *BlipTesterClient) tearDownBlipClientReplications() {
	btc.pullReplication.Close()
	btc.pushReplication.Close()
}

// createBlipTesterReplications creates the push and pull replications for the client.
func (btc *BlipTesterClient) createBlipTesterReplications() {
	id, err := uuid.NewRandom()
	require.NoError(btc.TB(), err)

	btc.pushReplication = newBlipTesterReplication(btc.TB(), "push"+id.String(), btc, btc.BlipTesterClientOpts.SkipCollectionsInitialization)
	btc.pullReplication = newBlipTesterReplication(btc.TB(), "pull"+id.String(), btc, btc.BlipTesterClientOpts.SkipCollectionsInitialization)

	collections := getCollectionsForBLIP(btc.TB(), btc.rt)
	if !btc.BlipTesterClientOpts.SkipCollectionsInitialization && len(collections) > 0 {
		btc.collectionClients = make([]*BlipTesterCollectionClient, len(collections))
		for i, collection := range collections {
			btc.initCollectionReplication(collection, i)
		}
	} else {
		btc.nonCollectionAwareClient = NewBlipTesterCollectionClient(btc)
	}

	btc.pullReplication.bt.avoidRestTesterClose = true
	btc.pushReplication.bt.avoidRestTesterClose = true
}

// initCollectionReplication initializes a BlipTesterCollectionClient for the given collection.
func (btc *BlipTesterClient) initCollectionReplication(collection string, collectionIdx int) {
	btcReplicator := NewBlipTesterCollectionClient(btc)
	btcReplicator.collection = collection
	btcReplicator.collectionIdx = collectionIdx
	btc.collectionClients[collectionIdx] = btcReplicator
}

// waitForReplicationMessage waits for a replication message with the given serial number.
func (btc *BlipTesterClient) waitForReplicationMessage(collection *db.DatabaseCollection, serialNumber blip.MessageNumber) *blip.Message {
	if base.IsDefaultCollection(collection.ScopeName, collection.Name) {
		return btc.pushReplication.WaitForMessage(serialNumber)
	}
	return btc.pushReplication.WaitForMessage(serialNumber + 1)
}

// getMostRecentChangesMessage returns the most recent non nil changes message received from the pull replication. This represents the latest set of changes.
func (btc *BlipTesterClient) getMostRecentChangesMessage() *blip.Message {
	var highestMsgSeq uint32
	var highestSeqMsg *blip.Message
	// Grab most recent changes message
	for _, message := range btc.pullReplication.GetMessages() {
		if message.Properties["Profile"] != db.MessageChanges {
			continue
		}
		messageBody, err := message.Body()
		require.NoError(btc.TB(), err)
		if string(messageBody) == "null" {
			continue
		}
		if highestMsgSeq >= uint32(message.SerialNumber()) {
			continue
		}
		highestMsgSeq = uint32(message.SerialNumber())
		highestSeqMsg = message
	}
	return highestSeqMsg
}

// SingleCollection returns a single collection blip tester if the RestTester database is configured with only one collection. Otherwise, throw a fatal test error.
func (btcRunner *BlipTestClientRunner) SingleCollection(clientID uint32) *BlipTesterCollectionClient {
	if btcRunner.clients[clientID].nonCollectionAwareClient != nil {
		return btcRunner.clients[clientID].nonCollectionAwareClient
	}
	require.Equal(btcRunner.clients[clientID].TB(), 1, len(btcRunner.clients[clientID].collectionClients))
	return btcRunner.clients[clientID].collectionClients[0]
}

// Collection return a collection blip tester by name, if configured in the RestTester database. Otherwise, throw a fatal test error.
func (btcRunner *BlipTestClientRunner) Collection(clientID uint32, collectionName string) *BlipTesterCollectionClient {
	if collectionName == "_default._default" && btcRunner.clients[clientID].nonCollectionAwareClient != nil {
		return btcRunner.clients[clientID].nonCollectionAwareClient
	}
	for _, collectionClient := range btcRunner.clients[clientID].collectionClients {
		if collectionClient.collection == collectionName {
			return collectionClient
		}
	}
	require.FailNow(btcRunner.clients[clientID].TB(), fmt.Sprintf("Could not find collection %s in BlipTesterClient", collectionName))
	return nil
}

// BlipTesterPushOptions
type BlipTesterPushOptions struct {
	Continuous bool
	Since      string

	// TODO: Not Implemented
	// Channels   string
	// DocIDs     []string
	// changesBatchSize int
}

// StartPush will begin a continuous push replication since 0 between the client and server
func (btcc *BlipTesterCollectionClient) StartPush() {
	btcc.StartPushWithOpts(BlipTesterPushOptions{Continuous: true, Since: "0"})
}

// TODO: CBG-4401 Implement opts.changesBatchSize and raise default batch to ~20-200 to match real CBL client
const changesBatchSize = 1

type proposeChangeBatchEntry struct {
	docID               string
	version             DocVersion
	seq                 clientSeq
	history             []DocVersion
	latestServerVersion DocVersion
}

func (e proposeChangeBatchEntry) historyStr() string {
	sb := strings.Builder{}
	for i, version := range e.history {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(version.RevID)
	}
	return sb.String()
}

// StartPull will begin a push replication with the given options between the client and server
func (btcc *BlipTesterCollectionClient) StartPushWithOpts(opts BlipTesterPushOptions) {
	ctx := btcc.ctx
	sinceFromStr, err := db.ParsePlainSequenceID(opts.Since)
	require.NoError(btcc.TB(), err)
	seq := clientSeq(sinceFromStr.SafeSequence())
	btcc.goroutineWg.Add(1)
	go func() {
		defer btcc.goroutineWg.Done()
		// TODO: CBG-4401 wire up opts.changesBatchSize and implement a flush timeout for when the client doesn't fill the batch
		changesBatch := make([]proposeChangeBatchEntry, 0, changesBatchSize)
		base.DebugfCtx(ctx, base.KeySGTest, "Starting push replication iteration with since=%v", seq)
		for change := range btcc.changesSince(btcc.ctx, seq, opts.Continuous) {
			changesBatch = append(changesBatch, *change)
			if len(changesBatch) >= changesBatchSize {
				base.DebugfCtx(ctx, base.KeySGTest, "Sending batch of %d changes", len(changesBatch))
				btcc.sendRevisions(ctx, changesBatch)
				changesBatch = changesBatch[:0]
			}

		}
	}()
}

// sendProposeChanges sends a proposeChanges request to the server for the given changes, waits for a response and returns it.
func (btcc *BlipTesterCollectionClient) sendProposeChanges(ctx context.Context, changesBatch []proposeChangeBatchEntry) *blip.Message {
	proposeChangesRequest := blip.NewRequest()
	proposeChangesRequest.SetProfile(db.MessageProposeChanges)

	proposeChangesRequestBody := bytes.NewBufferString(`[`)
	for i, change := range changesBatch {
		if i > 0 {
			proposeChangesRequestBody.WriteString(",")
		}
		proposeChangesRequestBody.WriteString(fmt.Sprintf(`["%s","%s"`, change.docID, change.version.RevID))
		// write last known server version to support no-conflict mode
		if serverVersion, ok := btcc.getLastReplicatedRev(change.docID); ok {
			base.DebugfCtx(ctx, base.KeySGTest, "specifying last known server version for doc %s = %v", change.docID, serverVersion)
			proposeChangesRequestBody.WriteString(fmt.Sprintf(`,"%s"`, serverVersion.RevID))
		}
		proposeChangesRequestBody.WriteString(`]`)
	}
	proposeChangesRequestBody.WriteString(`]`)
	proposeChangesRequestBodyBytes := proposeChangesRequestBody.Bytes()
	proposeChangesRequest.SetBody(proposeChangesRequestBodyBytes)

	base.DebugfCtx(ctx, base.KeySGTest, "proposeChanges request: %s", string(proposeChangesRequestBodyBytes))

	btcc.addCollectionProperty(proposeChangesRequest)

	btcc.sendPushMsg(proposeChangesRequest)

	proposeChangesResponse := proposeChangesRequest.Response()
	rspBody, err := proposeChangesResponse.Body()
	require.NoError(btcc.TB(), err)
	require.NotContains(btcc.TB(), proposeChangesResponse.Properties, "Error-Domain", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)
	require.NotContains(btcc.TB(), proposeChangesResponse.Properties, "Error-Code", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)

	base.DebugfCtx(ctx, base.KeySGTest, "proposeChanges response: %s", string(rspBody))
	return proposeChangesResponse
}

// sends a rev request to the server for the given change, waits for response and updates the last known replication.
func (btcc *BlipTesterCollectionClient) sendRev(ctx context.Context, change proposeChangeBatchEntry, deltasSupported bool) {
	revRequest := blip.NewRequest()
	revRequest.SetProfile(db.MessageRev)
	revRequest.Properties[db.RevMessageID] = change.docID
	revRequest.Properties[db.RevMessageRev] = change.version.RevID
	revRequest.Properties[db.RevMessageHistory] = change.historyStr()

	var deltaVersion *DocVersion
	var docBody []byte
	if deltasSupported {
		docBody, deltaVersion = btcc.getDeltaBody(change.docID, change.version, change.latestServerVersion)
	} else {
		docBody = btcc.getBodyBytes(change.docID, change.version)
	}
	if deltasSupported && deltaVersion != nil {
		base.DebugfCtx(ctx, base.KeySGTest, "specifying last known server version as deltaSrc for doc %s = %v", change.docID, deltaVersion)
		revRequest.Properties[db.RevMessageDeltaSrc] = deltaVersion.RevID
	}
	revRequest.SetBody(docBody)

	btcc.addCollectionProperty(revRequest)
	btcc.sendPushMsg(revRequest)
	base.DebugfCtx(ctx, base.KeySGTest, "sent doc %s / %v", change.docID, change.version)
	// block until remote has actually processed the rev and sent a response
	revResp := revRequest.Response()
	require.NotContains(btcc.TB(), revResp.Properties, "Error-Domain", "unexpected error response from rev %v: %s", revResp)
	base.DebugfCtx(ctx, base.KeySGTest, "peer acked rev %s / %v", change.docID, change.version)
	btcc.updateLastReplicatedRev(change.docID, change.version, revRequest)
}

func (btcc *BlipTesterCollectionClient) sendRevisions(ctx context.Context, changesBatch []proposeChangeBatchEntry) {
	proposeChangesResponse := btcc.sendProposeChanges(ctx, changesBatch)

	var serverDeltas bool
	if proposeChangesResponse.Properties[db.ChangesResponseDeltas] == "true" {
		base.DebugfCtx(ctx, base.KeySGTest, "server supports deltas")
		serverDeltas = true
	}
	rspBody, err := proposeChangesResponse.Body()
	require.NoError(btcc.TB(), err)
	var response []int
	err = base.JSONUnmarshal(rspBody, &response)
	require.NoError(btcc.TB(), err)
	for i, change := range changesBatch {
		var status int
		if i >= len(response) {
			// trailing zeros are removed - treat as 0 from now on
			status = 0
		} else {
			status = response[i]
		}
		switch status {
		case 0:
			btcc.sendRev(ctx, change, serverDeltas && btcc.parent.ClientDeltas)
		case 304:
			// peer already has doc version
			base.DebugfCtx(ctx, base.KeySGTest, "peer already has doc %s / %v", change.docID, change.version)
			continue
		case 409:
			// conflict - puller will need to resolve (if enabled) - resolution pushed independently so we can ignore this one
			base.DebugfCtx(ctx, base.KeySGTest, "conflict for doc %s clientVersion:%v serverVersion:%v", change.docID, change.version, change.latestServerVersion)
			continue
		default:
			btcc.TB().Errorf("unexpected status %d for doc %s / %s", status, change.docID, change.version)
			return
		}
	}
}

// StartPull will begin a continuous pull replication since 0 between the client and server
func (btcc *BlipTesterCollectionClient) StartPull() {
	btcc.StartPullSince(BlipTesterPullOptions{Continuous: true, Since: "0"})
}

// StartOneShotPull will begin a one-shot pull replication since 0 and continuous=false between the client and server
func (btcc *BlipTesterCollectionClient) StartOneshotPull() {
	btcc.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0"})
}

// BlipTesterPullOptions represents options passed to StartPull (SubChanges) functions
type BlipTesterPullOptions struct {
	ActiveOnly  bool
	Channels    string
	Continuous  bool
	DocIDs      []string
	RequestPlus bool
	Since       string
}

// StartPullSince will begin a pull replication between the client and server with the given params.
func (btcc *BlipTesterCollectionClient) StartPullSince(options BlipTesterPullOptions) {
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile(db.MessageSubChanges)
	subChangesRequest.Properties[db.SubChangesContinuous] = fmt.Sprintf("%t", options.Continuous)
	subChangesRequest.Properties[db.SubChangesSince] = options.Since
	subChangesRequest.Properties[db.SubChangesActiveOnly] = fmt.Sprintf("%t", options.ActiveOnly)
	if options.Channels != "" {
		subChangesRequest.Properties[db.SubChangesFilter] = base.ByChannelFilter
		subChangesRequest.Properties[db.SubChangesChannels] = options.Channels
	}
	if options.RequestPlus {
		subChangesRequest.Properties[db.SubChangesRequestPlus] = "true"
	}
	subChangesRequest.SetNoReply(true)

	if btcc.parent.BlipTesterClientOpts.SendRevocations {
		subChangesRequest.Properties[db.SubChangesRevocations] = "true"
	}

	if btcc.parent.BlipTesterClientOpts.sendReplacementRevs {
		subChangesRequest.Properties[db.SubChangesSendReplacementRevs] = "true"
	}

	if len(options.DocIDs) > 0 {
		subChangesRequest.SetBody(base.MustJSONMarshal(btcc.TB(),
			db.SubChangesBody{
				DocIDs: options.DocIDs,
			},
		))
	}
	btcc.sendPullMsg(subChangesRequest)
}

// UnsubPullChanges will send an UnsubChanges message to the server to stop the pull replication. Fails test harness if Sync Gateway responds with an error.
func (btcc *BlipTesterCollectionClient) UnsubPullChanges() {
	unsubChangesRequest := blip.NewRequest()
	unsubChangesRequest.SetProfile(db.MessageUnsubChanges)

	btcc.sendPullMsg(unsubChangesRequest)

	response, err := unsubChangesRequest.Response().Body()
	require.NoError(btcc.TB(), err)
	require.Empty(btcc.TB(), response)
}

// NewBlipTesterCollectionClient creates a collection specific client from a BlipTesterClient
func NewBlipTesterCollectionClient(btc *BlipTesterClient) *BlipTesterCollectionClient {
	ctx, ctxCancel := context.WithCancel(btc.rt.Context())
	l := sync.RWMutex{}
	c := &BlipTesterCollectionClient{
		ctx:           ctx,
		ctxCancel:     ctxCancel,
		seqLock:       &l,
		_seqStore:     make(map[clientSeq]*clientDoc),
		_seqFromDocID: make(map[string]clientSeq),
		_seqCond:      sync.NewCond(&l),
		_attachments:  make(map[string][]byte),
		parent:        btc,
	}
	globalBlipTesterClients.add(btc.TB().Name())
	return c
}

// Close will empty the stored docs and close the underlying replications.
func (btcc *BlipTesterCollectionClient) Close() {
	btcc.ctxCancel()

	// wake up changes feeds to exit - don't need lock for sync.Cond
	btcc._seqCond.Broadcast()

	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	// empty storage
	btcc._seqStore = make(map[clientSeq]*clientDoc, 0)
	btcc._seqFromDocID = make(map[string]clientSeq, 0)

	btcc.attachmentsLock.Lock()
	defer btcc.attachmentsLock.Unlock()
	btcc._attachments = make(map[string][]byte, 0)
	globalBlipTesterClients.remove(btcc.TB(), btcc.TB().Name())
}

// sendMsg sends a blip message to the server and stores it on BlipTesterReplicator. The response is not read unless the caller calls msg.Response()
func (btr *BlipTesterReplicator) sendMsg(msg *blip.Message) {
	require.True(btr.TB(), btr.bt.sender.Send(msg))
	btr.storeMessage(msg)
}

// upsertDoc will create or update the doc based on whether parentVersion is passed or not. Enforces MVCC update.
func (btcc *BlipTesterCollectionClient) upsertDoc(docID string, parentVersion *DocVersion, body []byte) *clientDocRev {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	oldSeq, ok := btcc._seqFromDocID[docID]
	var doc *clientDoc
	if ok {
		require.NotNil(btcc.TB(), parentVersion, "docID: %v already exists on the client with seq: %v - expecting to create doc based on not nil parentVersion", docID, oldSeq)
		doc, ok = btcc._seqStore[oldSeq]
		require.True(btcc.TB(), ok, "seq %q for docID %q found but no doc in _seqStore", oldSeq, docID)
	} else {
		require.Nil(btcc.TB(), parentVersion, "docID: %v was not found on the client - expecting to create doc based on nil parentVersion, parentVersion=%v", docID, parentVersion)
		doc = newClientDocument(docID, 0, nil)
	}
	newGen := 1
	if parentVersion != nil {
		// grab latest version for this doc and make sure we're doing an upsert on top of it to avoid branching revisions
		latestRev := doc._latestRev(btcc.TB())
		latestVersion := latestRev.version
		require.Equal(btcc.TB(), *parentVersion, latestVersion, "latest version for docID: %v is %v, expected parentVersion: %v", docID, latestVersion, parentVersion)
		newGen = parentVersion.RevIDGeneration() + 1
	}

	body = btcc.ProcessInlineAttachments(body, newGen)

	digest := "abc" // TODO: Generate rev ID digest based on body hash?

	newRevID := fmt.Sprintf("%d-%s", newGen, digest)
	newSeq := btcc._nextSequence()
	rev := clientDocRev{clientSeq: newSeq, version: DocVersion{RevID: newRevID}, body: body}
	doc._addNewRev(rev)

	btcc._seqStore[newSeq] = doc
	btcc._seqFromDocID[docID] = newSeq
	delete(btcc._seqStore, oldSeq)

	// new sequence written, wake up changes feeds
	btcc._seqCond.Broadcast()

	return &rev
}

// AddRev creates a revision on the client.
// The rev ID is always: "N-abc", where N is rev generation for predictability.
func (btcc *BlipTesterCollectionClient) AddRev(docID string, parentVersion *DocVersion, body []byte) DocVersion {
	newRev := btcc.upsertDoc(docID, parentVersion, body)
	return newRev.version
}

func (btcc *BlipTesterCollectionClient) ProcessInlineAttachments(inputBody []byte, revGen int) (outputBody []byte) {
	if !bytes.Contains(inputBody, []byte(db.BodyAttachments)) {
		return inputBody
	}

	var newDocJSON map[string]interface{}
	require.NoError(btcc.TB(), base.JSONUnmarshal(inputBody, &newDocJSON))
	attachments, ok := newDocJSON[db.BodyAttachments]
	if !ok {
		return inputBody
	}
	attachmentMap, ok := attachments.(map[string]interface{})
	require.True(btcc.TB(), ok)
	for attachmentName, inlineAttachment := range attachmentMap {
		inlineAttachmentMap := inlineAttachment.(map[string]interface{})
		attachmentData, ok := inlineAttachmentMap["data"]
		if !ok {
			isStub, _ := inlineAttachmentMap["stub"].(bool)
			require.True(btcc.TB(), isStub, "couldn't find data and stub property for inline attachment %#v : %v", attachmentName, inlineAttachmentMap)
			// push the stub as-is
			continue
		}

		// Transform inline attachment data into metadata
		data, ok := attachmentData.(string)
		require.True(btcc.TB(), ok, "inline attachment data was not a string, got %T", attachmentData)

		length, digest := btcc.saveAttachment(data)

		attachmentMap[attachmentName] = map[string]interface{}{
			"digest": digest,
			"length": length,
			"revpos": revGen,
			"stub":   true,
		}
		newDocJSON[db.BodyAttachments] = attachmentMap
	}
	return base.MustJSONMarshal(btcc.TB(), newDocJSON)
}

// GetVersion returns the data stored in the Client under the given docID and version
func (btcc *BlipTesterCollectionClient) GetVersion(docID string, docVersion DocVersion) (data []byte, found bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	if !ok {
		return nil, false
	}
	revSeq, ok := doc._seqsByVersions[docVersion]
	if !ok {
		return nil, false
	}

	rev, ok := doc._revisionsBySeq[revSeq]
	require.True(btcc.TB(), ok, "seq %q for docID %q found but no rev in _seqStore", revSeq, docID)

	return rev.body, true
}

// WaitForVersion blocks until the given document version has been stored by the client, and returns the data when found. The test will fail after 10 seconds if a matching document is not found.
func (btcc *BlipTesterCollectionClient) WaitForVersion(docID string, docVersion DocVersion) (data []byte) {
	if data, found := btcc.GetVersion(docID, docVersion); found {
		return data
	}
	require.EventuallyWithT(btcc.TB(), func(c *assert.CollectT) {
		var found bool
		data, found = btcc.GetVersion(docID, docVersion)
		assert.True(c, found, "Could not find docID:%+v Version %+v", docID, docVersion)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v Version %+v", docID, docVersion)
	return data
}

// getBody returns the body for a specific revision. This will fail the test harness if not present.
func (btcc *BlipTesterCollectionClient) getBody(docID string, version DocVersion) db.Body {
	rawDoc := btcc.getBodyBytes(docID, version)
	var body db.Body
	require.NoError(btcc.TB(), body.Unmarshal(rawDoc))
	return body
}

// getBody returns the body for a specific revision. This will fail the test harness if not present.
func (btcc *BlipTesterCollectionClient) getBodyBytes(docID string, version DocVersion) []byte {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found", docID)
	return doc._getRev(btcc.TB(), version).body
}

// getDeltaBody returns the body of the given docID at a given version. If the serverVersion is available locally and not a tombstone, create the body as a delta. If there is no version available to make a delta, the parentVerison will be nil and the full body of the document at the version will be returned.
func (btcc *BlipTesterCollectionClient) getDeltaBody(docID string, version DocVersion, serverVersion DocVersion) (body []byte, parentVersion *DocVersion) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found", docID)
	rev := doc._getRev(btcc.TB(), version)
	serverRev, ok := doc._revisionsBySeq[doc._seqsByVersions[serverVersion]]
	if !ok || serverRev.isDelete {
		return rev.body, nil
	}
	var deltaBody db.Body
	require.NoError(btcc.TB(), deltaBody.Unmarshal(serverRev.body), "serverRev=%+v", serverRev)
	var newBodyUnmarshalled db.Body
	require.NoError(btcc.TB(), newBodyUnmarshalled.Unmarshal(rev.body))
	delta, err := base.Diff(deltaBody, newBodyUnmarshalled)
	require.NoError(btcc.TB(), err)

	return delta, &serverRev.version
}

// GetDoc returns a rev stored in the Client under the given docID.  (if multiple revs are present, rev body returned is non-deterministic)
func (btcc *BlipTesterCollectionClient) GetDoc(docID string) (data []byte, found bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	if !ok {
		return nil, false
	}

	latestRev := doc._latestRev(btcc.TB())
	return latestRev.body, true
}

// WaitForDoc blocks until any document with the doc ID has been stored by the client, and returns the document body when found. If a document will be reported multiple times, the latest copy of the document is returned (not necessarily the first). The test will fail after 10 seconds if the document
func (btcc *BlipTesterCollectionClient) WaitForDoc(docID string) (data []byte) {

	if data, found := btcc.GetDoc(docID); found {
		return data
	}
	require.EventuallyWithT(btcc.TB(), func(c *assert.CollectT) {
		var found bool
		data, found = btcc.GetDoc(docID)
		assert.True(c, found, "Could not find docID:%+v", docID)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v", docID)
	return data
}

// GetMessage returns the message stored in the Client under the given serial number
func (btr *BlipTesterReplicator) GetMessage(serialNumber blip.MessageNumber) (msg *blip.Message, found bool) {
	btr.messagesLock.RLock()
	defer btr.messagesLock.RUnlock()

	if msg, ok := btr.messages[serialNumber]; ok {
		return msg, ok
	}

	return nil, false
}

// GetMessages returns a map of all messages stored in the Client keyed by serial number. These messages are mutable, but the response of the messages has been received so they should be effectively immutable.
func (btr *BlipTesterReplicator) GetMessages() map[blip.MessageNumber]*blip.Message {
	btr.messagesLock.RLock()
	defer btr.messagesLock.RUnlock()

	messages := make(map[blip.MessageNumber]*blip.Message, len(btr.messages))
	for k, v := range btr.messages {
		// Read the body before copying, since it might be read asynchronously
		_, _ = v.Body()
		messages[k] = v
	}

	return messages
}

// WaitForMessage blocks until the given message serial number has been stored by the replicator, and returns the message when found. The test will fail if message is not found after 10 seconds.
func (btr *BlipTesterReplicator) WaitForMessage(serialNumber blip.MessageNumber) (msg *blip.Message) {
	require.EventuallyWithT(btr.TB(), func(c *assert.CollectT) {
		var ok bool
		msg, ok = btr.GetMessage(serialNumber)
		assert.True(c, ok)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterReplicator timed out waiting for BLIP message: %v", serialNumber)
	return msg
}

func (btr *BlipTesterReplicator) storeMessage(msg *blip.Message) {
	btr.messagesLock.Lock()
	defer btr.messagesLock.Unlock()
	btr.messages[msg.SerialNumber()] = msg
}

// WaitForBlipRevMessage blocks until the given doc ID and rev ID has been stored by the client, and returns the message when found. If not found after 10 seconds, test will fail.
func (btcc *BlipTesterCollectionClient) WaitForBlipRevMessage(docID string, docVersion DocVersion) (msg *blip.Message) {
	require.EventuallyWithT(btcc.TB(), func(c *assert.CollectT) {
		var ok bool
		msg, ok = btcc.GetBlipRevMessage(docID, docVersion)
		assert.True(c, ok, "Could not find docID:%+v, RevID: %+v", docID, docVersion.RevID)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterReplicator timed out waiting for BLIP message")
	require.NotNil(btcc.TB(), msg)
	return msg
}

// GetBlipRevMessage returns the rev message that wrote the given docID/DocVersion on the client.
func (btcc *BlipTesterCollectionClient) GetBlipRevMessage(docID string, version DocVersion) (msg *blip.Message, found bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()

	if doc, ok := btcc._getClientDoc(docID); ok {
		if seq, ok := doc._seqsByVersions[version]; ok {
			if rev, ok := doc._revisionsBySeq[seq]; ok {
				require.NotNil(btcc.TB(), rev.message, "rev.message is nil for docID:%+v, version: %+v", docID, version)
				return rev.message, true
			}
		}
	}

	return nil, false
}

func (btcRunner *BlipTestClientRunner) StartPull(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartPull()
}

func (btcRunner *BlipTestClientRunner) StartPush(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartPush()
}

func (btcRunner *BlipTestClientRunner) StartPushWithOpts(clientID uint32, opts BlipTesterPushOptions) {
	btcRunner.SingleCollection(clientID).StartPushWithOpts(opts)
}

// WaitForVersion blocks until the given document version has been stored by the client, and returns the data when found or fails test if document is not found after 10 seconds.
func (btcRunner *BlipTestClientRunner) WaitForVersion(clientID uint32, docID string, docVersion DocVersion) (data []byte) {
	return btcRunner.SingleCollection(clientID).WaitForVersion(docID, docVersion)
}

// WaitForBlipRevMessage blocks until any blip message with a given docID has been stored by the client, and returns the message when found. If document is not not found after 10 seconds, test will fail.
func (btcRunner *BlipTestClientRunner) WaitForDoc(clientID uint32, docID string) []byte {
	return btcRunner.SingleCollection(clientID).WaitForDoc(docID)
}

// WaitForBlipRevMessage blocks until the given doc ID and rev ID has been stored by the client, and returns the message when found. If document is not found after 10 seconds, test will fail.
func (btcRunner *BlipTestClientRunner) WaitForBlipRevMessage(clientID uint32, docID string, docVersion DocVersion) *blip.Message {
	return btcRunner.SingleCollection(clientID).WaitForBlipRevMessage(docID, docVersion)
}

func (btcRunner *BlipTestClientRunner) StartOneshotPull(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartOneshotPull()
}

// AddRev creates a revision on the client.
// The rev ID is always: "N-abc", where N is rev generation for predictability.
func (btcRunner *BlipTestClientRunner) AddRev(clientID uint32, docID string, version *DocVersion, body []byte) DocVersion {
	return btcRunner.SingleCollection(clientID).AddRev(docID, version, body)
}

func (btcRunner *BlipTestClientRunner) StartPullSince(clientID uint32, options BlipTesterPullOptions) {
	btcRunner.SingleCollection(clientID).StartPullSince(options)
}

func (btcRunner *BlipTestClientRunner) GetVersion(clientID uint32, docID string, docVersion DocVersion) ([]byte, bool) {
	return btcRunner.SingleCollection(clientID).GetVersion(docID, docVersion)
}

// saveAttachment takes base64 encoded data and stores the attachment on the client.
func (btcRunner *BlipTestClientRunner) saveAttachment(clientID uint32, attachmentData string) (int, string) {
	return btcRunner.SingleCollection(clientID).saveAttachment(attachmentData)
}

// UnsubPullChanges will send an UnsubChanges message to the server to stop the pull replication. Fails test harness if Sync Gateway responds with an error.
func (btcRunner *BlipTestClientRunner) UnsubPullChanges(clientID uint32) {
	btcRunner.SingleCollection(clientID).UnsubPullChanges()
}

// addCollectionProperty adds a collection index to the message properties.
func (btcc *BlipTesterCollectionClient) addCollectionProperty(msg *blip.Message) {
	if btcc.collection != "" {
		msg.Properties[db.BlipCollection] = strconv.Itoa(btcc.collectionIdx)
	}
}

// addCollectionProperty will automatically add a collection. If we are running with the default collection, or a single named collection, automatically add the right value. If there are multiple collections on the database, the test will fatally exit, since the behavior is undefined.
func (btc *BlipTesterClient) addCollectionProperty(msg *blip.Message) {
	if btc.nonCollectionAwareClient == nil {
		require.Equal(btc.TB(), 1, len(btc.collectionClients), "Multiple collection clients, exist so assuming that the only named collection is the single element of an array is not valid")
		msg.Properties[db.BlipCollection] = "0"
	}
}

// getCollectionClientFromMessage returns a the right blip tester client. This works automatically when BlipTesterClient is initialized when skipCollectionsInitialization is false.
func (btc *BlipTesterClient) getCollectionClientFromMessage(msg *blip.Message) *BlipTesterCollectionClient {
	collectionIdx, exists := msg.Properties[db.BlipCollection]
	if !exists {
		// If a collection property is passed, assume that the BlipTesterClient hasn't been initialized with collections.
		// If this fails, this means a message wasn't sent with the correct BlipCollection property, see use of addCollectionProperty
		require.NotNil(btc.TB(), btc.nonCollectionAwareClient)
		return btc.nonCollectionAwareClient
	}

	require.NotEqual(btc.TB(), "", collectionIdx, "no collection given in %q message", msg.Profile())

	idx, err := strconv.Atoi(collectionIdx)
	require.NoError(btc.TB(), err)
	require.Greater(btc.TB(), len(btc.collectionClients), idx)
	return btc.collectionClients[idx]
}

// sendPullMsg sends a message to the server and stores the message locally. This function does not wait for a response.
func (btcc *BlipTesterCollectionClient) sendPullMsg(msg *blip.Message) {
	btcc.addCollectionProperty(msg)
	btcc.parent.pullReplication.sendMsg(msg)
}

// sendPushMsg sends a message to the server and stores the message locally. This function does not wait for a response.
func (btcc *BlipTesterCollectionClient) sendPushMsg(msg *blip.Message) {
	btcc.addCollectionProperty(msg)
	btcc.parent.pushReplication.sendMsg(msg)
}

// lastSeq returns the latest sequence number for this collection.
func (btcc *BlipTesterCollectionClient) lastSeq() clientSeq {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	return btcc._seqLast
}

// _nextSequence returns the next sequence number for this collection.
func (btcc *BlipTesterCollectionClient) _nextSequence() clientSeq {
	btcc._seqLast++
	return btcc._seqLast
}

// _pruneVersion removes the given version from the specified doc. This is not allowed for the latest version of a document.
func (btcc *BlipTesterCollectionClient) pruneVersion(docID string, version DocVersion) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found")
	doc._pruneVersion(btcc.TB(), version)
}

type revOptions struct {
	body                      []byte        // body of the revision, nil if this is a deletion
	msg                       *blip.Message // message that wrote this revision, could be a rev or changes messages
	isDelete                  bool          // isDelete is true if this revision is a tombstone
	newVersion                DocVersion    // newVersion is the version of the new revision
	replacedVersion           *DocVersion   // replacedVersion is the version of the revision that was replaced, to update the global structure of all docIDs<->rev
	updateLatestServerVersion bool          // updateLatestServerVersion is true if the latestServerVersion should be updated to the newVersion. This only keeps track of a single Sync Gateway.
}

// addRev adds a revision for a specific document.
func (btcc *BlipTesterCollectionClient) addRev(docID string, opts revOptions) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	newClientSeq := btcc._nextSequence()

	docRev := clientDocRev{
		clientSeq: newClientSeq,
		version:   opts.newVersion,
		body:      opts.body,
		isDelete:  opts.isDelete,
		message:   opts.msg,
	}

	doc, ok := btcc._getClientDoc(docID)
	if !ok {
		doc = newClientDocument(docID, newClientSeq, &docRev)
	} else {
		// remove existing entry and replace with new seq
		delete(btcc._seqStore, doc._latestSeq)
		doc._addNewRev(docRev)
	}
	btcc._seqStore[newClientSeq] = doc
	btcc._seqFromDocID[docID] = newClientSeq

	if opts.replacedVersion != nil {
		// store the new sequence for a replaced rev for tests waiting for this specific rev
		doc._seqsByVersions[*opts.replacedVersion] = newClientSeq
	}
	if opts.updateLatestServerVersion {
		doc._latestServerVersion = docRev.version
	}
}

// getAllRevisions returns all revisions for a given docID
func (btcc *BlipTesterCollectionClient) getAllRevisions(docID string) []DocVersion {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._getClientDoc(docID)
	if !ok {
		return nil
	}
	docSeqs := doc._docRevSeqsNewestToOldest()
	versions := make([]DocVersion, 0, len(docSeqs))
	for _, seq := range docSeqs {
		versions = append(versions, doc._revisionBySeq(btcc.TB(), seq).version)
	}
	return versions
}
