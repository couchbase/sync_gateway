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

// getClientDocForSeq returns the clientDoc for the given sequence number, if it exists.
func (btcc *BlipTesterCollectionClient) getClientDocForSeq(seq clientSeq) (*clientDoc, bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	doc, ok := btcc._seqStore[seq]
	return doc, ok
}

// OneShotDocsSince is an iterator that yields client sequence and document pairs that are newer than the given since value.
func (btcc *BlipTesterCollectionClient) OneShotDocsSince(ctx context.Context, since clientSeq) iter.Seq2[clientSeq, *clientDoc] {
	return func(yield func(clientSeq, *clientDoc) bool) {
		btcc.seqLock.Lock()
		seqLast := btcc._seqLast
		for btcc._seqLast <= since {
			if ctx.Err() != nil {
				btcc.seqLock.Unlock()
				return
			}
			// block until new seq
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - waiting for new sequence", since, btcc._seqLast)
			btcc._seqCond.Wait()
			// Check to see if we were woken because of Close()
			if ctx.Err() != nil {
				btcc.seqLock.Unlock()
				return
			}
			seqLast = btcc._seqLast
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - woke up", since, btcc._seqLast)
		}
		btcc.seqLock.Unlock()
		base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - iterating", since, seqLast)
		for seq := since; seq <= seqLast; seq++ {
			doc, ok := btcc.getClientDocForSeq(seq)
			// filter non-latest entries in cases where we haven't pruned _seqStore
			if !ok {
				continue
			}
			// make sure that seq is latestseq
			require.Equal(btcc.TB(), doc.latestSeq(), seq, "this should've been pruned out!")
			if !yield(seq, doc) {
				base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - stopping iteration", since, seqLast)
				return
			}
		}
		base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - done", since, seqLast)
	}
}

// docsSince returns a channel which will yield client documents that are newer than the given since value.
// The channel will be closed when the iteration is finished. In the case of a continuous iteration, the channel will remain open until the context is cancelled.
func (btcc *BlipTesterCollectionClient) docsSince(ctx context.Context, since clientSeq, continuous bool) chan *clientDoc {
	ch := make(chan *clientDoc)
	btcc.goroutineWg.Add(1)
	go func() {
		defer btcc.goroutineWg.Done()
		sinceVal := since
		defer close(ch)
		for {
			if ctx.Err() != nil {
				return
			}
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: sinceVal=%d", sinceVal)
			for _, doc := range btcc.OneShotDocsSince(ctx, sinceVal) {
				select {
				case <-ctx.Done():
					return
				case ch <- doc:
					base.DebugfCtx(ctx, base.KeySGTest, "sent doc %q to changes feed", doc.id)
					sinceVal = doc.latestSeq()
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
	lock                 sync.RWMutex               // protects all of the below properties
	_latestSeq           clientSeq                  // Latest sequence number we have for the doc - the active rev
	_latestServerVersion DocVersion                 // Latest version we know the server had (via push or a pull)
	_revisionsBySeq      map[clientSeq]clientDocRev // Full history of doc from client POV
	_seqsByVersions      map[DocVersion]clientSeq   // Lookup from version into revisionsBySeq
}

// docRevSeqsNewestToOldest returns a list of sequences associated with this document, ordered newest to oldest.
// Can be used for lookups in clientDoc.revisionsBySeq
func (cd *clientDoc) docRevSeqsNewestToOldest() []clientSeq {
	cd.lock.RLock()
	defer cd.lock.RUnlock()
	return cd._docRevSeqsNewestToOldest()
}

// _docRevSeqsNewestToOldest returns a list of sequences associated with this document, ordered newest to oldest.
func (cd *clientDoc) _docRevSeqsNewestToOldest() []clientSeq {
	seqs := make([]clientSeq, 0, len(cd._revisionsBySeq))
	for _, rev := range cd._revisionsBySeq {
		seqs = append(seqs, rev.clientSeq)
	}
	slices.Sort(seqs)    // oldest to newest
	slices.Reverse(seqs) // newest to oldest
	return seqs
}

// latestRev returns the latest revision of the document.
func (cd *clientDoc) latestRev() (*clientDocRev, error) {
	cd.lock.RLock()
	defer cd.lock.RUnlock()
	rev, ok := cd._revisionsBySeq[cd._latestSeq]
	if !ok {
		return nil, fmt.Errorf("latestSeq %d not found in revisionsBySeq", cd._latestSeq)
	}
	return &rev, nil
}

// addNewRev adds a new revision to the document.
func (cd *clientDoc) addNewRev(rev clientDocRev) {
	cd.lock.Lock()
	defer cd.lock.Unlock()
	cd._latestSeq = rev.clientSeq
	cd._revisionsBySeq[rev.clientSeq] = rev
	cd._seqsByVersions[rev.version] = rev.clientSeq
}

// latestSeq returns the latest sequence number for a document known to the client.
func (cd *clientDoc) latestSeq() clientSeq {
	cd.lock.RLock()
	defer cd.lock.RUnlock()
	return cd._latestSeq
}

// revisionBySeq returns the revision associated with the given sequence number.
func (cd *clientDoc) revisionBySeq(seq clientSeq) (*clientDocRev, error) {
	cd.lock.RLock()
	defer cd.lock.RUnlock()
	rev, ok := cd._revisionsBySeq[seq]
	if !ok {
		return nil, fmt.Errorf("seq %d not found in revisionsBySeq", seq)
	}
	return &rev, nil
}

// setLatestServerVersion sets the latest server version for the document.
func (cd *clientDoc) setLatestServerVersion(version DocVersion) {
	cd.lock.Lock()
	defer cd.lock.Unlock()
	cd._latestServerVersion = version
}

// getRev returns the revision associated with the given version.
func (cd *clientDoc) getRev(version DocVersion) (*clientDocRev, error) {
	cd.lock.RLock()
	defer cd.lock.RUnlock()
	seq, ok := cd._seqsByVersions[version]
	if !ok {
		return nil, fmt.Errorf("version %v not found in seqsByVersions", version)
	}
	rev, ok := cd._revisionsBySeq[seq]
	if !ok {
		return nil, fmt.Errorf("seq %d not found in revisionsBySeq", seq)
	}
	return &rev, nil
}

// pruneVersion removes the given version from the document.
func (cd *clientDoc) pruneVersion(t testing.TB, version DocVersion) {
	cd.lock.Lock()
	defer cd.lock.Unlock()
	seq, ok := cd._seqsByVersions[version]
	require.Less(t, seq, cd._latestSeq, "seq %d is the latest seq for doc %q, can not prune latest version", seq, cd.id)
	require.True(t, ok, "version %v not found in seqsByVersions", version)
	delete(cd._seqsByVersions, version)
	delete(cd._revisionsBySeq, seq)
}

type BlipTesterCollectionClient struct {
	parent *BlipTesterClient

	ctx         context.Context
	ctxCancel   context.CancelFunc
	goroutineWg sync.WaitGroup

	collection    string
	collectionIdx int

	// seqLock protects all _seq... fields below
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

// getClientDoc returns the clientDoc for the given docID, if it exists.
func (btcc *BlipTesterCollectionClient) getClientDoc(docID string) (*clientDoc, bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	return btcc._getClientDoc(docID)
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
	btr.bt.blipContext.HandlerForProfile[db.MessageNoRev] = btr.handleNoRev(btc)
	btr.bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = btr.handleGetAttachment(btc)
	btr.bt.blipContext.HandlerForProfile[db.MessageRev] = btr.handleRev(ctx, btc)
	btr.bt.blipContext.HandlerForProfile[db.MessageProposeChanges] = btr.handleProposeChanges(btc)
	btr.bt.blipContext.HandlerForProfile[db.MessageChanges] = btr.handleChanges(btc)
	btr.bt.blipContext.HandlerForProfile[db.MessageProveAttachment] = btr.handleProveAttachment(ctx, btc)
}

// handleProveAttachment handles proveAttachment received by blip client
func (btr *BlipTesterReplicator) handleProveAttachment(ctx context.Context, btc *BlipTesterClient) func(*blip.Message) {
	return func(msg *blip.Message) {
		defer btr.storeMessage(msg)

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
		defer btr.storeMessage(msg)

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
					}
				}

				// Build up a list of revisions known to the client for each change
				// The first element of each revision list must be the parent revision of the change
				if doc, haveDoc := btcc.getClientDoc(docID); haveDoc {
					docSeqs := doc.docRevSeqsNewestToOldest()
					revList := make([]string, 0, revsLimit)

					for _, seq := range docSeqs {
						if deletedInt&2 == 2 {
							continue
						}

						rev, err := doc.revisionBySeq(seq)
						require.NoError(btr.TB(), err)

						if revID == rev.version.RevID {
							knownRevs[i] = nil // Send back null to signal we don't need this change
							continue outer
						}

						if len(revList) < revsLimit {
							revList = append(revList, rev.version.RevID)
						} else {
							break
						}
					}

					knownRevs[i] = revList
				} else {
					knownRevs[i] = []interface{}{} // sending empty array means we've not seen the doc before, but still want it
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
			btcc.seqLock.Lock()
			defer btcc.seqLock.Unlock()
			btcc._seqLast++
			newClientSeq := btcc._seqLast
			newVersion := DocVersion{RevID: revID}

			docRev := clientDocRev{
				clientSeq: newClientSeq,
				version:   newVersion,
				body:      body,
				isDelete:  true,
				message:   msg,
			}

			doc, ok := btcc._getClientDoc(docID)
			if !ok {
				doc = &clientDoc{
					id:         docID,
					_latestSeq: newClientSeq,
					_revisionsBySeq: map[clientSeq]clientDocRev{
						newClientSeq: docRev,
					},
					_seqsByVersions: map[DocVersion]clientSeq{
						newVersion: newClientSeq,
					},
				}
			} else {
				// remove existing entry and replace with new seq
				delete(btcc._seqStore, doc.latestSeq())
				doc.addNewRev(docRev)
			}
			btcc._seqStore[newClientSeq] = doc
			btcc._seqFromDocID[docID] = newClientSeq

			if replacedRev != "" {
				// store the new sequence for a replaced rev for tests waiting for this specific rev
				doc.lock.Lock()
				doc._seqsByVersions[DocVersion{RevID: replacedRev}] = newClientSeq
				doc.lock.Unlock()
			}
			doc.setLatestServerVersion(newVersion)

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

			var old db.Body
			doc, ok := btcc.getClientDoc(docID)
			require.True(btc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
			oldRev, err := doc.getRev(DocVersion{RevID: deltaSrc})
			require.NoError(btc.TB(), err)
			err = old.Unmarshal(oldRev.body)
			require.NoError(btc.TB(), err)

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

		// TODO: Duplicated code from the deleted case above - factor into shared function?
		btcc.seqLock.Lock()
		defer btcc.seqLock.Unlock()
		btcc._seqLast++
		newClientSeq := btcc._seqLast
		newVersion := DocVersion{RevID: revID}

		docRev := clientDocRev{
			clientSeq: newClientSeq,
			version:   newVersion,
			body:      body,
			message:   msg,
		}

		doc, ok := btcc._getClientDoc(docID)
		if !ok {
			doc = &clientDoc{
				id:         docID,
				_latestSeq: newClientSeq,
				_revisionsBySeq: map[clientSeq]clientDocRev{
					newClientSeq: docRev,
				},
				_seqsByVersions: map[DocVersion]clientSeq{
					newVersion: newClientSeq,
				},
			}
		} else {
			// remove existing entry and replace with new seq
			delete(btcc._seqStore, doc.latestSeq())
			doc.addNewRev(docRev)
		}
		btcc._seqStore[newClientSeq] = doc
		btcc._seqFromDocID[docID] = newClientSeq

		if replacedRev != "" {
			// store the new sequence for a replaced rev for tests waiting for this specific rev
			doc.lock.Lock()
			doc._seqsByVersions[DocVersion{RevID: replacedRev}] = newClientSeq
			doc.lock.Unlock()
		}
		doc.setLatestServerVersion(newVersion)

		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}
}

// handleGetAttachment handles getAttachment messages on the blip tester client
func (btr *BlipTesterReplicator) handleGetAttachment(btc *BlipTesterClient) func(msg *blip.Message) {
	return func(msg *blip.Message) {
		defer btr.storeMessage(msg)

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
		defer btr.storeMessage(msg)

		btcc := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.NorevMessageId]
		revID := msg.Properties[db.NorevMessageRev]

		btcc.seqLock.Lock()
		defer btcc.seqLock.Unlock()
		btcc._seqLast++
		newSeq := btcc._seqLast
		doc, ok := btcc._getClientDoc(docID)
		if !ok {
			doc = &clientDoc{
				id:              docID,
				_latestSeq:      newSeq,
				_revisionsBySeq: make(map[clientSeq]clientDocRev, 1),
				_seqsByVersions: make(map[DocVersion]clientSeq, 1),
			}
		}
		doc.addNewRev(clientDocRev{
			clientSeq: newSeq,
			version:   DocVersion{RevID: revID},
			body:      nil,
			isDelete:  false,
			message:   msg,
		})
		btcc._seqStore[newSeq] = doc
		btcc._seqFromDocID[docID] = newSeq
	}

}

// defaultHandler is the default handler for the blip tester client, this will fail the test harness
func (btr *BlipTesterReplicator) defaultHandler() func(msg *blip.Message) {
	return func(msg *blip.Message) {
		btr.storeMessage(msg)
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
func (btcc *BlipTesterCollectionClient) updateLastReplicatedRev(docID string, version DocVersion) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	doc.setLatestServerVersion(version)
}

// getLastReplicatedRev returns the last version replicated to Sync Gateway for the given docID.
func (btcc *BlipTesterCollectionClient) getLastReplicatedRev(docID string) (version DocVersion, ok bool) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	doc.lock.RLock()
	latestServerVersion := doc._latestServerVersion
	doc.lock.RUnlock()
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

func proposeChangesEntryForDoc(doc *clientDoc) proposeChangeBatchEntry {
	doc.lock.RLock()
	defer doc.lock.RUnlock()
	latestRev := doc._revisionsBySeq[doc._latestSeq]
	var revisionHistory []DocVersion
	for i, seq := range doc._docRevSeqsNewestToOldest() {
		if i == 0 {
			// skip current rev
			continue
		} else if i == 19 {
			break // only send 20 history entries
		}
		revisionHistory = append(revisionHistory, doc._revisionsBySeq[seq].version)
	}
	return proposeChangeBatchEntry{docID: doc.id, version: latestRev.version, history: revisionHistory, latestServerVersion: doc._latestServerVersion}
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
		for doc := range btcc.docsSince(btcc.ctx, seq, opts.Continuous) {
			changesBatch = append(changesBatch, proposeChangesEntryForDoc(doc))
			if len(changesBatch) >= changesBatchSize {
				base.DebugfCtx(ctx, base.KeySGTest, "Sending batch of %d changes", len(changesBatch))
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

				var serverDeltas bool
				if proposeChangesResponse.Properties[db.ChangesResponseDeltas] == "true" {
					base.DebugfCtx(ctx, base.KeySGTest, "server supports deltas")
					serverDeltas = true
				}

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
						// send
						revRequest := blip.NewRequest()
						revRequest.SetProfile(db.MessageRev)
						revRequest.Properties[db.RevMessageID] = change.docID
						revRequest.Properties[db.RevMessageRev] = change.version.RevID
						revRequest.Properties[db.RevMessageHistory] = change.historyStr()

						doc, ok := btcc.getClientDoc(change.docID)
						require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", change.docID)
						doc.lock.RLock()
						serverRev := doc._revisionsBySeq[doc._seqsByVersions[change.latestServerVersion]]
						docBody := doc._revisionsBySeq[doc._seqsByVersions[change.version]].body
						doc.lock.RUnlock()

						if serverDeltas && btcc.parent.ClientDeltas && ok && !serverRev.isDelete {
							base.DebugfCtx(ctx, base.KeySGTest, "specifying last known server version as deltaSrc for doc %s = %v", change.docID, change.latestServerVersion)
							revRequest.Properties[db.RevMessageDeltaSrc] = change.latestServerVersion.RevID
							var parentBodyUnmarshalled db.Body
							require.NoError(btcc.TB(), parentBodyUnmarshalled.Unmarshal(serverRev.body))
							var newBodyUnmarshalled db.Body
							require.NoError(btcc.TB(), newBodyUnmarshalled.Unmarshal(docBody))
							delta, err := base.Diff(parentBodyUnmarshalled, newBodyUnmarshalled)
							require.NoError(btcc.TB(), err)
							revRequest.SetBody(delta)
						} else {
							revRequest.SetBody(docBody)
						}

						btcc.addCollectionProperty(revRequest)
						btcc.sendPushMsg(revRequest)
						base.DebugfCtx(ctx, base.KeySGTest, "sent doc %s / %v", change.docID, change.version)
						// block until remote has actually processed the rev and sent a response
						revResp := revRequest.Response()
						require.NotContains(btcc.TB(), revResp.Properties, "Error-Domain", "unexpected error response from rev %v: %s", revResp)
						base.DebugfCtx(ctx, base.KeySGTest, "peer acked rev %s / %v", change.docID, change.version)
						btcc.updateLastReplicatedRev(change.docID, change.version)
						doc, ok = btcc.getClientDoc(change.docID)
						require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", change.docID)
						doc.lock.Lock()
						rev := doc._revisionsBySeq[doc._seqsByVersions[change.version]]
						rev.message = revRequest
						doc.lock.Unlock()
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

				// empty batch
				changesBatch = changesBatch[:0]
			}
		}
	}()
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
		doc = &clientDoc{
			id:              docID,
			_latestSeq:      0,
			_revisionsBySeq: make(map[clientSeq]clientDocRev, 1),
			_seqsByVersions: make(map[DocVersion]clientSeq, 1),
		}
	}
	newGen := 1
	if parentVersion != nil {
		// grab latest version for this doc and make sure we're doing an upsert on top of it to avoid branching revisions
		latestRev, err := doc.latestRev()
		require.NoError(btcc.TB(), err)
		latestVersion := latestRev.version
		require.Equal(btcc.TB(), *parentVersion, latestVersion, "latest version for docID: %v is %v, expected parentVersion: %v", docID, latestVersion, parentVersion)
		newGen = parentVersion.RevIDGeneration() + 1
	}

	body = btcc.ProcessInlineAttachments(body, newGen)

	digest := "abc" // TODO: Generate rev ID digest based on body hash?

	newRevID := fmt.Sprintf("%d-%s", newGen, digest)
	btcc._seqLast++
	newSeq := btcc._seqLast
	rev := clientDocRev{clientSeq: newSeq, version: DocVersion{RevID: newRevID}, body: body}
	doc.addNewRev(rev)

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
	doc, ok := btcc.getClientDoc(docID)
	if !ok {
		return nil, false
	}
	doc.lock.RLock()
	defer doc.lock.RUnlock()
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

// GetDoc returns a rev stored in the Client under the given docID.  (if multiple revs are present, rev body returned is non-deterministic)
func (btcc *BlipTesterCollectionClient) GetDoc(docID string) (data []byte, found bool) {
	doc, ok := btcc.getClientDoc(docID)
	if !ok {
		return nil, false
	}

	latestRev, err := doc.latestRev()
	require.NoError(btcc.TB(), err)
	if latestRev == nil {
		return nil, false
	}

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

// GetMessages returns a copy of all messages stored in the Client keyed by serial number
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

// GetBLipRevMessage returns the rev message that wrote the given docID/DocVersion on the client.
func (btcc *BlipTesterCollectionClient) GetBlipRevMessage(docID string, version DocVersion) (msg *blip.Message, found bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()

	if doc, ok := btcc._getClientDoc(docID); ok {
		doc.lock.RLock()
		defer doc.lock.RUnlock()
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

// pruneVersion removes the given version from the specified doc. This is not allowed for the latest version of a document.
func (btcc *BlipTesterCollectionClient) pruneVersion(docID string, version DocVersion) {
	btcc.seqLock.Lock()
	defer btcc.seqLock.Unlock()
	doc, ok := btcc._getClientDoc(docID)
	require.True(btcc.TB(), ok, "docID %q not found")
	doc.pruneVersion(btcc.TB(), version)
}
