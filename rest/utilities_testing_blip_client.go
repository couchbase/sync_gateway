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
	"github.com/couchbaselabs/rosmar"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	VersionVectorSubtestName = "versionVector"
	RevtreeSubtestName       = "revTree"
)

type BlipTesterClientOpts struct {
	ClientDeltas                  bool // Support deltas on the client side
	Username                      string
	Channels                      []string
	SendRevocations               bool
	SupportedBLIPProtocols        []string
	SkipCollectionsInitialization bool

	AllowCreationWithoutBlipTesterClientRunner bool // Allow the client to be created outside of a BlipTesterClientRunner.Run() subtest
	// a deltaSrc rev ID for which to reject a delta
	rejectDeltasForSrcRev string

	// changesEntryCallback is a callback function invoked for each changes entry being received (pull)
	changesEntryCallback func(docID, revID string)

	// optional Origin header
	origin *string

	// sendReplacementRevs opts into the replacement rev behaviour in the event that we do not find the requested one.
	sendReplacementRevs bool

	revsLimit *int // defaults to 20

	// SourceID is used to define the SourceID for the blip client
	SourceID string
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

	hlc *rosmar.HybridLogicalClock
}

// getClientDocForSeq returns the clientDoc for the given sequence number, if it exists.
func (c *BlipTesterCollectionClient) getClientDocForSeq(seq clientSeq) (*clientDoc, bool) {
	c.seqLock.RLock()
	defer c.seqLock.RUnlock()
	doc, ok := c._seqStore[seq]
	return doc, ok
}

// OneShotDocsSince is an iterator that yields client sequence and document pairs that are newer than the given since value.
func (c *BlipTesterCollectionClient) OneShotDocsSince(ctx context.Context, since clientSeq) iter.Seq2[clientSeq, *clientDoc] {
	return func(yield func(clientSeq, *clientDoc) bool) {
		c.seqLock.Lock()
		seqLast := c._seqLast
		for c._seqLast <= since {
			if ctx.Err() != nil {
				c.seqLock.Unlock()
				return
			}
			// block until new seq
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - waiting for new sequence", since, c._seqLast)
			c._seqCond.Wait()
			// Check to see if we were woken because of Close()
			if ctx.Err() != nil {
				c.seqLock.Unlock()
				return
			}
			seqLast = c._seqLast
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - woke up", since, c._seqLast)
		}
		c.seqLock.Unlock()
		base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: since=%d, _seqLast=%d - iterating", since, seqLast)
		for seq := since; seq <= seqLast; seq++ {
			doc, ok := c.getClientDocForSeq(seq)
			// filter non-latest entries in cases where we haven't pruned _seqStore
			if !ok {
				continue
			}
			// make sure that seq is latestseq
			require.Equal(c.TB(), doc.latestSeq(), seq, "this should've been pruned out!")
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
func (c *BlipTesterCollectionClient) docsSince(ctx context.Context, since clientSeq, continuous bool) chan *clientDoc {
	ch := make(chan *clientDoc)
	c.goroutineWg.Add(1)
	go func() {
		defer c.goroutineWg.Done()
		sinceVal := since
		defer close(ch)
		for {
			if ctx.Err() != nil {
				return
			}
			base.DebugfCtx(ctx, base.KeySGTest, "OneShotDocsSince: sinceVal=%d", sinceVal)
			for _, doc := range c.OneShotDocsSince(ctx, sinceVal) {
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
	HLV       db.HybridLogicalVector // The full HLV for the revision, populated when mode = HLV
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
	return cd._latestRev()
}

// _latestRev returns the latest revision of the document.
func (cd *clientDoc) _latestRev() (*clientDocRev, error) {
	if cd._latestSeq == 0 {
		return nil, nil
	}
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
	cd._addNewRev(rev)
}

// addNewRev adds a new revision to the document.
func (cd *clientDoc) _addNewRev(rev clientDocRev) {
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

func (cd *clientDoc) currentVersion(t testing.TB) *db.Version {
	rev, err := cd.latestRev()
	require.NoError(t, err)
	return &rev.version.CV
}

type BlipTesterCollectionClient struct {
	parent *BlipTesterClient

	ctx         context.Context
	ctxCancel   context.CancelFunc
	goroutineWg sync.WaitGroup

	pushRunning   base.AtomicBool
	pushCtx       context.Context
	pushCtxCancel context.CancelFunc

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

	hlc *rosmar.HybridLogicalClock
}

// GetDoc returns the latest revision of a document stored on the client.
func (btcc *BlipTesterCollectionClient) GetDoc(docID string) ([]byte, *db.HybridLogicalVector, *DocVersion) {
	doc, exists := btcc.getClientDoc(docID)
	if !exists {
		return nil, nil, nil
	}
	latestRev, err := doc.latestRev()
	require.NoError(btcc.TB(), err)
	if latestRev == nil {
		return nil, nil, nil
	}
	return latestRev.body, &latestRev.HLV, &latestRev.version
}

// IsTombstoned returns true if the latest version of the doc is a tombstone.
func (btcc *BlipTesterCollectionClient) IsTombstoned(docID string) (bool, error) {
	doc, exists := btcc.getClientDoc(docID)
	if !exists {
		return false, base.ErrNotFound
	}
	rev, err := doc.latestRev()
	if err != nil {
		return false, err
	}
	return rev.isDelete, nil
}

// IsVersionTombstone returns true if the given version is found and is a tombstone.
func (btcc *BlipTesterCollectionClient) IsVersionTombstone(docID string, version DocVersion) (bool, error) {
	doc, exists := btcc.getClientDoc(docID)
	if !exists {
		return false, base.ErrNotFound
	}
	rev, err := doc.getRev(version)
	if err != nil {
		return false, err
	}
	return rev.isDelete, nil
}

// getClientDoc returns the clientDoc for the given docID, if it exists.
func (btcc *BlipTesterCollectionClient) getClientDoc(docID string) (*clientDoc, bool) {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	return btcc._getClientDoc(docID)
}

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
	clients                     map[uint32]*BlipTesterClient // map of created BlipTesterClient's
	t                           *testing.T
	initialisedInsideRunnerCode bool            // flag to check that the BlipTesterClient is being initialised in the correct area (inside the Run() method)
	SkipSubtest                 map[string]bool // map of sub tests on the blip tester runner to skip
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
		t:           t,
		clients:     make(map[uint32]*BlipTesterClient),
		SkipSubtest: make(map[string]bool),
	}
}

func (btr *BlipTesterReplicator) Close() {
	btr.bt.Close()
	btr.messagesLock.Lock()
	btr.messages = make(map[blip.MessageNumber]*blip.Message, 0)
	btr.messagesLock.Unlock()
}

func (btr *BlipTesterReplicator) initHandlers(btc *BlipTesterClient) {
	revsLimit := base.IntDefault(btc.revsLimit, defaultBlipTesterClientRevsLimit)

	if btr.replicationStats == nil {
		btr.replicationStats = db.NewBlipSyncStats()
	}

	ctx := base.DatabaseLogCtx(base.TestCtx(btr.bt.restTester.TB()), btr.bt.restTester.GetDatabase().Name, nil)
	btr.bt.blipContext.HandlerForProfile[db.MessageProveAttachment] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		nonce, err := msg.Body()
		require.NoError(btr.TB(), err)

		require.NotEmpty(btr.TB(), nonce, "no nonce sent with proveAttachment")

		digest, ok := msg.Properties[db.ProveAttachmentDigest]
		require.True(btr.TB(), ok, "no digest sent with proveAttachment")

		btcr := btc.getCollectionClientFromMessage(msg)

		attData := btcr.getAttachment(digest)

		proof := db.ProveAttachment(ctx, attData, nonce)

		resp := msg.Response()
		resp.SetBody([]byte(proof))
		btr.replicationStats.ProveAttachment.Add(1)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageChanges] = func(msg *blip.Message) {
		btr.storeMessage(msg)
		btcr := btc.getCollectionClientFromMessage(msg)

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
				if doc, haveDoc := btcr.getClientDoc(docID); haveDoc {
					if btc.UseHLV() {
						changesVersion, err := db.ParseVersion(revID)
						require.NoError(btr.TB(), err, "error parsing version %q: %v", revID, err)
						currentRev, _ := doc.latestRev()
						if currentRev != nil && currentRev.HLV.DominatesSource(changesVersion) {
							knownRevs[i] = nil // Send back null to signal we don't need this change
							continue outer
						}

						// HLV clients only need to send the current version
						knownRevs[i] = []interface{}{doc.currentVersion(btc.TB()).String()}
						continue
					}

					docSeqs := doc.docRevSeqsNewestToOldest()
					revList := make([]string, 0, revsLimit)

					for _, seq := range docSeqs {
						if deletedInt&2 == 2 {
							continue
						}

						rev, err := doc.revisionBySeq(seq)
						require.NoError(btr.TB(), err)

						revTreeID := rev.version.RevTreeID
						if revTreeID == revID {
							knownRevs[i] = nil // Send back null to signal we don't need this change
							continue outer
						}

						if len(revList) < revsLimit {
							revList = append(revList, revTreeID)
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

	btr.bt.blipContext.HandlerForProfile[db.MessageProposeChanges] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageRev] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

		btcr := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.RevMessageID]
		revID := msg.Properties[db.RevMessageRev]
		deltaSrc := msg.Properties[db.RevMessageDeltaSrc]
		replacedRev := msg.Properties[db.RevMessageReplacedRev]
		revHistory := msg.Properties[db.RevMessageHistory]

		body, err := msg.Body()
		require.NoError(btr.TB(), err)

		if msg.Properties[db.RevMessageDeleted] == "1" {
			btcr.seqLock.Lock()
			defer btcr.seqLock.Unlock()
			btcr._seqLast++
			newClientSeq := btcr._seqLast

			doc, ok := btcr._getClientDoc(docID)
			if !ok {
				doc = &clientDoc{
					id:              docID,
					_revisionsBySeq: make(map[clientSeq]clientDocRev),
					_seqsByVersions: make(map[DocVersion]clientSeq),
				}
			}

			doc.lock.Lock()
			defer doc.lock.Unlock()

			var newVersion DocVersion
			var hlv db.HybridLogicalVector
			if btc.UseHLV() {
				if revHistory != "" {
					existingVersion, _, err := db.ExtractHLVFromBlipMessage(revHistory)
					require.NoError(btr.TB(), err, "error extracting HLV %q: %v", revHistory, err)
					hlv = *existingVersion
				}
				v, err := db.ParseVersion(revID)
				require.NoError(btr.TB(), err, "error parsing version %q: %v", revID, err)
				newVersion = DocVersion{CV: v}
				require.NoError(btr.TB(), hlv.AddVersion(v))
			} else {
				newVersion = DocVersion{RevTreeID: revID}
			}

			docRev := clientDocRev{
				clientSeq: newClientSeq,
				version:   newVersion,
				body:      body,
				HLV:       hlv,
				isDelete:  true,
				message:   msg,
			}

			// remove existing entry and replace with new seq
			delete(btcr._seqStore, doc._latestSeq)
			doc._addNewRev(docRev)
			btcr._seqStore[newClientSeq] = doc
			btcr._seqFromDocID[docID] = newClientSeq

			if replacedRev != "" {
				var replacedVersion DocVersion
				if btc.UseHLV() {
					v, err := db.ParseVersion(replacedRev)
					require.NoError(btr.TB(), err, "error parsing version %q: %v", replacedRev, err)
					replacedVersion = DocVersion{CV: v}
				} else {
					replacedVersion = DocVersion{RevTreeID: revID}
				}
				// store the new sequence for a replaced rev for tests waiting for this specific rev
				doc._seqsByVersions[replacedVersion] = newClientSeq
			}
			doc._latestServerVersion = newVersion

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
			doc, ok := btcr.getClientDoc(docID)
			require.True(btc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
			var deltaSrcVersion DocVersion
			if btc.UseHLV() {
				v, err := db.ParseVersion(deltaSrc)
				require.NoError(btr.TB(), err, "error parsing version %q: %v", deltaSrc, err)
				deltaSrcVersion = DocVersion{CV: v}
			} else {
				deltaSrcVersion = DocVersion{RevTreeID: deltaSrc}
			}
			oldRev, err := doc.getRev(deltaSrcVersion)
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
				btcr.attachmentsLock.RLock()
				for _, attachment := range attsMap {
					attMap, ok := attachment.(map[string]interface{})
					require.True(btr.TB(), ok, "att in doc wasn't map[string]interface{}")
					digest := attMap["digest"].(string)

					if _, found := btcr._attachments[digest]; !found {
						missingDigests = append(missingDigests, digest)
					} else {
						if btr.bt.activeSubprotocol == db.CBMobileReplicationV2 {
							// only v2 clients care about proveAttachments
							knownDigests = append(knownDigests, digest)
						}
					}
				}
				btcr.attachmentsLock.RUnlock()

				for _, digest := range knownDigests {
					attData := btcr.getAttachment(digest)
					nonce, proof, err := db.GenerateProofOfAttachment(ctx, attData)
					require.NoError(btr.TB(), err)

					// if we already have this attachment, _we_ should ask the peer whether _they_ have the attachment
					outrq := blip.NewRequest()
					outrq.SetProfile(db.MessageProveAttachment)
					outrq.Properties[db.ProveAttachmentDigest] = digest
					outrq.SetBody(nonce)

					btcr.sendPullMsg(outrq)

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

					btcr.sendPullMsg(outrq)

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

					btcr.attachmentsLock.Lock()
					btcr._attachments[digest] = respBody
					btcr.attachmentsLock.Unlock()
				}
			}

		}

		if bodyJSON != nil {
			body, err = base.JSONMarshal(bodyJSON)
			require.NoError(btr.TB(), err)
		}

		// TODO: Duplicated code from the deleted case above - factor into shared function?
		btcr.seqLock.Lock()
		defer btcr.seqLock.Unlock()
		btcr._seqLast++
		newClientSeq := btcr._seqLast

		doc, ok := btcr._getClientDoc(docID)
		if !ok {
			doc = &clientDoc{
				id:              docID,
				_revisionsBySeq: make(map[clientSeq]clientDocRev),
				_seqsByVersions: make(map[DocVersion]clientSeq),
			}
		}
		doc.lock.Lock()
		defer doc.lock.Unlock()

		var newVersion DocVersion
		var hlv db.HybridLogicalVector
		if btc.UseHLV() {
			if revHistory != "" {
				existingVersion, _, err := db.ExtractHLVFromBlipMessage(revHistory)
				require.NoError(btr.TB(), err, "error extracting HLV %q: %v", revHistory, err)
				hlv = *existingVersion
			}
			v, err := db.ParseVersion(revID)
			require.NoError(btr.TB(), err, "error parsing version %q: %v", revID, err)
			newVersion = DocVersion{CV: v}
			require.NoError(btr.TB(), hlv.AddVersion(v))
		} else {
			newVersion = DocVersion{RevTreeID: revID}
		}
		docRev := clientDocRev{
			clientSeq: newClientSeq,
			version:   newVersion,
			HLV:       hlv,
			body:      body,
			message:   msg,
		}

		// remove existing entry and replace with new seq
		delete(btcr._seqStore, doc._latestSeq)
		doc._addNewRev(docRev)
		btcr._seqStore[newClientSeq] = doc
		btcr._seqFromDocID[docID] = newClientSeq

		if replacedRev != "" {
			var replacedVersion DocVersion
			if btc.UseHLV() {
				v, err := db.ParseVersion(replacedRev)
				require.NoError(btr.TB(), err, "error parsing version %q: %v", replacedRev, err)
				replacedVersion = DocVersion{CV: v}
			} else {
				replacedVersion = DocVersion{RevTreeID: replacedRev}
			}
			// store the new sequence for a replaced rev for tests waiting for this specific rev
			doc._seqsByVersions[replacedVersion] = newClientSeq
		}
		doc._latestServerVersion = newVersion

		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		digest, ok := msg.Properties[db.GetAttachmentDigest]
		require.True(btr.TB(), ok, "couldn't find digest in getAttachment message properties")

		btcr := btc.getCollectionClientFromMessage(msg)

		attachment := btcr.getAttachment(digest)

		response := msg.Response()
		response.SetBody(attachment)
		btr.replicationStats.GetAttachment.Add(1)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageNoRev] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		btcr := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.NorevMessageId]
		revID := msg.Properties[db.NorevMessageRev]

		btcr.seqLock.Lock()
		defer btcr.seqLock.Unlock()
		btcr._seqLast++
		newSeq := btcr._seqLast
		doc, ok := btcr._getClientDoc(docID)
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
			version:   DocVersion{RevTreeID: revID},
			body:      nil,
			isDelete:  false,
			message:   msg,
		})
		btcr._seqStore[newSeq] = doc
		btcr._seqFromDocID[docID] = newSeq
	}

	btr.bt.blipContext.DefaultHandler = func(msg *blip.Message) {
		btr.storeMessage(msg)
		base.PanicfCtx(ctx, "Unknown profile: %s caught by client DefaultHandler - msg: %#v", msg.Profile(), msg)
	}
}

// TB returns testing.TB for the current test
func (btr *BlipTesterReplicator) TB() testing.TB {
	return btr.bt.restTester.TB()
}

// TB returns testing.TB for the current test
func (btc *BlipTesterCollectionClient) TB() testing.TB {
	return btc.parent.rt.TB()
}

func (btcc *BlipTesterCollectionClient) UseHLV() bool {
	return btcc.parent.UseHLV()
}

// saveAttachment takes base64 encoded data and stores the attachment on the client.
func (btc *BlipTesterCollectionClient) saveAttachment(base64data string) (dataLength int, digest string) {
	btc.attachmentsLock.Lock()
	defer btc.attachmentsLock.Unlock()

	ctx := base.DatabaseLogCtx(base.TestCtx(btc.parent.rt.TB()), btc.parent.rt.GetDatabase().Name, nil)

	data, err := base64.StdEncoding.DecodeString(base64data)
	require.NoError(btc.TB(), err)

	digest = db.Sha1DigestKey(data)
	if _, found := btc._attachments[digest]; found {
		base.InfofCtx(ctx, base.KeySync, "attachment with digest %s already exists", digest)
	} else {
		btc._attachments[digest] = data
	}

	return len(data), digest
}

func (btc *BlipTesterCollectionClient) getAttachment(digest string) (attachment []byte) {
	btc.attachmentsLock.RLock()
	defer btc.attachmentsLock.RUnlock()

	require.Contains(btc.TB(), btc._attachments, digest, "attachment not found for digest %s", digest)
	return btc._attachments[digest]
}

func (btc *BlipTesterCollectionClient) updateLastReplicatedVersion(docID string, version DocVersion) {
	btc.seqLock.Lock()
	defer btc.seqLock.Unlock()
	doc, ok := btc._getClientDoc(docID)
	require.True(btc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	doc.setLatestServerVersion(version)
}

func (btc *BlipTesterCollectionClient) getLastReplicatedVersion(docID string) (version DocVersion, ok bool) {
	btc.seqLock.Lock()
	defer btc.seqLock.Unlock()
	doc, ok := btc._getClientDoc(docID)
	require.True(btc.TB(), ok, "docID %q not found in _seqFromDocID", docID)
	doc.lock.RLock()
	latestServerVersion := doc._latestServerVersion
	doc.lock.RUnlock()
	return latestServerVersion, latestServerVersion.RevTreeID != "" || latestServerVersion.CV.Value != 0
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
	dbc := rt.GetDatabase()
	var collections []string
	for _, collection := range dbc.CollectionByID {
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
	if opts == nil {
		opts = &BlipTesterClientOpts{}
	}
	if !opts.AllowCreationWithoutBlipTesterClientRunner && !btcRunner.initialisedInsideRunnerCode {
		require.FailNow(btcRunner.TB(), "must initialise BlipTesterClient inside Run() method")
	}
	id, err := uuid.NewRandom()
	require.NoError(btcRunner.TB(), err)
	if opts.SourceID == "" {
		opts.SourceID = fmt.Sprintf("btc-%d", id.ID())
	}

	client = &BlipTesterClient{
		BlipTesterClientOpts: *opts,
		rt:                   rt,
		id:                   id.ID(),
		hlc:                  rosmar.NewHybridLogicalClock(0),
	}
	btcRunner.clients[client.id] = client
	client.createBlipTesterReplications()

	return client
}

// ID returns the unique ID of the client.
func (btc *BlipTesterClient) ID() uint32 {
	return btc.id
}

// TB returns testing.TB for the current test
func (btc *BlipTesterClient) TB() testing.TB {
	return btc.rt.TB()
}

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

// Add subtest to skip in runner code, if that is notes we skip the subtest. Remove skipnon hlv aware and version vector one
func (btcRunner *BlipTestClientRunner) Run(test func(t *testing.T, SupportedBLIPProtocols []string)) {
	btcRunner.initialisedInsideRunnerCode = true
	// reset to protect against someone creating a new client after Run() is run
	defer func() { btcRunner.initialisedInsideRunnerCode = false }()
	if !btcRunner.SkipSubtest[RevtreeSubtestName] {
		btcRunner.t.Run(RevtreeSubtestName, func(t *testing.T) {
			test(t, []string{db.CBMobileReplicationV3.SubprotocolString()})
		})
	}
	if !btcRunner.SkipSubtest[VersionVectorSubtestName] {
		btcRunner.t.Run(VersionVectorSubtestName, func(t *testing.T) {
			// bump sub protocol version here
			test(t, []string{db.CBMobileReplicationV4.SubprotocolString()})
		})
	}
}

func (btc *BlipTesterClient) tearDownBlipClientReplications() {
	btc.pullReplication.Close()
	btc.pushReplication.Close()
}

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
		l := sync.RWMutex{}
		ctx, ctxCancel := context.WithCancel(btc.rt.Context())
		btc.nonCollectionAwareClient = &BlipTesterCollectionClient{
			ctx:           ctx,
			ctxCancel:     ctxCancel,
			seqLock:       &l,
			_seqStore:     make(map[clientSeq]*clientDoc),
			_seqFromDocID: make(map[string]clientSeq),
			_seqCond:      sync.NewCond(&l),
			_attachments:  make(map[string][]byte),
			parent:        btc,
			hlc:           btc.hlc,
		}
	}

	btc.pullReplication.bt.avoidRestTesterClose = true
	btc.pushReplication.bt.avoidRestTesterClose = true
}

func (btc *BlipTesterClient) initCollectionReplication(collection string, collectionIdx int) {
	l := sync.RWMutex{}
	ctx, ctxCancel := context.WithCancel(btc.rt.Context())
	btcReplicator := &BlipTesterCollectionClient{
		ctx:           ctx,
		ctxCancel:     ctxCancel,
		seqLock:       &l,
		_seqStore:     make(map[clientSeq]*clientDoc),
		_seqCond:      sync.NewCond(&l),
		_seqFromDocID: make(map[string]clientSeq),
		_attachments:  make(map[string][]byte),
		parent:        btc,
		hlc:           btc.hlc,
	}

	btcReplicator.collection = collection
	btcReplicator.collectionIdx = collectionIdx

	btc.collectionClients[collectionIdx] = btcReplicator
}

func (btc *BlipTesterClient) waitForReplicationMessage(collection *db.DatabaseCollection, serialNumber blip.MessageNumber) *blip.Message {
	if base.IsDefaultCollection(collection.ScopeName, collection.Name) {
		return btc.pushReplication.WaitForMessage(serialNumber)
	}
	return btc.pushReplication.WaitForMessage(serialNumber + 1)
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
	//Channels   string
	//DocIDs     []string
	//changesBatchSize int
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
	revTreeIDHistory    []string
	hlvHistory          db.HybridLogicalVector
	latestServerVersion DocVersion
	isDelete            bool
}

func (e proposeChangeBatchEntry) historyStr() string {
	sb := strings.Builder{}
	for i, revTreeID := range e.revTreeIDHistory {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(revTreeID)
	}
	return sb.String()
}

func proposeChangesEntryForDoc(doc *clientDoc) proposeChangeBatchEntry {
	doc.lock.RLock()
	defer doc.lock.RUnlock()
	latestRev := doc._revisionsBySeq[doc._latestSeq]
	var revisionHistory []string
	for i, seq := range doc._docRevSeqsNewestToOldest() {
		if i == 0 {
			// skip current rev
			continue
		}
		revisionHistory = append(revisionHistory, doc._revisionsBySeq[seq].version.RevTreeID)
	}
	return proposeChangeBatchEntry{docID: doc.id, version: latestRev.version, revTreeIDHistory: revisionHistory, hlvHistory: latestRev.HLV, latestServerVersion: doc._latestServerVersion, isDelete: latestRev.isDelete}
}

// StartPull will begin a push replication with the given options between the client and server
func (btcc *BlipTesterCollectionClient) StartPushWithOpts(opts BlipTesterPushOptions) {
	require.True(btcc.TB(), btcc.pushRunning.CASRetry(false, true), "push replication already running")

	btcc.pushCtx, btcc.pushCtxCancel = context.WithCancel(btcc.ctx)
	ctx := btcc.pushCtx
	sinceFromStr, err := db.ParsePlainSequenceID(opts.Since)
	require.NoError(btcc.TB(), err)
	seq := clientSeq(sinceFromStr.SafeSequence())
	btcc.goroutineWg.Add(1)
	go func() {
		defer btcc.pushRunning.Set(false)
		defer btcc.goroutineWg.Done()
		// TODO: CBG-4401 wire up opts.changesBatchSize and implement a flush timeout for when the client doesn't fill the batch
		changesBatch := make([]proposeChangeBatchEntry, 0, changesBatchSize)
		base.DebugfCtx(ctx, base.KeySGTest, "Starting push replication iteration with since=%v", seq)
		for doc := range btcc.docsSince(ctx, seq, opts.Continuous) {
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
					proposedVersion := change.version.RevTreeID
					if btcc.UseHLV() {
						proposedVersion = change.version.CV.String()
					}
					proposeChangesRequestBody.WriteString(fmt.Sprintf(`["%s","%s"`, change.docID, proposedVersion))
					// write last known server version to support no-conflict mode
					if serverVersion, ok := btcc.getLastReplicatedVersion(change.docID); ok {
						serverVersionStr := serverVersion.RevTreeID
						if btcc.UseHLV() {
							serverVersionStr = serverVersion.CV.String()
						}
						base.DebugfCtx(ctx, base.KeySGTest, "specifying last known server version for doc %s = %v", change.docID, serverVersionStr)
						proposeChangesRequestBody.WriteString(fmt.Sprintf(`,"%s"`, serverVersionStr))
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
				require.NoError(btcc.TB(), err, "Error reading proposeChanges response body")
				require.NotContains(btcc.TB(), proposeChangesResponse.Properties, "Error-Domain", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)
				require.NotContains(btcc.TB(), proposeChangesResponse.Properties, "Error-Code", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)

				base.DebugfCtx(ctx, base.KeySGTest, "proposeChanges response: %s", string(rspBody))

				if len(rspBody) == 0 {
					// replication was closed underneath proposeChanges request/response - abort
					return
				}

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

						revID := change.version.RevTreeID
						if btcc.UseHLV() {
							revID = change.version.CV.String()
						}
						revRequest.Properties[db.RevMessageRev] = revID
						var history string
						if btcc.UseHLV() {
							history = change.hlvHistory.ToHistoryForHLV()
						} else {
							history = change.historyStr()
						}
						revRequest.Properties[db.RevMessageHistory] = history

						doc, ok := btcc.getClientDoc(change.docID)
						require.True(btcc.TB(), ok, "docID %q not found in _seqFromDocID", change.docID)
						doc.lock.RLock()
						serverRev := doc._revisionsBySeq[doc._seqsByVersions[change.latestServerVersion]]
						docBody := doc._revisionsBySeq[doc._seqsByVersions[change.version]].body
						doc.lock.RUnlock()

						if change.isDelete {
							revRequest.Properties[db.RevMessageDeleted] = "1"
							// SG doesn't like nil bodies - transform the tombstone into an empty body
							docBody = []byte(base.EmptyDocument)
						}

						if serverDeltas && btcc.parent.ClientDeltas && ok && !serverRev.isDelete {
							base.DebugfCtx(ctx, base.KeySGTest, "specifying last known server version as deltaSrc for doc %s = %v", change.docID, change.latestServerVersion)
							var deltaSrc string
							if btcc.UseHLV() {
								deltaSrc = change.latestServerVersion.CV.String()
							} else {
								deltaSrc = change.latestServerVersion.RevTreeID
							}
							revRequest.Properties[db.RevMessageDeltaSrc] = deltaSrc
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
						btcc.updateLastReplicatedVersion(change.docID, change.version)
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
						base.WarnfCtx(ctx, "conflict for doc %s clientVersion:%v serverVersion:%v", change.docID, change.version, change.latestServerVersion)
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

func (btcc *BlipTesterCollectionClient) StartOneshotPull() {
	btcc.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0"})
}

func (btcc *BlipTesterCollectionClient) StartOneshotPullFiltered(channels string) {
	btcc.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0", Channels: channels})
}

func (btcc *BlipTesterCollectionClient) StartOneshotPullRequestPlus() {
	btcc.StartPullSince(BlipTesterPullOptions{Continuous: false, Since: "0", RequestPlus: true})
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
func (btc *BlipTesterCollectionClient) StartPullSince(options BlipTesterPullOptions) {
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

	if btc.parent.BlipTesterClientOpts.SendRevocations {
		subChangesRequest.Properties[db.SubChangesRevocations] = "true"
	}

	if btc.parent.BlipTesterClientOpts.sendReplacementRevs {
		subChangesRequest.Properties[db.SubChangesSendReplacementRevs] = "true"
	}

	if len(options.DocIDs) > 0 {
		subChangesRequest.SetBody(base.MustJSONMarshal(btc.TB(),
			db.SubChangesBody{
				DocIDs: options.DocIDs,
			},
		))
	}
	btc.sendPullMsg(subChangesRequest)

	// ensure subChanges was successful
	subChangesResponse := subChangesRequest.Response()
	rspBody, err := subChangesResponse.Body()
	require.NoError(btc.TB(), err)
	errorDomain := subChangesResponse.Properties["Error-Domain"]
	errorCode := subChangesResponse.Properties["Error-Code"]
	if errorDomain != "" && errorCode != "" {
		require.FailNow(btc.TB(), fmt.Sprintf("error %s %s from subChanges with body: %s", errorDomain, errorCode, string(rspBody)))
	}
}

func (btc *BlipTesterCollectionClient) StopPush() {
	require.True(btc.TB(), btc.pushRunning.IsTrue(), "can't stop push replication - not running")
	btc.pushCtxCancel()

	// Wake up any waiting push loops to check for cancellation
	btc._seqCond.Broadcast()

	// wait for push replication to stop running
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		require.True(c, btc.pushRunning.IsTrue() == false)
	}, 10*time.Second, 1*time.Millisecond)

}

// UnsubPullChanges will send an UnsubChanges message to the server to stop the pull replication. Fails test harness if Sync Gateway responds with an error.
func (btc *BlipTesterCollectionClient) UnsubPullChanges() {
	unsubChangesRequest := blip.NewRequest()
	unsubChangesRequest.SetProfile(db.MessageUnsubChanges)

	btc.sendPullMsg(unsubChangesRequest)
	body, err := unsubChangesRequest.Response().Body()
	require.NoError(btc.TB(), err)
	require.Empty(btc.TB(), body)
}

// Close will empty the stored docs and close the underlying replications.
func (btc *BlipTesterCollectionClient) Close() {
	btc.ctxCancel()

	// wake up changes feeds to exit - don't need lock for sync.Cond
	btc._seqCond.Broadcast()

	btc.seqLock.Lock()
	defer btc.seqLock.Unlock()
	// empty storage
	btc._seqStore = make(map[clientSeq]*clientDoc, 0)
	btc._seqFromDocID = make(map[string]clientSeq, 0)

	btc.attachmentsLock.Lock()
	defer btc.attachmentsLock.Unlock()
	btc._attachments = make(map[string][]byte, 0)
}

func (btr *BlipTesterReplicator) sendMsg(msg *blip.Message) {
	require.True(btr.TB(), btr.bt.sender.Send(msg))
	btr.storeMessage(msg)
}

// upsertDoc will create or update the doc based on whether parentVersion is passed or not. Enforces MVCC update.
// body can be nil and the update will be treated as a tombstone/delete.
func (btc *BlipTesterCollectionClient) upsertDoc(docID string, parentVersion *DocVersion, body []byte) *clientDocRev {
	btc.seqLock.Lock()
	defer btc.seqLock.Unlock()
	oldSeq, ok := btc._seqFromDocID[docID]
	var doc *clientDoc
	if ok {
		require.NotNil(btc.TB(), parentVersion, "docID: %v already exists on the client with seq: %v - expecting to create doc based on nil parentVersion", docID, oldSeq)
		doc, ok = btc._seqStore[oldSeq]
		require.True(btc.TB(), ok, "seq %q for docID %q found but no doc in _seqStore", oldSeq, docID)
	} else {
		require.Nil(btc.TB(), parentVersion, "docID: %v was not found on the client - expecting to update doc based on parentVersion %v", docID, parentVersion)
		doc = &clientDoc{
			id:              docID,
			_latestSeq:      0,
			_revisionsBySeq: make(map[clientSeq]clientDocRev, 1),
			_seqsByVersions: make(map[DocVersion]clientSeq, 1),
		}
	}

	doc.lock.Lock()
	defer doc.lock.Unlock()

	newGen := 1
	var hlv db.HybridLogicalVector
	if parentVersion != nil {
		// grab latest version for this doc and make sure we're doing an upsert on top of it to avoid branching revisions
		latestRev, err := doc._latestRev()
		require.NoError(btc.TB(), err)
		latestVersion := latestRev.version
		// CV or RevTreeID match is enough to ensure we're updating the latest revision
		require.True(btc.TB(), parentVersion.CV == latestVersion.CV || parentVersion.RevTreeID == latestVersion.RevTreeID, "latest version for docID: %v is %v, expected parentVersion: %v", docID, latestVersion, parentVersion)
		if btc.UseHLV() {
			hlv = latestRev.HLV
		} else {
			newGen = parentVersion.RevIDGeneration() + 1
		}
	}

	body = btc.ProcessInlineAttachments(body, newGen)

	var docVersion DocVersion
	if btc.UseHLV() {
		newVersion := db.Version{SourceID: btc.parent.SourceID, Value: uint64(btc.hlc.Now())}
		require.NoError(btc.TB(), hlv.AddVersion(newVersion))
		docVersion = DocVersion{CV: *hlv.ExtractCurrentVersionFromHLV()}
	} else {
		digest := "abc" // TODO: Generate rev ID digest based on body hash?
		newRevID := fmt.Sprintf("%d-%s", newGen, digest)
		docVersion = DocVersion{RevTreeID: newRevID}
	}

	btc._seqLast++
	newSeq := btc._seqLast
	rev := clientDocRev{clientSeq: newSeq, version: docVersion, body: body, HLV: hlv, isDelete: body == nil}
	doc._addNewRev(rev)

	btc._seqStore[newSeq] = doc
	btc._seqFromDocID[docID] = newSeq
	delete(btc._seqStore, oldSeq)

	// new sequence written, wake up changes feeds
	btc._seqCond.Broadcast()

	return &rev
}

// Delete creates a tombstone for the document and returns a the current DocVersion and hlv. If the BlipTesterCollectionClient is using revtrees, hlv will be nil.
func (btc *BlipTesterCollectionClient) Delete(docID string, parentVersion *DocVersion) (DocVersion, *db.HybridLogicalVector) {
	require.NotNil(btc.TB(), parentVersion, "parentVersion must be provided for delete operation")
	newRev := btc.upsertDoc(docID, parentVersion, nil)
	return newRev.version, &newRev.HLV
}

// AddRev creates a revision on the client. This creates a new revision from the parentVersion and returns the new DocVersion.
// The rev ID is always: "N-abc", where N is rev generation for predictability.
func (btc *BlipTesterCollectionClient) AddRev(docID string, parentVersion *DocVersion, body []byte) DocVersion {
	newRev := btc.upsertDoc(docID, parentVersion, body)
	return newRev.version
}

// AddHLVRev creates a revision on the client. This returns an HLV in addition to DocVersion and is otherwise identical to AddRev
func (btc *BlipTesterCollectionClient) AddHLVRev(docID string, parentVersion *DocVersion, body []byte) (DocVersion, *db.HybridLogicalVector) {
	newRev := btc.upsertDoc(docID, parentVersion, body)
	return newRev.version, &newRev.HLV
}

// GetDocVersion fetches revid and cv directly from the bucket.  Used to support REST-based verification in btc tests
// even while REST only supports revTreeId
// TODO: This doesn't support multi-collection testing, btc.GetDocVersion uses
//
//	GetSingleTestDatabaseCollection()
func (btcc *BlipTesterCollectionClient) GetDocVersion(docID string) DocVersion {
	return btcc.parent.GetDocVersion(docID)
}

// GetDocVersion fetches revid and cv directly from the bucket.  Used to support REST-based verification in btc tests
// even while REST only supports revTreeId
func (btc *BlipTesterClient) GetDocVersion(docID string) DocVersion {
	collection, ctx := btc.rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalSync)
	require.NoError(btc.rt.TB(), err)
	if !btc.UseHLV() || doc.HLV == nil {
		return DocVersion{RevTreeID: doc.CurrentRev}
	}
	return DocVersion{RevTreeID: doc.CurrentRev, CV: db.Version{SourceID: doc.HLV.SourceID, Value: doc.HLV.Version}}
}

func (btc *BlipTesterCollectionClient) PushUnsolicitedRev(docID string, parentRev *DocVersion, body []byte) (version *DocVersion, err error) {
	return btc.PushRevWithHistory(docID, parentRev, body, 1, 0)
}

// PushRevWithHistory creates a revision on the client with history, and immediately sends a changes request for it.
func (btc *BlipTesterCollectionClient) PushRevWithHistory(docID string, parentVersion *DocVersion, body []byte, revCount, prunedRevCount int) (version *DocVersion, err error) {
	if btc.UseHLV() {
		// TODO: CBG-4427 - HLV not supported for PushRevWithHistory
		btc.TB().Skip("CBG-4427: HLV not supported for PushRevWithHistory")
	}

	ctx := base.DatabaseLogCtx(base.TestCtx(btc.parent.rt.TB()), btc.parent.rt.GetDatabase().Name, nil)
	parentRevGen := parentVersion.RevIDGeneration()
	revGen := parentRevGen + revCount + prunedRevCount

	var revisionHistory []string
	for i := revGen - 1; i > parentRevGen; i-- {
		// TODO: HLV/CV support from anemone branch
		rev := fmt.Sprintf("%d-%s", i, "abc")
		revisionHistory = append(revisionHistory, rev)
	}

	// Inline attachment processing
	body = btc.ProcessInlineAttachments(body, revGen)

	var parentDocBody []byte
	if parentVersion != nil {
		doc, ok := btc.getClientDoc(docID)
		if !ok {
			return nil, fmt.Errorf("doc %s not found in client", docID)
		}
		doc.lock.RLock()
		parentDocBody = doc._revisionsBySeq[doc._seqsByVersions[*parentVersion]].body
		doc.lock.RUnlock()
	}

	newRevID := fmt.Sprintf("%d-%s", revGen, "abc")
	newRev := btc.upsertDoc(docID, parentVersion, body)

	// send a proposeChanges message with the single rev we just created on the client
	proposeChangesRequest := blip.NewRequest()
	proposeChangesRequest.SetProfile(db.MessageProposeChanges)
	var serverVersionComponent string
	if parentVersion != nil {
		serverVersionComponent = fmt.Sprintf(`,"%s"`, parentVersion.RevTreeID)
	}
	proposeChangesRequest.SetBody([]byte(fmt.Sprintf(`[["%s","%s"%s]]`, docID, newRevID, serverVersionComponent)))

	btc.addCollectionProperty(proposeChangesRequest)

	btc.sendPushMsg(proposeChangesRequest)

	proposeChangesResponse := proposeChangesRequest.Response()
	rspBody, err := proposeChangesResponse.Body()
	require.NoError(btc.TB(), err)
	require.NotContains(btc.TB(), proposeChangesResponse.Properties, "Error-Domain", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)
	require.NotContains(btc.TB(), proposeChangesResponse.Properties, "Error-Code", "unexpected error response from proposeChanges: %v, %s", proposeChangesResponse, rspBody)
	require.Equal(btc.TB(), "[]", string(rspBody))

	// send msg rev with new doc
	revRequest := blip.NewRequest()
	revRequest.SetProfile(db.MessageRev)
	revRequest.Properties[db.RevMessageID] = docID
	revRequest.Properties[db.RevMessageRev] = newRevID
	revRequest.Properties[db.RevMessageHistory] = strings.Join(revisionHistory, ",")

	btc.addCollectionProperty(revRequest)
	if btc.parent.ClientDeltas && proposeChangesResponse.Properties[db.ProposeChangesResponseDeltas] == "true" && parentVersion != nil {
		base.DebugfCtx(ctx, base.KeySync, "Sending deltas from test client from parent %v", parentVersion)
		var parentDocJSON, newDocJSON db.Body
		require.NoError(btc.TB(), parentDocJSON.Unmarshal(parentDocBody))
		require.NoError(btc.TB(), newDocJSON.Unmarshal(body))
		delta, err := base.Diff(parentDocJSON, newDocJSON)
		require.NoError(btc.TB(), err)
		revRequest.Properties[db.RevMessageDeltaSrc] = parentVersion.RevTreeID
		body = delta
	} else {
		base.DebugfCtx(ctx, base.KeySync, "Not sending deltas from test client")
	}

	revRequest.SetBody(body)

	btc.sendPushMsg(revRequest)

	revResponse := revRequest.Response()
	rspBody, err = revResponse.Body()
	require.NoError(btc.TB(), err)
	if revResponse.Type() == blip.ErrorType {
		return nil, fmt.Errorf("error %s %s from revResponse: %s", revResponse.Properties["Error-Domain"], revResponse.Properties["Error-Code"], rspBody)
	}

	btc.updateLastReplicatedVersion(docID, newRev.version)
	return &newRev.version, nil
}

func (btc *BlipTesterCollectionClient) StoreRevOnClient(docID string, parentVersion *DocVersion, body []byte) {
	btc.upsertDoc(docID, parentVersion, body)
}

func (btc *BlipTesterCollectionClient) ProcessInlineAttachments(inputBody []byte, revGen int) (outputBody []byte) {
	if !bytes.Contains(inputBody, []byte(db.BodyAttachments)) {
		return inputBody
	}
	var newDocJSON map[string]interface{}
	require.NoError(btc.TB(), base.JSONUnmarshal(inputBody, &newDocJSON))
	attachments, ok := newDocJSON[db.BodyAttachments]
	if !ok {
		return inputBody
	}
	attachmentMap, ok := attachments.(map[string]interface{})
	require.True(btc.TB(), ok)
	for attachmentName, inlineAttachment := range attachmentMap {
		inlineAttachmentMap := inlineAttachment.(map[string]interface{})
		attachmentData, ok := inlineAttachmentMap["data"]
		if !ok {
			isStub, _ := inlineAttachmentMap["stub"].(bool)
			require.True(btc.TB(), isStub, "couldn't find data and stub property for inline attachment %#v : %v", attachmentName, inlineAttachmentMap)
			// push the stub as-is
			continue
		}

		// Transform inline attachment data into metadata
		data, ok := attachmentData.(string)
		require.True(btc.TB(), ok, "inline attachment data was not a string, got %T", attachmentData)

		length, digest := btc.saveAttachment(data)

		attachmentMap[attachmentName] = map[string]interface{}{
			"digest": digest,
			"length": length,
			"stub":   true,
		}
		if !btc.UseHLV() {
			attachmentMap[attachmentName].(map[string]interface{})["revpos"] = revGen
		}
		newDocJSON[db.BodyAttachments] = attachmentMap
	}
	return base.MustJSONMarshal(btc.TB(), newDocJSON)
}

// GetVersion returns the data stored in the Client under the given docID and version
func (btc *BlipTesterCollectionClient) GetVersion(docID string, docVersion DocVersion) (data []byte, found bool) {
	doc, ok := btc.getClientDoc(docID)
	if !ok {
		return nil, false
	}
	var lookupVersion DocVersion
	if btc.UseHLV() {
		lookupVersion = DocVersion{CV: docVersion.CV}
	} else {
		lookupVersion = DocVersion{RevTreeID: docVersion.RevTreeID}
	}
	doc.lock.RLock()
	defer doc.lock.RUnlock()
	revSeq, ok := doc._seqsByVersions[lookupVersion]
	if !ok {
		return nil, false
	}

	rev, ok := doc._revisionsBySeq[revSeq]
	require.True(btc.TB(), ok, "seq %q for docID %q found but no rev in _seqStore", revSeq, docID)

	return rev.body, true
}

func (btc *BlipTesterClient) UseHLV() bool {
	for _, protocol := range btc.SupportedBLIPProtocols {
		subProtocol, err := db.ParseSubprotocolString(protocol)
		require.NoError(btc.rt.TB(), err)
		if subProtocol >= db.CBMobileReplicationV4 {
			return true
		}
	}
	return false
}

func (btc *BlipTesterClient) AssertOnBlipHistory(t *testing.T, msg *blip.Message, docVersion DocVersion) {
	subProtocol, err := db.ParseSubprotocolString(btc.SupportedBLIPProtocols[0])
	require.NoError(t, err)
	if subProtocol >= db.CBMobileReplicationV4 { // history could be empty a lot of the time in HLV messages as updates from the same source won't populate previous versions
		if msg.Properties[db.RevMessageHistory] != "" {
			assert.Equal(t, docVersion.CV.String(), msg.Properties[db.RevMessageHistory])
		}
	} else {
		assert.Equal(t, docVersion.RevTreeID, msg.Properties[db.RevMessageHistory])
	}
}

// WaitForVersion blocks until the given document version has been stored by the client, and returns the data when found. The test will fail after 10 seconds if a matching document is not found.
func (btc *BlipTesterCollectionClient) WaitForVersion(docID string, docVersion DocVersion) (data []byte) {
	if data, found := btc.GetVersion(docID, docVersion); found {
		return data
	}
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var found bool
		data, found = btc.GetVersion(docID, docVersion)
		assert.True(c, found, "Could not find docID:%+v Version %+v", docID, docVersion)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v Version %+v", docID, docVersion)
	return data
}

// WaitForDoc blocks until any document with the doc ID has been stored by the client, and returns the document body when found. If a document will be reported multiple times, the latest copy of the document is returned (not necessarily the first). The test will fail after 10 seconds if the document
func (btc *BlipTesterCollectionClient) WaitForDoc(docID string) (data []byte) {

	if data, _, version := btc.GetDoc(docID); version != nil {
		return data
	}
	timeout := 10 * time.Second
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var version *DocVersion
		data, _, version = btc.GetDoc(docID)
		assert.NotNil(c, version, "Could not find docID:%+v", docID)
	}, timeout, 5*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v after %s", docID, timeout)
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
func (btr *BlipTesterReplicator) GetMessages() map[blip.MessageNumber]blip.Message {
	btr.messagesLock.RLock()
	defer btr.messagesLock.RUnlock()

	messages := make(map[blip.MessageNumber]blip.Message, len(btr.messages))
	for k, v := range btr.messages {
		// Read the body before copying, since it might be read asynchronously
		_, _ = v.Body()
		messages[k] = *v
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
func (btc *BlipTesterCollectionClient) WaitForBlipRevMessage(docID string, version DocVersion) (msg *blip.Message) {
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var ok bool
		msg, ok = btc.GetBlipRevMessage(docID, version)
		assert.True(c, ok, "Could not find docID:%+v, Version: %+v", docID, version)
	}, 10*time.Second, 5*time.Millisecond, "BlipTesterClient timed out waiting for BLIP message docID: %v, Version: %v", docID, version)
	require.NotNil(btc.TB(), msg)
	return msg
}

// GetBLipRevMessage returns the rev message that wrote the given docID/DocVersion on the client.
func (btc *BlipTesterCollectionClient) GetBlipRevMessage(docID string, version DocVersion) (msg *blip.Message, found bool) {
	btc.seqLock.RLock()
	defer btc.seqLock.RUnlock()

	if doc, ok := btc._getClientDoc(docID); ok {
		var lookupVersion DocVersion
		if btc.UseHLV() {
			lookupVersion = DocVersion{CV: version.CV}
		} else {
			lookupVersion = DocVersion{RevTreeID: version.RevTreeID}
		}
		doc.lock.RLock()
		defer doc.lock.RUnlock()
		if seq, ok := doc._seqsByVersions[lookupVersion]; ok {
			if rev, ok := doc._revisionsBySeq[seq]; ok {
				require.NotNil(btc.TB(), rev.message, "rev.message is nil for docID:%+v, version: %+v", docID, version)
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
func (btcRunner *BlipTestClientRunner) WaitForBlipRevMessage(clientID uint32, docID string, version DocVersion) *blip.Message {
	return btcRunner.SingleCollection(clientID).WaitForBlipRevMessage(docID, version)
}

func (btcRunner *BlipTestClientRunner) StartOneshotPull(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartOneshotPull()
}

func (btcRunner *BlipTestClientRunner) StartOneshotPullFiltered(clientID uint32, channels string) {
	btcRunner.SingleCollection(clientID).StartOneshotPullFiltered(channels)
}

func (btcRunner *BlipTestClientRunner) StartOneshotPullRequestPlus(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartOneshotPullRequestPlus()
}

// AddRev creates a revision on the client. This creates a new revision from the parentVersion and returns the new DocVersion.
func (btcRunner *BlipTestClientRunner) AddRev(clientID uint32, docID string, parentVersion *DocVersion, body []byte) DocVersion {
	return btcRunner.SingleCollection(clientID).AddRev(docID, parentVersion, body)
}

// Delete creates a tombstone for the document and returns a the current DocVersion and hlv. If the BlipTesterCollectionClient is using revtrees, hlv will be nil.
func (btcRunner *BlipTestClientRunner) Delete(clientID uint32, docID string, version *DocVersion) (DocVersion, *db.HybridLogicalVector) {
	return btcRunner.SingleCollection(clientID).Delete(docID, version)
}

func (btcRunner *BlipTestClientRunner) PushUnsolicitedRev(clientID uint32, docID string, parentVersion *DocVersion, body []byte) (*DocVersion, error) {
	return btcRunner.SingleCollection(clientID).PushUnsolicitedRev(docID, parentVersion, body)
}

func (btcRunner *BlipTestClientRunner) StartPullSince(clientID uint32, options BlipTesterPullOptions) {
	btcRunner.SingleCollection(clientID).StartPullSince(options)
}

func (btcRunner *BlipTestClientRunner) GetVersion(clientID uint32, docID string, version DocVersion) ([]byte, bool) {
	return btcRunner.SingleCollection(clientID).GetVersion(docID, version)
}

// saveAttachment takes base64 encoded data and stores the attachment on the client.
func (btcRunner *BlipTestClientRunner) saveAttachment(clientID uint32, attachmentData string) (int, string) {
	return btcRunner.SingleCollection(clientID).saveAttachment(attachmentData)
}

func (btcRunner *BlipTestClientRunner) StoreRevOnClient(clientID uint32, docID string, parentVersion *DocVersion, body []byte) {
	btcRunner.SingleCollection(clientID).StoreRevOnClient(docID, parentVersion, body)
}

func (btcRunner *BlipTestClientRunner) PushRevWithHistory(clientID uint32, docID string, parentVersion *DocVersion, body []byte, revCount, prunedRevCount int) (*DocVersion, error) {
	return btcRunner.SingleCollection(clientID).PushRevWithHistory(docID, parentVersion, body, revCount, prunedRevCount)
}

func (btcRunner *BlipTestClientRunner) AttachmentsLock(clientID uint32) *sync.RWMutex {
	return &btcRunner.SingleCollection(clientID).attachmentsLock
}

func (btc *BlipTesterCollectionClient) AttachmentsLock() *sync.RWMutex {
	return &btc.attachmentsLock
}

func (btcRunner *BlipTestClientRunner) Attachments(clientID uint32) map[string][]byte {
	return btcRunner.SingleCollection(clientID)._attachments
}

func (btc *BlipTesterCollectionClient) Attachments() map[string][]byte {
	return btc._attachments
}

// UnsubPullChanges will send an UnsubChanges message to the server to stop the pull replication. Fails test harness if Sync Gateway responds with an error.
func (btcRunner *BlipTestClientRunner) UnsubPullChanges(clientID uint32) {
	btcRunner.SingleCollection(clientID).UnsubPullChanges()
}

func (btcRunner *BlipTestClientRunner) StopPush(clientID uint32) {
	btcRunner.SingleCollection(clientID).StopPush()
}

func (btc *BlipTesterCollectionClient) addCollectionProperty(msg *blip.Message) {
	if btc.collection != "" {
		msg.Properties[db.BlipCollection] = strconv.Itoa(btc.collectionIdx)
	}
}

// addCollectionProperty will automatically add a collection. If we are running with the default collection, or a single named collection, automatically add the right value. If there are multiple collections on the database, the test will fatally exit, since the behavior is undefined.
func (bt *BlipTesterClient) addCollectionProperty(msg *blip.Message) *blip.Message {
	if bt.nonCollectionAwareClient == nil {
		require.Equal(bt.TB(), 1, len(bt.collectionClients), "Multiple collection clients, exist so assuming that the only named collection is the single element of an array is not valid")
		msg.Properties[db.BlipCollection] = "0"
	}

	return msg
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

func (btc *BlipTesterCollectionClient) sendPullMsg(msg *blip.Message) {
	btc.addCollectionProperty(msg)
	btc.parent.pullReplication.sendMsg(msg)
}

func (btc *BlipTesterCollectionClient) sendPushMsg(msg *blip.Message) {
	btc.addCollectionProperty(msg)
	btc.parent.pushReplication.sendMsg(msg)
}

func (c *BlipTesterCollectionClient) lastSeq() clientSeq {
	c.seqLock.RLock()
	defer c.seqLock.RUnlock()
	return c._seqLast
}

func (btc *BlipTesterClient) AssertDeltaSrcProperty(t *testing.T, msg *blip.Message, docVersion DocVersion) {
	subProtocol, err := db.ParseSubprotocolString(btc.SupportedBLIPProtocols[0])
	require.NoError(t, err)
	rev := docVersion.GetRev(subProtocol >= db.CBMobileReplicationV4)
	assert.Equal(t, rev, msg.Properties[db.RevMessageDeltaSrc])
}
