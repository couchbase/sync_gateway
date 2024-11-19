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
	"encoding/base64"
	"fmt"
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
}

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

type BlipTesterCollectionClient struct {
	parent          *BlipTesterClient
	collection      string
	collectionIdx   int
	docs            map[string]*BlipTesterDoc // Client's local store of documents, indexed by DocID
	attachments     map[string][]byte         // Client's local store of attachments - Map of digest to bytes
	docsLock        sync.RWMutex              // lock for docs map
	attachmentsLock sync.RWMutex              // lock for attachments map
}

// BlipTestClientRunner is for running the blip tester client and its associated methods in test framework
type BlipTestClientRunner struct {
	clients                     map[uint32]*BlipTesterClient // map of created BlipTesterClient's
	t                           *testing.T
	initialisedInsideRunnerCode bool            // flag to check that the BlipTesterClient is being initialised in the correct area (inside the Run() method)
	SkipSubtest                 map[string]bool // map of sub tests on the blip tester runner to skip
}

type BlipTesterDoc struct {
	revMode           blipTesterRevMode
	body              []byte
	revMessageHistory map[string]*blip.Message // History of rev messages received for this document, indexed by revID
	revHistory        []string                 // ordered history of revTreeIDs (newest first), populated when mode = revtree
	HLV               db.HybridLogicalVector   // HLV, populated when mode = HLV
}

const (
	revModeRevTree blipTesterRevMode = iota
	revModeHLV
)

type blipTesterRevMode uint32

func (doc *BlipTesterDoc) isRevKnown(revID string) bool {
	if doc.revMode == revModeHLV {
		version := VersionFromRevID(revID)
		return doc.HLV.IsVersionKnown(version)
	} else {
		for _, revTreeID := range doc.revHistory {
			if revTreeID == revID {
				return true
			}
		}
	}
	return false
}

func (doc *BlipTesterDoc) makeRevHistoryForChangesResponse() []string {
	if doc.revMode == revModeHLV {
		// For HLV, a changes response only needs to send cv, since rev message will always send full HLV
		return []string{doc.HLV.GetCurrentVersionString()}
	} else {
		var revList []string
		if len(doc.revHistory) < 20 {
			revList = doc.revHistory
		} else {
			revList = doc.revHistory[0:19]
		}
		return revList
	}
}

func (doc *BlipTesterDoc) getCurrentRevID() string {
	if doc.revMode == revModeHLV {
		return doc.HLV.GetCurrentVersionString()
	} else {
		if len(doc.revHistory) == 0 {
			return ""
		}
		return doc.revHistory[0]
	}
}

func (doc *BlipTesterDoc) addRevision(revID string, body []byte, message *blip.Message) {
	doc.revMessageHistory[revID] = message
	doc.body = body
	if doc.revMode == revModeHLV {
		_ = doc.HLV.AddVersion(VersionFromRevID(revID))
	} else {
		// prepend revID to revHistory
		doc.revHistory = append([]string{revID}, doc.revHistory...)
	}
}

func (btcr *BlipTesterCollectionClient) NewBlipTesterDoc(revID string, body []byte, message *blip.Message) *BlipTesterDoc {
	doc := &BlipTesterDoc{
		body:              body,
		revMessageHistory: map[string]*blip.Message{revID: message},
	}
	if btcr.UseHLV() {
		doc.revMode = revModeHLV
		doc.HLV = db.NewHybridLogicalVector()
		_ = doc.HLV.AddVersion(VersionFromRevID(revID))
	} else {
		doc.revMode = revModeRevTree
		doc.revHistory = []string{revID}
	}
	return doc
}

func VersionFromRevID(revID string) db.Version {
	version, err := db.ParseVersion(revID)
	if err != nil {
		panic(err)
	}
	return version
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

		attData, err := btcr.getAttachment(digest)
		require.NoError(btr.TB(), err, "error getting client attachment: %v", err)

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
			btcr.docsLock.RLock() // TODO: Move locking to accessor methods
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
				if doc, haveDoc := btcr.docs[docID]; haveDoc {
					if deletedInt&2 == 2 {
						continue
					}

					if doc.isRevKnown(revID) {
						knownRevs[i] = nil
						continue outer
					}
					knownRevs[i] = doc.makeRevHistoryForChangesResponse()
				} else {
					knownRevs[i] = []interface{}{} // sending empty array means we've not seen the doc before, but still want it
				}

			}
			btcr.docsLock.RUnlock()
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

		body, err := msg.Body()
		require.NoError(btr.TB(), err)

		if msg.Properties[db.RevMessageDeleted] == "1" {
			btcr.docsLock.Lock()
			defer btcr.docsLock.Unlock()
			var doc *BlipTesterDoc
			var ok bool
			if doc, ok = btcr.docs[docID]; !ok {
				doc = btcr.NewBlipTesterDoc(revID, body, msg)
				btcr.docs[docID] = doc
			}
			// Add replacedRev first to maintain ordering
			if replacedRev != "" {
				doc.addRevision(replacedRev, body, msg)
			}
			doc.addRevision(revID, body, msg)

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
				if !msg.NoReply() {
					response := msg.Response()
					response.SetError("HTTP", http.StatusUnprocessableEntity, "test code intentionally rejected delta")
					return
				}
				require.FailNow(btr.TB(), "expected delta rev message to be sent without noreply flag: %+v", msg)
			}

			// unmarshal body to extract deltaSrc
			var delta db.Body
			err := delta.Unmarshal(body)
			require.NoError(btc.TB(), err)

			var old db.Body
			btcr.docsLock.RLock()
			// deltaSrc must be the current rev
			doc := btcr.docs[docID]
			if doc.getCurrentRevID() != deltaSrc {
				panic("current rev doesn't match deltaSrc")
			}
			oldBytes := doc.body
			btcr.docsLock.RUnlock()
			err = old.Unmarshal(oldBytes)
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

					if _, found := btcr.attachments[digest]; !found {
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
					attData, err := btcr.getAttachment(digest)
					require.NoError(btr.TB(), err)
					nonce, proof, err := db.GenerateProofOfAttachment(ctx, attData)
					require.NoError(btr.TB(), err)

					// if we already have this attachment, _we_ should ask the peer whether _they_ have the attachment
					outrq := blip.NewRequest()
					outrq.SetProfile(db.MessageProveAttachment)
					outrq.Properties[db.ProveAttachmentDigest] = digest
					outrq.SetBody(nonce)

					err = btcr.sendPullMsg(outrq)
					require.NoError(btr.TB(), err)

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

					err := btcr.sendPullMsg(outrq)
					require.NoError(btr.TB(), err)

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
					btcr.attachments[digest] = respBody
					btcr.attachmentsLock.Unlock()
				}
			}

		}

		if bodyJSON != nil {
			body, err = base.JSONMarshal(bodyJSON)
			require.NoError(btr.TB(), err)
		}

		btcr.docsLock.Lock()
		defer btcr.docsLock.Unlock()

		var doc *BlipTesterDoc
		var ok bool
		if doc, ok = btcr.docs[docID]; !ok {
			doc = btcr.NewBlipTesterDoc(revID, body, msg)
			btcr.docs[docID] = doc
		}
		if replacedRev != "" {
			doc.addRevision(replacedRev, body, msg)
		}
		doc.addRevision(revID, body, msg)

		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		digest, ok := msg.Properties[db.GetAttachmentDigest]
		if !ok {
			base.PanicfCtx(ctx, "couldn't find digest in getAttachment message properties")
		}

		btcr := btc.getCollectionClientFromMessage(msg)

		attachment, err := btcr.getAttachment(digest)
		if err != nil {
			base.PanicfCtx(ctx, "couldn't find attachment for digest: %v", digest)
		}

		response := msg.Response()
		response.SetBody(attachment)
		btr.replicationStats.GetAttachment.Add(1)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageNoRev] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		btcr := btc.getCollectionClientFromMessage(msg)

		docID := msg.Properties[db.NorevMessageId]
		revID := msg.Properties[db.NorevMessageRev]

		btcr.docsLock.Lock()
		defer btcr.docsLock.Unlock()

		if doc, ok := btcr.docs[docID]; ok {
			doc.addRevision(revID, nil, msg)
		} else {
			btcr.docs[docID] = btcr.NewBlipTesterDoc(revID, nil, msg)
		}
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

// saveAttachment takes a content-type, and base64 encoded data and stores the attachment on the client
func (btc *BlipTesterCollectionClient) saveAttachment(_, base64data string) (dataLength int, digest string, err error) {
	btc.attachmentsLock.Lock()
	defer btc.attachmentsLock.Unlock()

	ctx := base.DatabaseLogCtx(base.TestCtx(btc.parent.rt.TB()), btc.parent.rt.GetDatabase().Name, nil)

	data, err := base64.StdEncoding.DecodeString(base64data)
	if err != nil {
		return 0, "", err
	}

	digest = db.Sha1DigestKey(data)
	if _, found := btc.attachments[digest]; found {
		base.InfofCtx(ctx, base.KeySync, "attachment with digest %s already exists", digest)
	} else {
		btc.attachments[digest] = data
	}

	return len(data), digest, nil
}

func (btc *BlipTesterCollectionClient) getAttachment(digest string) (attachment []byte, err error) {
	btc.attachmentsLock.RLock()
	defer btc.attachmentsLock.RUnlock()

	attachment, found := btc.attachments[digest]
	if !found {
		return nil, fmt.Errorf("attachment not found")
	}

	return attachment, nil
}

func newBlipTesterReplication(tb testing.TB, id string, btc *BlipTesterClient, skipCollectionsInitialization bool) (*BlipTesterReplicator, error) {
	bt, err := NewBlipTesterFromSpecWithRT(tb, &BlipTesterSpec{
		connectingPassword:            RestTesterDefaultUserPassword,
		connectingUsername:            btc.Username,
		connectingUserChannelGrants:   btc.Channels,
		blipProtocols:                 btc.SupportedBLIPProtocols,
		skipCollectionsInitialization: skipCollectionsInitialization,
		origin:                        btc.origin,
	}, btc.rt)
	if err != nil {
		return nil, err
	}

	r := &BlipTesterReplicator{
		id:       id,
		bt:       bt,
		messages: make(map[blip.MessageNumber]*blip.Message),
	}

	r.initHandlers(btc)

	return r, nil
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

	client = &BlipTesterClient{
		BlipTesterClientOpts: *opts,
		rt:                   rt,
		id:                   id.ID(),
	}
	btcRunner.clients[client.id] = client
	err = client.createBlipTesterReplications()
	require.NoError(btcRunner.TB(), err)

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

func (btc *BlipTesterClient) createBlipTesterReplications() error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	if btc.pushReplication, err = newBlipTesterReplication(btc.TB(), "push"+id.String(), btc, btc.BlipTesterClientOpts.SkipCollectionsInitialization); err != nil {
		return err
	}
	if btc.pullReplication, err = newBlipTesterReplication(btc.TB(), "pull"+id.String(), btc, btc.BlipTesterClientOpts.SkipCollectionsInitialization); err != nil {
		return err
	}

	collections := getCollectionsForBLIP(btc.TB(), btc.rt)
	if !btc.BlipTesterClientOpts.SkipCollectionsInitialization && len(collections) > 0 {
		btc.collectionClients = make([]*BlipTesterCollectionClient, len(collections))
		for i, collection := range collections {
			if err := btc.initCollectionReplication(collection, i); err != nil {
				return err
			}
		}
	} else {
		btc.nonCollectionAwareClient = &BlipTesterCollectionClient{
			docs:        make(map[string]*BlipTesterDoc),
			attachments: make(map[string][]byte),
			parent:      btc,
		}
	}

	btc.pullReplication.bt.avoidRestTesterClose = true
	btc.pushReplication.bt.avoidRestTesterClose = true

	return nil
}

func (btc *BlipTesterClient) initCollectionReplication(collection string, collectionIdx int) error {
	btcReplicator := &BlipTesterCollectionClient{
		docs:        make(map[string]*BlipTesterDoc),
		attachments: make(map[string][]byte),
		parent:      btc,
	}

	btcReplicator.collection = collection
	btcReplicator.collectionIdx = collectionIdx

	btc.collectionClients[collectionIdx] = btcReplicator
	return nil
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
	require.FailNow(btcRunner.clients[clientID].TB(), "Could not find collection %s in BlipTesterClient", collectionName)
	return nil
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
	subChangesRequest.SetNoReply(true)

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
	require.NoError(btc.TB(), btc.sendPullMsg(subChangesRequest))
}

func (btc *BlipTesterCollectionClient) UnsubPullChanges() (response []byte, err error) {
	unsubChangesRequest := blip.NewRequest()
	unsubChangesRequest.SetProfile(db.MessageUnsubChanges)

	err = btc.sendPullMsg(unsubChangesRequest)
	if err != nil {
		return nil, err
	}

	response, err = unsubChangesRequest.Response().Body()
	return response, err
}

func (btc *BlipTesterCollectionClient) UnsubPushChanges() (response []byte, err error) {
	unsubChangesRequest := blip.NewRequest()
	unsubChangesRequest.SetProfile(db.MessageUnsubChanges)

	err = btc.sendPushMsg(unsubChangesRequest)
	if err != nil {
		return nil, err
	}

	response, err = unsubChangesRequest.Response().Body()
	return response, err
}

// Close will empty the stored docs and close the underlying replications.
func (btc *BlipTesterCollectionClient) Close() {
	btc.docsLock.Lock()
	btc.docs = make(map[string]*BlipTesterDoc, 0)
	btc.docsLock.Unlock()

	btc.attachmentsLock.Lock()
	btc.attachments = make(map[string][]byte, 0)
	btc.attachmentsLock.Unlock()
}

func (btr *BlipTesterReplicator) sendMsg(msg *blip.Message) (err error) {

	if !btr.bt.sender.Send(msg) {
		return fmt.Errorf("error sending message")
	}
	btr.storeMessage(msg)
	return nil
}

// PushRev creates a revision on the client, and immediately sends a changes request for it.
// The rev ID is always: "N-abc", where N is rev generation for predictability.
func (btc *BlipTesterCollectionClient) PushRev(docID string, parentVersion DocVersion, body []byte) (DocVersion, error) {

	parentRev := parentVersion.GetRev(btc.UseHLV())
	revID, err := btc.PushRevWithHistory(docID, parentRev, body, 1, 0)
	if err != nil {
		return DocVersion{}, err
	}
	docVersion := btc.GetDocVersion(docID)
	btc.requireRevID(docVersion, revID)
	return docVersion, nil
}

func (btc *BlipTesterCollectionClient) requireRevID(expected DocVersion, revID string) {
	if btc.UseHLV() {
		require.Equal(btc.parent.rt.TB(), expected.CV.String(), revID)
	} else {
		require.Equal(btc.parent.rt.TB(), expected.RevTreeID, revID)
	}
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

// PushRevWithHistory creates a revision on the client with history, and immediately sends a changes request for it.
func (btc *BlipTesterCollectionClient) PushRevWithHistory(docID, parentRev string, body []byte, revCount, prunedRevCount int) (revID string, err error) {

	ctx := base.DatabaseLogCtx(base.TestCtx(btc.parent.rt.TB()), btc.parent.rt.GetDatabase().Name, nil)

	revGen := 0
	newRevID := ""
	var revisionHistory []string
	if btc.UseHLV() {
		// When using version vectors:
		//  - source is "abc"
		//  - version value is simple counter
		//  - revisionHistory is just previous cv (parentRev) for changes response
		startValue := uint64(0)
		if parentRev != "" {
			parentVersion, _ := db.ParseDecodedVersion(parentRev)
			startValue = parentVersion.Value
			revisionHistory = append(revisionHistory, parentRev)
		}
		newVersion := db.DecodedVersion{SourceID: "abc", Value: startValue + uint64(revCount)}
		newRevID = newVersion.String()

	} else {
		// When using revtrees:
		//  - all revIDs are of the form [generation]-abc
		//  - [revCount] history entries are generated between the parent and the new rev
		parentRevGen, _ := db.ParseRevID(ctx, parentRev)
		revGen = parentRevGen + revCount + prunedRevCount

		for i := revGen - 1; i > parentRevGen; i-- {
			rev := fmt.Sprintf("%d-%s", i, "abc")
			revisionHistory = append(revisionHistory, rev)
		}
		if parentRev != "" {

			revisionHistory = append(revisionHistory, parentRev)
		}
		newRevID = fmt.Sprintf("%d-%s", revGen, "abc")
	}

	// Inline attachment processing
	body, err = btc.ProcessInlineAttachments(body, revGen)
	if err != nil {
		return "", err
	}

	var parentDocBody []byte
	btc.docsLock.Lock()
	if parentRev != "" {
		if doc, ok := btc.docs[docID]; ok {
			// create new rev if doc exists and parent rev is current rev
			if doc.getCurrentRevID() == parentRev {
				parentDocBody = doc.body
				doc.addRevision(newRevID, body, nil)
			} else {
				btc.docsLock.Unlock()
				return "", fmt.Errorf("docID: %v with current rev: %v was not found on the client", docID, parentRev)
			}
		} else {
			btc.docsLock.Unlock()
			return "", fmt.Errorf("docID: %v was not found on the client", docID)
		}
	} else {
		// create new doc + rev
		if _, ok := btc.docs[docID]; !ok {
			btc.docs[docID] = btc.NewBlipTesterDoc(newRevID, body, nil)
		}
	}
	btc.docsLock.Unlock()

	// send msg proposeChanges with rev
	proposeChangesRequest := blip.NewRequest()
	proposeChangesRequest.SetProfile(db.MessageProposeChanges)
	proposeChangesRequest.SetBody([]byte(fmt.Sprintf(`[["%s","%s","%s"]]`, docID, newRevID, parentRev)))

	btc.addCollectionProperty(proposeChangesRequest)

	if err := btc.sendPushMsg(proposeChangesRequest); err != nil {
		return "", err
	}

	proposeChangesResponse := proposeChangesRequest.Response()
	rspBody, err := proposeChangesResponse.Body()
	if err != nil {
		return "", err
	}
	errorDomain := proposeChangesResponse.Properties["Error-Domain"]
	errorCode := proposeChangesResponse.Properties["Error-Code"]
	if errorDomain != "" && errorCode != "" {
		return "", fmt.Errorf("error %s %s from proposeChanges with body: %s", errorDomain, errorCode, string(rspBody))
	}
	if string(rspBody) != `[]` {
		return "", fmt.Errorf("unexpected body in proposeChangesResponse: %s", string(rspBody))
	}

	// send msg rev with new doc
	revRequest := blip.NewRequest()
	revRequest.SetProfile(db.MessageRev)
	revRequest.Properties[db.RevMessageID] = docID
	revRequest.Properties[db.RevMessageRev] = newRevID
	revRequest.Properties[db.RevMessageHistory] = strings.Join(revisionHistory, ",")

	btc.addCollectionProperty(revRequest)
	if btc.parent.ClientDeltas && proposeChangesResponse.Properties[db.ProposeChangesResponseDeltas] == "true" {
		base.DebugfCtx(ctx, base.KeySync, "Sending deltas from test client")
		var parentDocJSON, newDocJSON db.Body
		err := parentDocJSON.Unmarshal(parentDocBody)
		if err != nil {
			return "", err
		}

		err = newDocJSON.Unmarshal(body)
		if err != nil {
			return "", err
		}

		delta, err := base.Diff(parentDocJSON, newDocJSON)
		if err != nil {
			return "", err
		}
		revRequest.Properties[db.RevMessageDeltaSrc] = parentRev
		body = delta
	} else {
		base.DebugfCtx(ctx, base.KeySync, "Not sending deltas from test client")
	}

	revRequest.SetBody(body)

	if err := btc.sendPushMsg(revRequest); err != nil {
		return "", err
	}

	revResponse := revRequest.Response()
	rspBody, err = revResponse.Body()
	if err != nil {
		return "", fmt.Errorf("error getting body of revResponse: %v", err)
	}

	if revResponse.Type() == blip.ErrorType {
		return "", fmt.Errorf("error %s %s from revResponse: %s", revResponse.Properties["Error-Domain"], revResponse.Properties["Error-Code"], rspBody)
	}

	return newRevID, nil
}

func (btc *BlipTesterCollectionClient) StoreRevOnClient(docID, rev string, body []byte) error {
	ctx := base.DatabaseLogCtx(base.TestCtx(btc.parent.rt.TB()), btc.parent.rt.GetDatabase().Name, nil)

	revGen := 0
	if !btc.UseHLV() {
		revGen, _ = db.ParseRevID(ctx, rev)
	}
	newBody, err := btc.ProcessInlineAttachments(body, revGen)
	if err != nil {
		return err
	}
	btc.docsLock.Lock()
	defer btc.docsLock.Unlock()
	btc.docs[docID] = btc.NewBlipTesterDoc(rev, newBody, nil)
	return nil
}

func (btc *BlipTesterCollectionClient) ProcessInlineAttachments(inputBody []byte, revGen int) (outputBody []byte, err error) {
	if bytes.Contains(inputBody, []byte(db.BodyAttachments)) {
		var newDocJSON map[string]interface{}
		if err := base.JSONUnmarshal(inputBody, &newDocJSON); err != nil {
			return nil, err
		}
		if attachments, ok := newDocJSON[db.BodyAttachments]; ok {
			if attachmentMap, ok := attachments.(map[string]interface{}); ok {
				for attachmentName, inlineAttachment := range attachmentMap {
					inlineAttachmentMap := inlineAttachment.(map[string]interface{})
					attachmentData, ok := inlineAttachmentMap["data"]
					if !ok {
						if isStub, _ := inlineAttachmentMap["stub"].(bool); isStub {
							// push the stub as-is
							continue
						}
						return nil, fmt.Errorf("couldn't find data property for inline attachment")
					}

					// Transform inline attachment data into metadata
					data, ok := attachmentData.(string)
					if !ok {
						return nil, fmt.Errorf("inline attachment data was not a string")
					}

					contentType, _ := inlineAttachmentMap["content_type"].(string)

					length, digest, err := btc.saveAttachment(contentType, data)
					if err != nil {
						return nil, err
					}

					attachmentMap[attachmentName] = map[string]interface{}{
						"content_type": contentType,
						"digest":       digest,
						"length":       length,
						"revpos":       revGen,
						"stub":         true,
					}
					newDocJSON[db.BodyAttachments] = attachmentMap
				}
			}
			var err error
			if outputBody, err = base.JSONMarshal(newDocJSON); err != nil {
				return nil, err
			}
			return outputBody, nil
		}
	}
	return inputBody, nil
}

// GetCurrentRevID gets the current revID for the specified docID
func (btc *BlipTesterCollectionClient) GetCurrentRevID(docID string) (revID string, data []byte, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if doc, ok := btc.docs[docID]; ok {
		return doc.getCurrentRevID(), doc.body, true
	}

	return "", nil, false
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

// GetVersion returns the document body when the provided version matches the document's current revision
func (btc *BlipTesterCollectionClient) GetVersion(docID string, docVersion DocVersion) (data []byte, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if doc, ok := btc.docs[docID]; ok {
		if doc.revMode == revModeHLV {
			if doc.getCurrentRevID() == docVersion.CV.String() {
				return doc.body, true
			}
		} else {
			if doc.getCurrentRevID() == docVersion.RevTreeID {
				return doc.body, true
			}
		}
	}
	return nil, false
}

// WaitForVersion blocks until the given document version has been stored by the client, and returns the data when found. The test will fail after 10 seocnds if a matching document is not found.
func (btc *BlipTesterCollectionClient) WaitForVersion(docID string, docVersion DocVersion) (data []byte) {
	if data, found := btc.GetVersion(docID, docVersion); found {
		return data
	}
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var found bool
		data, found = btc.GetVersion(docID, docVersion)
		assert.True(c, found, "Could not find docID:%+v Version %+v", docID, docVersion)
	}, 10*time.Second, 50*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v Version %+v", docID, docVersion)
	return data
}

// GetDoc returns the current body stored in the Client for the given docID.
func (btc *BlipTesterCollectionClient) GetDoc(docID string) (data []byte, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if doc, ok := btc.docs[docID]; ok {
		return doc.body, true
	}

	return nil, false
}

// WaitForDoc blocks until any document with the doc ID has been stored by the client, and returns the document body when found. If a document will be reported multiple times, the latest copy of the document is returned (not necessarily the first). The test will fail after 10 seconds if the document
func (btc *BlipTesterCollectionClient) WaitForDoc(docID string) (data []byte) {

	if data, found := btc.GetDoc(docID); found {
		return data
	}
	timeout := 10 * time.Second
	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var found bool
		data, found = btc.GetDoc(docID)
		assert.True(c, found, "Could not find docID:%+v", docID)
	}, timeout, 50*time.Millisecond, "BlipTesterClient timed out waiting for doc %+v after %s", docID, timeout)
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
	}, 10*time.Second, 50*time.Millisecond, "BlipTesterReplicator timed out waiting for BLIP message: %v", serialNumber)
	return msg
}

func (btr *BlipTesterReplicator) storeMessage(msg *blip.Message) {
	btr.messagesLock.Lock()
	defer btr.messagesLock.Unlock()
	btr.messages[msg.SerialNumber()] = msg
}

// WaitForBlipRevMessage blocks until the given doc ID and rev ID has been stored by the client, and returns the message when found. If not found after 10 seconds, test will fail.
func (btc *BlipTesterCollectionClient) WaitForBlipRevMessage(docID string, version DocVersion) (msg *blip.Message) {
	var revID string
	if btc.UseHLV() {
		revID = version.CV.String()
	} else {
		revID = version.RevTreeID
	}

	require.EventuallyWithT(btc.TB(), func(c *assert.CollectT) {
		var ok bool
		msg, ok = btc.GetBlipRevMessage(docID, revID)
		assert.True(c, ok, "Could not find docID:%+v, RevID: %+v", docID, revID)
	}, 10*time.Second, 50*time.Millisecond, "BlipTesterClient timed out waiting for BLIP message docID: %v, revID: %v", docID, revID)
	return msg
}

func (btc *BlipTesterCollectionClient) GetBlipRevMessage(docID string, revID string) (msg *blip.Message, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if doc, ok := btc.docs[docID]; ok {
		if message, found := doc.revMessageHistory[revID]; found {
			return message, found
		}
	}

	return nil, false
}

func (btcRunner *BlipTestClientRunner) StartPull(clientID uint32) {
	btcRunner.SingleCollection(clientID).StartPull()
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

func (btcRunner *BlipTestClientRunner) PushRev(clientID uint32, docID string, version DocVersion, body []byte) (DocVersion, error) {
	return btcRunner.SingleCollection(clientID).PushRev(docID, version, body)
}

func (btcRunner *BlipTestClientRunner) StartPullSince(clientID uint32, options BlipTesterPullOptions) {
	btcRunner.SingleCollection(clientID).StartPullSince(options)
}

func (btcRunner *BlipTestClientRunner) GetVersion(clientID uint32, docID string, version DocVersion) ([]byte, bool) {
	return btcRunner.SingleCollection(clientID).GetVersion(docID, version)
}

func (btcRunner *BlipTestClientRunner) saveAttachment(clientID uint32, contentType string, attachmentData string) (int, string, error) {
	return btcRunner.SingleCollection(clientID).saveAttachment(contentType, attachmentData)
}

func (btcRunner *BlipTestClientRunner) StoreRevOnClient(clientID uint32, docID, revID string, body []byte) error {
	return btcRunner.SingleCollection(clientID).StoreRevOnClient(docID, revID, body)
}

func (btcRunner *BlipTestClientRunner) PushRevWithHistory(clientID uint32, docID, revID string, body []byte, revCount, prunedRevCount int) (string, error) {
	return btcRunner.SingleCollection(clientID).PushRevWithHistory(docID, revID, body, revCount, prunedRevCount)
}

func (btcRunner *BlipTestClientRunner) AttachmentsLock(clientID uint32) *sync.RWMutex {
	return &btcRunner.SingleCollection(clientID).attachmentsLock
}

func (btc *BlipTesterCollectionClient) AttachmentsLock() *sync.RWMutex {
	return &btc.attachmentsLock
}

func (btcRunner *BlipTestClientRunner) Attachments(clientID uint32) map[string][]byte {
	return btcRunner.SingleCollection(clientID).attachments
}

func (btc *BlipTesterCollectionClient) Attachments() map[string][]byte {
	return btc.attachments
}

func (btcRunner *BlipTestClientRunner) UnsubPullChanges(clientID uint32) ([]byte, error) {
	return btcRunner.SingleCollection(clientID).UnsubPullChanges()
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

func (btc *BlipTesterCollectionClient) sendPullMsg(msg *blip.Message) error {
	btc.addCollectionProperty(msg)
	return btc.parent.pullReplication.sendMsg(msg)
}

func (btc *BlipTesterCollectionClient) sendPushMsg(msg *blip.Message) error {
	btc.addCollectionProperty(msg)
	return btc.parent.pushReplication.sendMsg(msg)
}

// Wrappers for RT helpers that populate version information for use in HLV tests
// PutDoc will upsert the document with a given contents.
func (btc *BlipTesterClient) PutDoc(docID string, body string) DocVersion {
	rt := btc.rt
	version := rt.PutDoc(docID, body)
	if btc.UseHLV() {
		collection, _ := rt.GetSingleTestDatabaseCollection()
		source, value := collection.GetDocumentCurrentVersion(rt.TB(), docID)
		version.CV = db.Version{
			SourceID: source,
			Value:    value,
		}
	}
	return version
}

// RequireRev checks the current rev for the specified docID on the backend the BTC is replicating
// with (NOT in the btc store)
func (btc *BlipTesterClient) RequireRev(t *testing.T, expectedRev string, doc *db.Document) {
	if btc.UseHLV() {
		require.Equal(t, expectedRev, doc.HLV.GetCurrentVersionString())
	} else {
		require.Equal(t, expectedRev, doc.CurrentRev)
	}
}

func (btc *BlipTesterClient) AssertDeltaSrcProperty(t *testing.T, msg *blip.Message, docVersion DocVersion) {
	subProtocol, err := db.ParseSubprotocolString(btc.SupportedBLIPProtocols[0])
	require.NoError(t, err)
	rev := docVersion.GetRev(subProtocol >= db.CBMobileReplicationV4)
	assert.Equal(t, rev, msg.Properties[db.RevMessageDeltaSrc])
}
