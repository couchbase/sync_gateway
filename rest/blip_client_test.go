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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
)

type BlipTesterClientOpts struct {
	ClientDeltas           bool // Support deltas on the client side
	Username               string
	Channels               []string
	SendRevocations        bool
	SupportedBLIPProtocols []string

	rejectDeltasForSrcRev string // a deltaSrc rev ID for which to reject a delta
}

// BlipTesterClient is a fully fledged client to emulate CBL behaviour on both push and pull replications through methods on this type.
type BlipTesterClient struct {
	BlipTesterClientOpts

	rt *RestTester

	docs map[string]map[string]*BodyMessagePair // Client's local store of documents - Map of docID
	// to rev ID to bytes
	attachments           map[string][]byte // Client's local store of attachments - Map of digest to bytes
	lastReplicatedRev     map[string]string // Latest known rev pulled or pushed
	docsLock              sync.RWMutex      // lock for docs map
	attachmentsLock       sync.RWMutex      // lock for attachments map
	lastReplicatedRevLock sync.RWMutex      // lock for lastReplicatedRev map

	pullReplication *BlipTesterReplicator // SG -> CBL replications
	pushReplication *BlipTesterReplicator // CBL -> SG replications
}

type BodyMessagePair struct {
	body    []byte
	message *blip.Message
}

// BlipTesterReplicator is a BlipTester which stores a map of messages keyed by Serial Number
type BlipTesterReplicator struct {
	bt *BlipTester
	id string // Generated UUID on creation

	messagesLock sync.RWMutex                         // lock for messages map
	messages     map[blip.MessageNumber]*blip.Message // Map of blip messages keyed by message number

	replicationStats *db.BlipSyncStats // Stats of replications
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

	btr.bt.blipContext.HandlerForProfile[db.MessageProveAttachment] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		nonce, err := msg.Body()
		if err != nil {
			panic(err)
		}

		if len(nonce) == 0 {
			panic("no nonce sent with proveAttachment")
		}

		digest, ok := msg.Properties[db.ProveAttachmentDigest]
		if !ok {
			panic("no digest sent with proveAttachment")
		}

		attData, err := btc.getAttachment(digest)
		if err != nil {
			panic(fmt.Sprintf("error getting client attachment: %v", err))
		}

		proof := db.ProveAttachment(attData, nonce)

		resp := msg.Response()
		resp.SetBody([]byte(proof))
		btr.replicationStats.ProveAttachment.Add(1)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageChanges] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		// Exit early when there's nothing to do
		if msg.NoReply() {
			return
		}

		body, err := msg.Body()
		if err != nil {
			panic(err)
		}

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			if err != nil {
				panic(err)
			}

			knownRevs = make([]interface{}, len(changesReqs))
			// changesReqs == [[sequence, docID, revID, {deleted}, {size (bytes)}], ...]
			btc.docsLock.RLock() // TODO: Move locking to accessor methods
		outer:
			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)

				deletedInt := 0
				if len(changesReq) > 3 {
					castedDeleted, ok := changesReq[3].(float64)
					if ok {
						deletedInt = int(castedDeleted)
					}
				}

				// Build up a list of revisions known to the client for each change
				// The first element of each revision list must be the parent revision of the change
				if revs, haveDoc := btc.docs[docID]; haveDoc {
					revList := make([]string, 0, len(revs))

					// Insert the highest ancestor rev generation at the start of the revList
					latest, ok := btc.getLastReplicatedRev(docID)
					if ok {
						revList = append(revList, latest)
					}

					for knownRevID := range revs {
						if deletedInt&2 == 2 {
							continue
						}

						if revID == knownRevID {
							knownRevs[i] = nil // Send back null to signal we don't need this change
							continue outer
						} else if latest == knownRevID {
							// We inserted this rev as the first element above, so skip it here
							continue
						}

						// TODO: Limit known revs to 20 to copy CBL behaviour
						revList = append(revList, knownRevID)
					}

					knownRevs[i] = revList
				} else {
					knownRevs[i] = []interface{}{} // sending empty array means we've not seen the doc before, but still want it
				}

			}
			btc.docsLock.RUnlock()
		}

		response := msg.Response()
		if btc.ClientDeltas {
			// Enable deltas from the client side
			response.Properties["deltas"] = "true"
		}

		b, err := base.JSONMarshal(knownRevs)
		if err != nil {
			panic(err)
		}

		response.SetBody(b)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageProposeChanges] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageRev] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

		docID := msg.Properties[db.RevMessageId]
		revID := msg.Properties[db.RevMessageRev]
		deltaSrc := msg.Properties[db.RevMessageDeltaSrc]

		body, err := msg.Body()
		if err != nil {
			panic(err)
		}

		if msg.Properties[db.RevMessageDeleted] == "1" {
			btc.docsLock.Lock()
			defer btc.docsLock.Unlock()
			if _, ok := btc.docs[docID]; ok {
				bodyMessagePair := &BodyMessagePair{body: body, message: msg}
				btc.docs[docID][revID] = bodyMessagePair
			} else {
				bodyMessagePair := &BodyMessagePair{body: body, message: msg}
				btc.docs[docID] = map[string]*BodyMessagePair{revID: bodyMessagePair}
			}
			btc.updateLastReplicatedRev(docID, revID)

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
				} else {
					panic("expected delta rev message to be sent without noreply flag")
				}
			}

			// unmarshal body to extract deltaSrc
			var delta db.Body
			if err := delta.Unmarshal(body); err != nil {
				panic(err)
			}

			var old db.Body
			btc.docsLock.RLock()
			oldBytes := btc.docs[docID][deltaSrc].body
			btc.docsLock.RUnlock()
			if err := old.Unmarshal(oldBytes); err != nil {
				panic(err)
			}

			var oldMap = map[string]interface{}(old)
			if err := base.Patch(&oldMap, delta); err != nil {
				panic(err)
			}

			bodyJSON = oldMap
		}

		// Fetch any missing attachments (if required) during this rev processing
		if bytes.Contains(body, []byte(db.BodyAttachments)) {

			// We'll need to unmarshal the body in order to do attachment processing
			if bodyJSON == nil {
				if err := bodyJSON.Unmarshal(body); err != nil {
					panic(err)
				}
			}

			if atts, ok := bodyJSON[db.BodyAttachments]; ok {
				attsMap, ok := atts.(map[string]interface{})
				if !ok {
					panic("atts in doc wasn't map[string]interface{}")
				}

				var missingDigests []string
				btc.attachmentsLock.RLock()
				for _, attachment := range attsMap {
					attMap, ok := attachment.(map[string]interface{})
					if !ok {
						panic("att in doc wasn't map[string]interface{}")
					}
					digest := attMap["digest"].(string)

					if _, found := btc.attachments[digest]; !found {
						missingDigests = append(missingDigests, digest)
					}
				}
				btc.attachmentsLock.RUnlock()

				for _, digest := range missingDigests {
					outrq := blip.NewRequest()
					outrq.SetProfile(db.MessageGetAttachment)
					outrq.Properties[db.GetAttachmentDigest] = digest
					if btr.bt.blipContext.ActiveSubprotocol() == db.BlipCBMobileReplicationV3 {
						outrq.Properties[db.GetAttachmentID] = docID
					}

					err := btc.pullReplication.sendMsg(outrq)
					if err != nil {
						panic(err)
					}

					resp := outrq.Response()
					btc.pullReplication.storeMessage(resp)
					respBody, err := resp.Body()
					if err != nil {
						panic(err)
					}

					if resp.Type() == blip.ErrorType {
						// forward error from getAttachment response into rev response
						if !msg.NoReply() {
							response := msg.Response()
							errorCode, _ := strconv.Atoi(resp.Properties["Error-Code"])
							response.SetError(resp.Properties["Error-Code"], errorCode, string(respBody))
							return
						}
					}

					btc.attachmentsLock.Lock()
					btc.attachments[digest] = respBody
					btc.attachmentsLock.Unlock()
				}
			}

		}

		if bodyJSON != nil {
			body, err = base.JSONMarshal(bodyJSON)
			if err != nil {
				panic(err)
			}
		}

		btc.docsLock.Lock()
		defer btc.docsLock.Unlock()

		if _, ok := btc.docs[docID]; ok {
			bodyMessagePair := &BodyMessagePair{body: body, message: msg}
			btc.docs[docID][revID] = bodyMessagePair
		} else {
			bodyMessagePair := &BodyMessagePair{body: body, message: msg}
			btc.docs[docID] = map[string]*BodyMessagePair{revID: bodyMessagePair}
		}
		btc.updateLastReplicatedRev(docID, revID)

		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = func(msg *blip.Message) {
		btr.storeMessage(msg)

		digest, ok := msg.Properties[db.GetAttachmentDigest]
		if !ok {
			base.PanicfCtx(context.TODO(), "couldn't find digest in getAttachment message properties")
		}

		attachment, err := btc.getAttachment(digest)
		if err != nil {
			base.PanicfCtx(context.TODO(), "couldn't find attachment for digest: %v", digest)
		}

		response := msg.Response()
		response.SetBody(attachment)
		btr.replicationStats.GetAttachment.Add(1)
	}

	btr.bt.blipContext.HandlerForProfile[db.MessageNoRev] = func(msg *blip.Message) {
		// TODO: Support norev messages
		btr.storeMessage(msg)
	}

	btr.bt.blipContext.DefaultHandler = func(msg *blip.Message) {
		btr.storeMessage(msg)
		base.PanicfCtx(context.TODO(), "Unknown profile: %s caught by client DefaultHandler - msg: %#v", msg.Profile(), msg)
	}
}

// saveAttachment takes a content-type, and base64 encoded data and stores the attachment on the client
func (btc *BlipTesterClient) saveAttachment(contentType, base64data string) (dataLength int, digest string, err error) {
	btc.attachmentsLock.Lock()
	defer btc.attachmentsLock.Unlock()

	data, err := base64.StdEncoding.DecodeString(base64data)
	if err != nil {
		return 0, "", err
	}

	digest = db.Sha1DigestKey(data)
	if _, found := btc.attachments[digest]; found {
		return 0, "", fmt.Errorf("attachment with digest already exists")
	}

	btc.attachments[digest] = data
	return len(data), digest, nil
}

func (btc *BlipTesterClient) getAttachment(digest string) (attachment []byte, err error) {
	btc.attachmentsLock.RLock()
	defer btc.attachmentsLock.RUnlock()

	attachment, found := btc.attachments[digest]
	if !found {
		return nil, fmt.Errorf("attachment not found")
	}

	return attachment, nil
}

func (btc *BlipTesterClient) updateLastReplicatedRev(docID, revID string) {
	btc.lastReplicatedRevLock.Lock()
	defer btc.lastReplicatedRevLock.Unlock()

	currentRevID, ok := btc.lastReplicatedRev[docID]
	if !ok {
		btc.lastReplicatedRev[docID] = revID
		return
	}

	currentGen, _ := db.ParseRevID(currentRevID)
	incomingGen, _ := db.ParseRevID(revID)
	if incomingGen > currentGen {
		btc.lastReplicatedRev[docID] = revID
	}
}

func (btc *BlipTesterClient) getLastReplicatedRev(docID string) (revID string, ok bool) {
	btc.lastReplicatedRevLock.RLock()
	defer btc.lastReplicatedRevLock.RUnlock()

	revID, ok = btc.lastReplicatedRev[docID]
	return revID, ok
}

func newBlipTesterReplication(tb testing.TB, id string, btc *BlipTesterClient) (*BlipTesterReplicator, error) {
	bt, err := NewBlipTesterFromSpecWithRT(tb, &BlipTesterSpec{
		connectingPassword:          "test",
		connectingUsername:          btc.Username,
		connectingUserChannelGrants: btc.Channels,
		blipProtocols:               btc.SupportedBLIPProtocols,
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

func createBlipTesterClientOpts(tb testing.TB, rt *RestTester, opts *BlipTesterClientOpts) (client *BlipTesterClient, err error) {
	if opts == nil {
		opts = &BlipTesterClientOpts{}
	}

	btc := BlipTesterClient{
		BlipTesterClientOpts: *opts,
		rt:                   rt,
		docs:                 make(map[string]map[string]*BodyMessagePair),
		attachments:          make(map[string][]byte),
		lastReplicatedRev:    make(map[string]string),
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	if btc.pushReplication, err = newBlipTesterReplication(tb, "push"+id.String(), &btc); err != nil {
		return nil, err
	}
	if btc.pullReplication, err = newBlipTesterReplication(tb, "pull"+id.String(), &btc); err != nil {
		return nil, err
	}

	return &btc, nil
}

// NewBlipTesterClient returns a client which emulates the behaviour of a CBL client over BLIP.
func NewBlipTesterClient(tb testing.TB, rt *RestTester) (client *BlipTesterClient, err error) {
	return createBlipTesterClientOpts(tb, rt, nil)
}

func NewBlipTesterClientOptsWithRT(tb testing.TB, rt *RestTester, opts *BlipTesterClientOpts) (client *BlipTesterClient, err error) {
	client, err = createBlipTesterClientOpts(tb, rt, opts)
	if err != nil {
		return nil, err
	}
	client.pullReplication.bt.avoidRestTesterClose = true
	client.pushReplication.bt.avoidRestTesterClose = true

	return client, nil
}

// StartPull will begin a continuous pull replication since 0 between the client and server
func (btc *BlipTesterClient) StartPull() (err error) {
	return btc.StartPullSince("true", "0", "false")
}

func (btc *BlipTesterClient) StartOneshotPull() (err error) {
	return btc.StartPullSince("false", "0", "false")
}

// StartPullSince will begin a pull replication between the client and server with the given params.
func (btc *BlipTesterClient) StartPullSince(continuous, since, activeOnly string) (err error) {
	getCheckpointRequest := blip.NewRequest()
	getCheckpointRequest.SetProfile(db.MessageGetCheckpoint)
	getCheckpointRequest.Properties[db.BlipClient] = btc.pullReplication.id
	if err := btc.pullReplication.sendMsg(getCheckpointRequest); err != nil {
		return err
	}

	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile(db.MessageSubChanges)
	subChangesRequest.Properties[db.SubChangesContinuous] = continuous
	subChangesRequest.Properties[db.SubChangesSince] = since
	subChangesRequest.Properties[db.SubChangesActiveOnly] = activeOnly
	subChangesRequest.SetNoReply(true)

	if btc.BlipTesterClientOpts.SendRevocations {
		subChangesRequest.Properties[db.SubChangesRevocations] = "true"
	}

	if err := btc.pullReplication.sendMsg(subChangesRequest); err != nil {
		return err
	}

	return nil
}

// Close will empty the stored docs and close the underlying replications.
func (btc *BlipTesterClient) Close() {
	btc.docsLock.Lock()
	btc.docs = make(map[string]map[string]*BodyMessagePair, 0)
	btc.docsLock.Unlock()

	btc.lastReplicatedRevLock.Lock()
	btc.lastReplicatedRev = make(map[string]string, 0)
	btc.lastReplicatedRevLock.Unlock()

	btc.attachmentsLock.Lock()
	btc.attachments = make(map[string][]byte, 0)
	btc.attachmentsLock.Unlock()

	btc.pullReplication.Close()
	btc.pushReplication.Close()
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
func (btc *BlipTesterClient) PushRev(docID, parentRev string, body []byte) (revID string, err error) {
	return btc.PushRevWithHistory(docID, parentRev, body, 1, 0)
}

// PushRevWithHistory creates a revision on the client with history, and immediately sends a changes request for it.
func (btc *BlipTesterClient) PushRevWithHistory(docID, parentRev string, body []byte, revCount, prunedRevCount int) (revID string, err error) {
	parentRevGen, _ := db.ParseRevID(parentRev)
	revGen := parentRevGen + revCount + prunedRevCount

	var revisionHistory []string
	for i := revGen - 1; i > parentRevGen; i-- {
		rev := fmt.Sprintf("%d-%s", i, "abc")
		revisionHistory = append(revisionHistory, rev)
	}

	// Inline attachment processing
	body, err = btc.ProcessInlineAttachments(body, revGen)
	if err != nil {
		return "", err
	}

	var parentDocBody []byte
	newRevID := fmt.Sprintf("%d-%s", revGen, "abc")
	btc.docsLock.Lock()
	if parentRev != "" {
		revisionHistory = append(revisionHistory, parentRev)
		if _, ok := btc.docs[docID]; ok {
			// create new rev if doc and parent rev already exists
			if parentDoc, okParent := btc.docs[docID][parentRev]; okParent {
				parentDocBody = parentDoc.body
				bodyMessagePair := &BodyMessagePair{body: body}
				btc.docs[docID][newRevID] = bodyMessagePair
			} else {
				btc.docsLock.Unlock()
				return "", fmt.Errorf("docID: %v with parent rev: %v was not found on the client", docID, parentRev)
			}
		} else {
			btc.docsLock.Unlock()
			return "", fmt.Errorf("docID: %v was not found on the client", docID)
		}
	} else {
		// create new doc + rev
		if _, ok := btc.docs[docID]; !ok {
			bodyMessagePair := &BodyMessagePair{body: body}
			btc.docs[docID] = map[string]*BodyMessagePair{newRevID: bodyMessagePair}
		}
	}
	btc.docsLock.Unlock()

	// send msg proposeChanges with rev
	proposeChangesRequest := blip.NewRequest()
	proposeChangesRequest.SetProfile(db.MessageProposeChanges)
	proposeChangesRequest.SetBody([]byte(fmt.Sprintf(`[["%s","%s","%s"]]`, docID, newRevID, parentRev)))
	if err := btc.pushReplication.sendMsg(proposeChangesRequest); err != nil {
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
	revRequest.Properties[db.RevMessageId] = docID
	revRequest.Properties[db.RevMessageRev] = newRevID
	revRequest.Properties[db.RevMessageHistory] = strings.Join(revisionHistory, ",")

	if btc.ClientDeltas && proposeChangesResponse.Properties[db.ProposeChangesResponseDeltas] == "true" {
		base.DebugfCtx(context.TODO(), base.KeySync, "Sending deltas from test client")
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
		base.DebugfCtx(context.TODO(), base.KeySync, "Not sending deltas from test client")
	}

	revRequest.SetBody(body)
	if err := btc.pushReplication.sendMsg(revRequest); err != nil {
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

	btc.updateLastReplicatedRev(docID, newRevID)
	return newRevID, nil
}

func (btc *BlipTesterClient) StoreRevOnClient(docID, revID string, body []byte) error {
	revGen, _ := db.ParseRevID(revID)
	newBody, err := btc.ProcessInlineAttachments(body, revGen)
	if err != nil {
		return err
	}
	bodyMessagePair := &BodyMessagePair{body: newBody}
	btc.docs[docID] = map[string]*BodyMessagePair{revID: bodyMessagePair}
	return nil
}

func (btc *BlipTesterClient) ProcessInlineAttachments(inputBody []byte, revGen int) (outputBody []byte, err error) {
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

// GetRev returns the data stored in the Client under the given docID and revID
func (btc *BlipTesterClient) GetRev(docID, revID string) (data []byte, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if rev, ok := btc.docs[docID]; ok {
		if data, ok := rev[revID]; ok && data != nil {
			return data.body, true
		}
	}

	return nil, false
}

// WaitForRev blocks until the given doc ID and rev ID have been stored by the client, and returns the data when found.
func (btc *BlipTesterClient) WaitForRev(docID, revID string) (data []byte, found bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			btc.rt.tb.Fatalf("BlipTesterClient timed out waiting for doc ID: %v rev ID: %v", docID, revID)
			return nil, false
		case <-ticker.C:
			if data, found := btc.GetRev(docID, revID); found {
				return data, found
			}
		}
	}
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
		messages[k] = *v
	}

	return messages
}

// WaitForMessage blocks until the given message serial number has been stored by the replicator, and returns the message when found.
func (btr *BlipTesterReplicator) WaitForMessage(serialNumber blip.MessageNumber) (msg *blip.Message, found bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			btr.bt.restTester.tb.Fatalf("BlipTesterReplicator timed out waiting for BLIP message: %v", serialNumber)
			return nil, false
		case <-ticker.C:
			if msg, ok := btr.GetMessage(serialNumber); ok {
				return msg, ok
			}
		}
	}
}

func (btr *BlipTesterReplicator) storeMessage(msg *blip.Message) {
	btr.messagesLock.Lock()
	defer btr.messagesLock.Unlock()
	btr.messages[msg.SerialNumber()] = msg
}

func (btc *BlipTesterClient) WaitForBlipRevMessage(docId, revId string) (msg *blip.Message, found bool) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			btc.rt.tb.Fatalf("BlipTesterClient timed out waiting for BLIP message docId: %v, revId: %v", docId, revId)
			return nil, false
		case <-ticker.C:
			if data, found := btc.GetBlipRevMessage(docId, revId); found {
				return data, found
			}
		}
	}
}

func (btc *BlipTesterClient) GetBlipRevMessage(docId, revId string) (msg *blip.Message, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if rev, ok := btc.docs[docId]; ok {
		if pair, found := rev[revId]; found {
			found = pair.message != nil
			return pair.message, found
		}
	}

	return nil, false
}
