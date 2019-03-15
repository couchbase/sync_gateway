package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
)

// BlipTesterClient is a fully fledged client to emulate CBL behaviour on both push and pull replications through methods on this type.
type BlipTesterClient struct {
	ClientDeltas bool // Support deltas on the client side

	rt *RestTester

	docs                  map[string]map[string][]byte // Client's local store of documents - Map of docID to rev ID to bytes
	attachments           map[string][]byte            // Client's local store of attachments - Map of digest to bytes
	lastReplicatedRev     map[string]string            // Latest known rev pulled or pushed
	docsLock              sync.RWMutex                 // lock for docs map
	attachmentsLock       sync.RWMutex                 // lock for attachments map
	lastReplicatedRevLock sync.RWMutex                 // lock for lastReplicatedRev map

	pullReplication *BlipTesterReplicator // SG -> CBL replications
	pushReplication *BlipTesterReplicator // CBL -> SG replications
}

// BlipTesterReplicator is a BlipTester which stores a map of messages keyed by Serial Number
type BlipTesterReplicator struct {
	bt *BlipTester
	id string // Generated UUID on creation

	messagesLock sync.RWMutex                         // lock for messages map
	messages     map[blip.MessageNumber]*blip.Message // Map of blip messages keyed by message number
}

func (btr *BlipTesterReplicator) Close() {
	btr.messagesLock.Lock()
	btr.messages = make(map[blip.MessageNumber]*blip.Message, 0)
	btr.messagesLock.Unlock()
}

func (btr *BlipTesterReplicator) initHandlers(btc *BlipTesterClient) {
	btr.bt.blipContext.HandlerForProfile[messageChanges] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

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
			err = json.Unmarshal(body, &changesReqs)
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

		b, err := json.Marshal(knownRevs)
		if err != nil {
			panic(err)
		}

		response.SetBody(b)
	}

	btr.bt.blipContext.HandlerForProfile[messageProposeChanges] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)
	}

	btr.bt.blipContext.HandlerForProfile[messageRev] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

		docID := msg.Properties[revMessageId]
		revID := msg.Properties[revMessageRev]
		deltaSrc := msg.Properties[revMessageDeltaSrc]

		body, err := msg.Body()
		if err != nil {
			panic(err)
		}

		// bodyJSON is unmarshalled into when required (e.g. Delta patching, or attachment processing)
		// Before being marshalled back into bytes for storage in the test client
		var bodyJSON db.Body

		// If deltas are enabled, and we see a deltaSrc property, we'll need to patch it before storing
		if btc.ClientDeltas && deltaSrc != "" {
			// unmarshal body to extract deltaSrc
			var delta db.Body
			if err := delta.Unmarshal(body); err != nil {
				panic(err)
			}

			var old db.Body
			btc.docsLock.RLock()
			oldBytes := btc.docs[docID][deltaSrc]
			btc.docsLock.RUnlock()
			if err := old.Unmarshal(oldBytes); err != nil {
				panic(err)
			}

			var oldMap = map[string]interface{}(old)
			if err := base.Patch(&oldMap, body); err != nil {
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
					outrq.SetProfile(messageGetAttachment)
					outrq.Properties[getAttachmentDigest] = digest

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
			body, err = json.Marshal(bodyJSON)
			if err != nil {
				panic(err)
			}
		}

		btc.docsLock.Lock()
		if _, ok := btc.docs[docID]; ok {
			btc.docs[docID][revID] = body
		} else {
			btc.docs[docID] = map[string][]byte{revID: body}
		}
		btc.updateLastReplicatedRev(docID, revID)
		btc.docsLock.Unlock()

		if !msg.NoReply() {
			response := msg.Response()
			response.SetBody([]byte(`[]`))
		}
	}

	btr.bt.blipContext.HandlerForProfile[messageGetAttachment] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

		digest, ok := msg.Properties[getAttachmentDigest]
		if !ok {
			base.Panicf(base.KeyAll, "couldn't find digest in getAttachment message properties")
		}

		attachment, err := btc.getAttachment(digest)
		if err != nil {
			base.Panicf(base.KeyAll, "couldn't find attachment for digest: %v", digest)
		}

		response := msg.Response()
		response.SetBody(attachment)
	}

	btr.bt.blipContext.HandlerForProfile[messageProveAttachment] = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)

		digest, ok := msg.Properties[getAttachmentDigest]
		if !ok {
			base.Panicf(base.KeyAll, "couldn't find digest in getAttachment message properties")
		}

		nonce, err := msg.Body()
		if err != nil {
			base.Panicf(base.KeyAll, "error reading body: %v", err)
		}

		attachment, err := btc.getAttachment(digest)
		if err != nil {
			base.Panicf(base.KeyAll, "couldn't find attachment for digest: %v", digest)
		}

		proof := db.GenerateProofOfAttachmentForNonce(attachment, nonce)
		response := msg.Response()
		response.SetBody([]byte(proof))
	}

	btr.bt.blipContext.DefaultHandler = func(msg *blip.Message) {
		btc.pullReplication.storeMessage(msg)
		base.Panicf(base.KeyAll, "Unknown profile: %s caught by client DefaultHandler - msg: %#v", msg.Profile(), msg)
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

func newBlipTesterReplication(id string, btc *BlipTesterClient) (*BlipTesterReplicator, error) {
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{restTester: btc.rt})
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

// NewBlipTesterClient returns a client which emulates the behaviour of a CBL client over BLIP.
func NewBlipTesterClient(rt *RestTester) (client *BlipTesterClient, err error) {
	btc := BlipTesterClient{
		rt:                rt,
		docs:              make(map[string]map[string][]byte),
		attachments:       make(map[string][]byte),
		lastReplicatedRev: make(map[string]string),
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	if btc.pushReplication, err = newBlipTesterReplication("push"+id.String(), &btc); err != nil {
		return nil, err
	}
	if btc.pullReplication, err = newBlipTesterReplication("pull"+id.String(), &btc); err != nil {
		return nil, err
	}

	return &btc, nil
}

// StartPull will begin a continuous pull replication since 0 between the client and server
func (btc *BlipTesterClient) StartPull() (err error) {
	return btc.StartPullSince("true", "0")
}

func (btc *BlipTesterClient) StartOneshotPull() (err error) {
	return btc.StartPullSince("false", "0")
}

// StartPullSince will begin a pull replication between the client and server with the given params.
func (btc *BlipTesterClient) StartPullSince(continuous, since string) (err error) {
	getCheckpointRequest := blip.NewRequest()
	getCheckpointRequest.SetProfile(messageGetCheckpoint)
	getCheckpointRequest.Properties[blipClient] = btc.pullReplication.id
	if err := btc.pullReplication.sendMsg(getCheckpointRequest); err != nil {
		return err
	}

	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile(messageSubChanges)
	subChangesRequest.Properties[subChangesContinuous] = continuous
	subChangesRequest.Properties[subChangesSince] = since
	subChangesRequest.SetNoReply(true)
	if err := btc.pullReplication.sendMsg(subChangesRequest); err != nil {
		return err
	}

	return nil
}

// Close will empty the stored docs and close the underlying replications.
func (btc *BlipTesterClient) Close() {
	btc.docsLock.Lock()
	btc.docs = make(map[string]map[string][]byte, 0)
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
// The rev ID is always: "N-abcxyz", where N is rev generation for predictability.
func (btc *BlipTesterClient) PushRev(docID, parentRev string, body []byte) (revID string, err error) {

	// generate fake rev for gen+1
	parentRevGen, _ := db.ParseRevID(parentRev)

	// Inline attachment processing
	if bytes.Contains(body, []byte(db.BodyAttachments)) {
		var newDocJSON map[string]interface{}
		if err = json.Unmarshal(body, &newDocJSON); err != nil {
			return "", err
		}
		if attachments, ok := newDocJSON[db.BodyAttachments]; ok {
			if attachmentMap, ok := attachments.(map[string]interface{}); ok {
				for attachmentName, inlineAttachment := range attachmentMap {
					inlineAttachmentMap := inlineAttachment.(map[string]interface{})

					// Check if the attachment is a stub, and we have an attachment of that digest
					if stub, ok := inlineAttachmentMap["stub"]; ok {
						if isStub, ok := stub.(bool); ok && isStub {
							if digest, ok := inlineAttachmentMap["digest"]; ok {
								if digestStr, ok := digest.(string); ok {
									if _, err := btc.getAttachment(digestStr); err == nil {
										attachmentMap[attachmentName] = inlineAttachmentMap
										continue
									}
								}
							}
						}
					}

					attachmentData, ok := inlineAttachmentMap["data"]
					if !ok {
						return "", fmt.Errorf("couldn't find data property for inline attachment")
					}
					attachmentContentType, ok := inlineAttachmentMap["content_type"]
					if !ok {
						// default to 'text/plain'
						attachmentContentType = "text/plain"
					}

					data, ok := attachmentData.(string)
					if !ok {
						return "", fmt.Errorf("inline attachment data was not a string")
					}
					contentType, ok := attachmentContentType.(string)
					if !ok {
						return "", fmt.Errorf("inline attachment content_type was not a string")
					}

					length, digest, err := btc.saveAttachment(contentType, data)
					if err != nil {
						return "", err
					}

					attachmentMap[attachmentName] = map[string]interface{}{
						"content_type": contentType,
						"digest":       digest,
						"length":       length,
						"revpos":       parentRevGen + 1,
						"stub":         true,
					}
					newDocJSON[db.BodyAttachments] = attachmentMap
				}
			}
			if body, err = json.Marshal(newDocJSON); err != nil {
				return "", err
			}
		}
	}

	var parentDocBody []byte
	newRevID := fmt.Sprintf("%d-%s", parentRevGen+1, "abcxyz")
	btc.docsLock.Lock()
	if parentRev != "" {
		if _, ok := btc.docs[docID]; ok {
			// create new rev if doc and parent rev already exists
			if parentDoc, okParent := btc.docs[docID][parentRev]; okParent {
				parentDocBody = parentDoc
				btc.docs[docID][newRevID] = body
			} else {
				return "", fmt.Errorf("docID: %v with parent rev: %v was not found on the client", docID, parentRev)
			}
		} else {
			return "", fmt.Errorf("docID: %v was not found on the client", docID)
		}
	} else {
		// create new doc + rev
		btc.docs[docID] = map[string][]byte{newRevID: body}
	}
	btc.docsLock.Unlock()

	// send msg proposeChanges with rev
	proposeChangesRequest := blip.NewRequest()
	proposeChangesRequest.SetProfile(messageProposeChanges)
	proposeChangesRequest.SetBody([]byte(fmt.Sprintf(`[["%s","%s","%s"]]`, docID, newRevID, parentRev)))
	if err := btc.pushReplication.sendMsg(proposeChangesRequest); err != nil {
		return "", err
	}

	proposeChangesResponse := proposeChangesRequest.Response()
	rspBody, err := proposeChangesResponse.Body()
	if err != nil || string(rspBody) != `[]` {
		return "", fmt.Errorf("error from proposeChangesResponse: %v %s\n", err, string(rspBody))
	}

	// send msg rev with new doc
	revRequest := blip.NewRequest()
	revRequest.SetProfile(messageRev)
	revRequest.Properties[revMessageId] = docID
	revRequest.Properties[revMessageRev] = newRevID

	if btc.ClientDeltas && proposeChangesResponse.Properties[proposeChangesResponseDeltas] == "true" {
		base.Debugf(base.KeySync, "TEST: sending deltas from test client")
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
		revRequest.Properties[revMessageDeltaSrc] = parentRev
		body = delta
	} else {
		base.Debugf(base.KeySync, "TEST: not sending deltas from client")
	}

	revRequest.SetBody(body)
	if err := btc.pushReplication.sendMsg(revRequest); err != nil {
		return "", err
	}

	revResponse := revRequest.Response()
	rspBody, err = revResponse.Body()
	if err != nil {
		return "", fmt.Errorf("error from revResponse: %v", err)
	}
	btc.updateLastReplicatedRev(docID, newRevID)

	return newRevID, nil
}

// GetRev returns the data stored in the Client under the given docID and revID
func (btc *BlipTesterClient) GetRev(docID, revID string) (data []byte, found bool) {
	btc.docsLock.RLock()
	defer btc.docsLock.RUnlock()

	if rev, ok := btc.docs[docID]; ok {
		data, found := rev[revID]
		return data, found
	}

	return nil, false
}

// WaitForRev blocks until the given doc ID and rev ID have been stored by the client, and returns the data when found.
func (btc *BlipTesterClient) WaitForRev(docID, revID string) (data []byte, found bool) {
	ticker := time.NewTicker(time.Millisecond * 50)
	for {
		select {
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

// WaitForMessage blocks until the given message serial number has been stored by the replicator, and returns the message when found.
func (btr *BlipTesterReplicator) WaitForMessage(serialNumber blip.MessageNumber) (msg *blip.Message, found bool) {
	ticker := time.NewTicker(time.Millisecond * 50)
	for {
		select {
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
	serial := msg.SerialNumber()
	if _, exists := btr.messages[serial]; exists {
		base.Panicf(base.KeyAll, "blip message with serial number %d already exists!", serial)
	}
	btr.messages[serial] = msg
}
