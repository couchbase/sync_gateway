package rest

import (
	"encoding/json"
	"fmt"
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

	docs              map[string]map[string][]byte // Client's local store of documents - Map of docID to rev ID to bytes
	lastReplicatedRev map[string]string            // Latest known rev pulled or pushed
	docsLock          sync.RWMutex                 // lock for docs map

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
					latest, ok := btc.lastReplicatedRev[docID]
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

		// If deltas are enabled, and we see a deltaSrc property, we'll need to patch it before storing
		if btc.ClientDeltas && deltaSrc != "" {
			// unmarshal body to extract deltaSrc
			var delta map[string]interface{}
			if err := json.Unmarshal(body, &delta); err != nil {
				panic(err)
			}

			var old map[string]interface{}
			btc.docsLock.RLock()
			oldBytes := btc.docs[docID][deltaSrc]
			btc.docsLock.RUnlock()
			if err := json.Unmarshal(oldBytes, &old); err != nil {
				panic(err)
			}

			if err := base.Patch(&old, body); err != nil {
				panic(err)
			}

			body, err = json.Marshal(old)
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

	btr.bt.blipContext.DefaultHandler = func(msg *blip.Message) {
		btr.storeMessage(msg)
		panic("unexpected msg to DefaultHandler")
	}
}

func (btc *BlipTesterClient) updateLastReplicatedRev(docID, revID string) {

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

// Close will empty GetServerUUIDthe stored docs, close the underlying replications, and finally close the rest tester.
func (btc *BlipTesterClient) Close() {
	btc.docsLock.Lock()
	btc.docs = make(map[string]map[string][]byte, 0)
	btc.lastReplicatedRev = make(map[string]string, 0)
	btc.docsLock.Unlock()

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
func (btc *BlipTesterClient) PushRev(docID, parentRev string, body []byte) (revID string, err error) {
	var parentDocBody []byte

	// generate fake rev for gen+1
	parentRevGen, _ := db.ParseRevID(parentRev)
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
		var parentDocJSON, newDocJSON map[string]interface{}
		err := json.Unmarshal(parentDocBody, &parentDocJSON)
		if err != nil {
			return "", err
		}

		err = json.Unmarshal(body, &newDocJSON)
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
	btc.updateLastReplicatedRev(docID, revID)

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
	btr.messages[msg.SerialNumber()] = msg
}
