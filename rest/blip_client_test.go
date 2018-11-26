package rest

import (
	"encoding/json"
	"sync"
	"time"

	blip "github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

type BlipTesterClient struct {
	Deltas bool // Support deltas on the client side

	bt *BlipTester
	id string // Generated UUID on creation

	docsLock sync.RWMutex                 // lock for docs map
	docs     map[string]map[string][]byte // Map of docID to rev ID to bytes

	messagesLock sync.RWMutex                         // lock for messages map
	messages     map[blip.MessageNumber]*blip.Message // Map of blip messages keyed by message number
}

// NewBlipTesterClient returns a client which emulates the behaviour of a CBL client over BLIP.
func NewBlipTesterClient(bt *BlipTester) (client *BlipTesterClient, err error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	btc := BlipTesterClient{
		id:       id.String(),
		bt:       bt,
		docs:     make(map[string]map[string][]byte, 0),
		messages: make(map[blip.MessageNumber]*blip.Message, 0),
	}

	bt.blipContext.HandlerForProfile[messageChanges] = func(request *blip.Message) {
		btc.storeMessage(request)

		// Exit early when there's nothing to do
		if request.NoReply() {
			return
		}

		body, err := request.Body()
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
				if revs, haveDoc := btc.docs[docID]; haveDoc {
					revList := make([]string, 0, len(revs))
					for knownRevID := range revs {
						if revID == knownRevID {
							knownRevs[i] = nil // Send back null to signal we don't need this change
							continue outer
						}
						// TODO: Limit known revs to 20 to copy CBL behaviour
						revList = append(revList, knownRevID)
					}
					knownRevs[i] = revList // send back all revs we have for SG to determine the ancestor
				} else {
					knownRevs[i] = []interface{}{} // sending empty array means we've not seen the doc before, but still want it
				}
			}
			btc.docsLock.RUnlock()
		}

		response := request.Response()
		if btc.Deltas {
			// Enable deltas from the client side
			response.Properties["deltas"] = "true"
		}

		b, err := json.Marshal(knownRevs)
		if err != nil {
			panic(err)
		}

		response.SetBody(b)
		btc.storeMessage(response)
	}

	bt.blipContext.HandlerForProfile[messageRev] = func(request *blip.Message) {
		btc.storeMessage(request)

		docID := request.Properties[revMessageId]
		revID := request.Properties[revMessageRev]
		deltaSrc := request.Properties[revMessageDeltaSrc]

		body, err := request.Body()
		if err != nil {
			panic(err)
		}

		// If deltas are enabled, and we see a deltaSrc property, we'll need to patch it before storing
		if btc.Deltas && deltaSrc != "" {
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
		btc.docsLock.Unlock()

		if !request.NoReply() {
			response := request.Response()
			response.SetBody([]byte(`[]`))
			btc.storeMessage(response)
		}
	}

	bt.blipContext.HandlerForProfile[messageGetAttachment] = func(request *blip.Message) {
		btc.storeMessage(request)
		panic("getAttachment not implemented")
	}

	bt.blipContext.DefaultHandler = func(request *blip.Message) {
		btc.storeMessage(request)
		panic("unexpected request to DefaultHandler")
	}

	return &btc, nil
}

// Start will begin a continous replication since 0 between the client and server
func (btc *BlipTesterClient) Start() {
	btc.StartSince("true", "0")
}

// Start will begin a replication between the client and server with the given params.
func (btc *BlipTesterClient) StartSince(continous, since string) {
	getCheckpointRequest := blip.NewRequest()
	getCheckpointRequest.SetProfile(messageGetCheckpoint)
	getCheckpointRequest.Properties[blipClient] = btc.id
	if !btc.bt.sender.Send(getCheckpointRequest) {
		panic("unable to send getCheckpointRequest")
	}

	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile(messageSubChanges)
	subChangesRequest.Properties[subChangesContinuous] = continous
	subChangesRequest.Properties[subChangesSince] = since
	subChangesRequest.SetNoReply(true)
	if !btc.bt.sender.Send(subChangesRequest) {
		panic("unable to send subChangesRequest")
	}
}

func (btc *BlipTesterClient) Close() {
	btc.bt.Close()
	btc.docsLock.Lock()
	btc.docs = nil
	btc.docsLock.Unlock()
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
func (btc *BlipTesterClient) GetMessage(serialNumber blip.MessageNumber) (msg *blip.Message, found bool) {
	btc.messagesLock.RLock()
	defer btc.messagesLock.RUnlock()

	if msg, ok := btc.messages[serialNumber]; ok {
		return msg, ok
	}

	return nil, false
}

// WaitForMessage blocks until the given message serial number has been stored by the client, and returns the message when found.
func (btc *BlipTesterClient) WaitForMessage(serialNumber blip.MessageNumber) (msg *blip.Message, found bool) {
	ticker := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-ticker.C:
			if msg, ok := btc.GetMessage(serialNumber); ok {
				return msg, ok
			}
		}
	}
}

func (btc *BlipTesterClient) storeMessage(msg *blip.Message) {
	btc.messagesLock.Lock()
	defer btc.messagesLock.Unlock()
	btc.messages[msg.SerialNumber()] = msg
}
