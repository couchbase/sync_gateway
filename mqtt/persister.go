package mqtt

import (
	"context"
	"fmt"
	"slices"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Manages persistent MQTT state, storing it in a DataStore. Used by `persistHook`.
type persister struct {
	ctx           context.Context    // Go context
	metadataStore sgbucket.DataStore // Bucket that stores broker metadata
	config        *ServerConfig      // Configuration
}

//======== CLIENTS:

// Struct used to marshal/unmarshal client session data to a bucket.
type clientDocument struct {
	Username      []byte                 `json:"user"`               // Username
	Remote        string                 `json:"remote"`             // Last known IP address
	Clean         bool                   `json:"clean,omitempty"`    // If true, don't reuse session
	Connection    clientConnection       `json:"conn"`               // Connection state
	Subscriptions []storage.Subscription `json:"subs,omitempty"`     // Subscriptions
	Inflights     []storage.Message      `json:"inflight,omitempty"` // Inflight ack messages
}

type clientConnection struct {
	Online    bool  `json:"online,omitempty"`  // Is client still online?
	UpdatedAt int64 `json:"updated,omitempty"` // Time record was last saved
}

const kClientDocumentConnectionKey = "conn"    // JSON property of clientDocument.Connection
const kClientDocumentSubscriptionsKey = "subs" // JSON property of clientDocument.Subscriptions
const kClientDocumentInflightsKey = "inflight" // JSON property of clientDocument.Inflights

const kClientDocRefreshInterval = 5 * 60 // How often to touch exp of client session docs

// Saves a client's persistent session data to the store.
func (p *persister) updateClient(cl *mochi.Client) error {
	timestamp := cl.StopTime()
	online := (timestamp <= 0)
	if online {
		timestamp = time.Now().Unix()
	}

	doc := clientDocument{
		Username: cl.Properties.Username,
		Remote:   cl.Net.Remote,
		Clean:    cl.Properties.Clean && cl.Properties.ProtocolVersion < 5,
		Connection: clientConnection{
			Online:    online,
			UpdatedAt: timestamp,
		},
		Subscriptions: p.clientSubs(cl),
		Inflights:     p.clientInflights(cl),
	}

	return p.metadataStore.Set(clientKey(cl), p.clientExpiry(cl, online), nil, doc)
}

// Updates just the "subs" property of a client doc.
func (p *persister) updateClientSubscriptions(cl *mochi.Client) error {
	if !isPersistentClient(cl) {
		return nil
	}
	var jsonData []byte
	if subs := p.clientSubs(cl); subs != nil {
		jsonData, _ = base.JSONMarshal(subs)
	} else {
		jsonData = []byte("[]")
	}
	_, err := p.metadataStore.WriteSubDoc(p.ctx, clientKey(cl), kClientDocumentSubscriptionsKey, 0, jsonData)
	return err
}

// Updates just the "inflight" property of a client doc.
func (p *persister) updateClientInflights(cl *mochi.Client) error {
	var jsonData []byte
	inflights := p.clientInflights(cl)
	if inflights != nil {
		jsonData, _ = base.JSONMarshal(inflights)
	} else {
		jsonData = []byte("[]")
	}
	_, err := p.metadataStore.WriteSubDoc(p.ctx, clientKey(cl), kClientDocumentInflightsKey, 0, jsonData)
	return err
}

// Marks a client session as disconnected.
func (p *persister) disconnectClient(cl *mochi.Client) error {
	key := clientKey(cl)
	connData, _ := base.JSONMarshal(clientConnection{Online: false, UpdatedAt: cl.StopTime()})
	_, err := p.metadataStore.WriteSubDoc(p.ctx, key, kClientDocumentConnectionKey, 0, connData)
	if err == nil {
		_, err = p.metadataStore.Touch(key, p.clientExpiry(cl, false))
	}
	return err
}

// Deletes a client's session data.
func (p *persister) deleteClient(cl *mochi.Client) {
	p.deleteDoc(clientKey(cl), "expired client data")
}

// Finds a stored session by ID & username, returning the relevant data.
func (p *persister) getStoredClient(id string, username []byte) (oldRemote string, subs []storage.Subscription, msgs []storage.Message, err error) {
	var doc clientDocument
	_, err = p.metadataStore.Get(clientIDKey(id), &doc)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return
	} else if !slices.Equal(doc.Username, username) {
		return // Username mismatch [MQTT-5.4.2]
	} else if doc.Clean {
		return // [MQTT-3.1.2-4] [MQTT-3.1.4-4]
	}

	oldRemote = doc.Remote
	subs = doc.Subscriptions
	msgs = doc.Inflights

	if len(subs) > 0 && doc.Connection.UpdatedAt > 0 {
		// Find published messages created since the client went offline.
		// First, construct a TopicMap for the client's subscribed topic filters:
		filters := NewTopicMap[bool](len(subs))
		for _, sub := range subs {
			if sub.Qos > 0 {
				filters.AddFilter(sub.Filter, true)
			}
		}

		// Query for all stored messages saved since the client went offline:
		params := map[string]any{"start_key": doc.Connection.UpdatedAt}
		result, err := p.query(kViewMessagesByTime, params)
		if err != nil {
			base.ErrorfCtx(p.ctx, "Unexpected error querying %s view: %v", kViewMessagesByTime, err)
			return "", nil, nil, err
		}
		for _, row := range result.Rows {
			// Does the message's topic match any of the subs?
			if topic, ok := row.Value.(string); ok {
				if filters.Get(topic) {
					var msg storage.Message
					if _, err := p.metadataStore.Get(row.ID, &msg); err == nil {
						base.DebugfCtx(p.ctx, base.KeyMQTT, "StoredClientByID: adding message %s", msg)
						msgs = append(msgs, msg)
					}
				}
			}
		}
	}
	return
}

// Returns the client's subscriptions in storeable form.
func (p *persister) clientSubs(cl *mochi.Client) (subs []storage.Subscription) {
	if allSubs := cl.State.Subscriptions.GetAll(); len(allSubs) > 0 {
		subs = make([]storage.Subscription, 0, len(allSubs))
		for _, pk := range allSubs {
			subs = append(subs, storage.Subscription{
				Filter:            pk.Filter,
				Identifier:        pk.Identifier,
				RetainHandling:    pk.RetainHandling,
				Qos:               pk.Qos,
				RetainAsPublished: pk.RetainAsPublished,
				NoLocal:           pk.NoLocal,
			})
		}
	}
	return subs
}

// Returns a client's pending inflight (ack) packets in storeable form.
func (p *persister) clientInflights(cl *mochi.Client) (inflights []storage.Message) {
	if cl.State.Inflight.Len() > 0 {
		for _, pk := range cl.State.Inflight.GetAll(false) {
			if pk.FixedHeader.Type != packets.Publish {
				inflights = append(inflights, p.storageMessage(&pk))
			}
		}
	}
	return
}

// Computes expiration time of session document:
func (p *persister) clientExpiry(cl *mochi.Client, online bool) uint32 {
	exp := base.Min(cl.Properties.Props.SessionExpiryInterval, p.config.MaximumSessionExpiryInterval)
	if online {
		exp = base.Max(exp, 2*kClientDocRefreshInterval)
	}
	return base.SecondsToCbsExpiry(int(exp))
}

//======== PUBLISHED MESSAGES:

// Saves a published message with QoS > 0.
func (p *persister) persistPublishedMessage(cl *mochi.Client, pk packets.Packet) error {
	in := p.storageMessage(&pk)
	return p.metadataStore.Set(queuedMessageKey(cl, &pk), p.messageExpiry(&pk), nil, in)
}

// Updates the 'retained' message for a topic.
func (p *persister) persistRetainedMessage(pk packets.Packet) error {
	in := p.storageMessage(&pk)
	return p.metadataStore.Set(retainedMessageKey(pk.TopicName), p.messageExpiry(&pk), nil, in)
}

// Deletes the 'retained' message of a topic.
func (p *persister) deleteRetainedMessage(pk packets.Packet) {
	p.deleteDoc(retainedMessageKey(pk.TopicName), "retained message data")
}

//======== QUERIES:

const kDesignDoc = "MQTT" // Name of the design doc, for map/reduce indexing
const kViewMessagesByTime = "msgs"

// `initQueries` sets up indexing on the DataStore, for the below queries.
func (p *persister) initQueries() error {
	viewStore, ok := p.metadataStore.(sgbucket.ViewStore)
	if !ok {
		return fmt.Errorf("DataStore does not support views")
	}
	// Get the design doc:
	ddoc, err := viewStore.GetDDoc(kDesignDoc)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			return err
		}
		err = nil
	}
	if ddoc.Views == nil {
		ddoc.Views = sgbucket.ViewMap{}
	}

	// Add views:
	save := false
	if _, ok := ddoc.Views[kViewMessagesByTime]; !ok {
		const kMsgsFn = `function(doc,meta) {
			 if (meta.id.slice(0,15) == "_sync:mqtt:QED:")
			 	 emit(doc.created, doc.topic_name);
		}`
		ddoc.Views[kViewMessagesByTime] = sgbucket.ViewDef{Map: kMsgsFn}
		save = true
	}

	// Save any changes:
	if save {
		err = viewStore.PutDDoc(p.ctx, kDesignDoc, &ddoc)
	}
	return err
}

// Queries a view.
func (p *persister) query(viewName string, params map[string]any) (sgbucket.ViewResult, error) {
	return p.metadataStore.(sgbucket.ViewStore).View(p.ctx, kDesignDoc, viewName, params)
}

//======== UTILITIES:

func (p *persister) deleteDoc(key string, what string) error {
	err := p.metadataStore.Delete(key)
	if err != nil && !base.IsDocNotFoundError(err) {
		base.ErrorfCtx(p.ctx, "MQTT: failed to delete %s %q: %v", what, key, err)
	}
	return err
}

// Creates a storage.Message from a Packet.
func (p *persister) storageMessage(pk *packets.Packet) storage.Message {
	return storage.Message{
		Payload:     pk.Payload,
		Origin:      pk.Origin,
		TopicName:   pk.TopicName,
		FixedHeader: pk.FixedHeader,
		PacketID:    pk.PacketID,
		Created:     pk.Created,
		Properties: storage.MessageProperties{
			CorrelationData:        pk.Properties.CorrelationData,
			SubscriptionIdentifier: pk.Properties.SubscriptionIdentifier,
			User:                   pk.Properties.User,
			ContentType:            pk.Properties.ContentType,
			ResponseTopic:          pk.Properties.ResponseTopic,
			MessageExpiryInterval:  pk.Properties.MessageExpiryInterval,
			TopicAlias:             pk.Properties.TopicAlias,
			PayloadFormat:          pk.Properties.PayloadFormat,
			PayloadFormatFlag:      pk.Properties.PayloadFormatFlag,
		},
	}
}

// Computes the expiry for a message doc.
func (p *persister) messageExpiry(pk *packets.Packet) uint32 {
	exp := pk.Properties.MessageExpiryInterval
	if exp == 0 {
		exp = uint32(p.config.MaximumMessageExpiryInterval)
	}
	if sessexp := p.config.MaximumSessionExpiryInterval; sessexp < exp {
		// No point keeping a message past the expiration of any session that wants it
		exp = sessexp
	}
	return base.SecondsToCbsExpiry(int(exp))
}

//======== DATABASE KEYS:

const keyPrefix = "_sync:mqtt:" // Prefix of all db keys saved by the persistHook.

// clientKey returns a primary key for a client session.
func clientKey(cl *mochi.Client) string {
	return clientIDKey(cl.ID)
}

// clientIDKey returns a primary key for a client session, given the client ID
func clientIDKey(id string) string {
	return keyPrefix + "CL:" + id
}

// retainedMessageKey returns a primary key for a retained message in a topic.
func retainedMessageKey(topic string) string {
	return keyPrefix + "RET:" + topic
}

// queuedMessageKey returns a primary key for a queued message.
func queuedMessageKey(cl *mochi.Client, pk *packets.Packet) string {
	return fmt.Sprintf("%sQED:%08x:%s", keyPrefix, pk.Created, cl.ID)
}
