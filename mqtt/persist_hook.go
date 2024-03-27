//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"bytes"
	"errors"

	"github.com/couchbase/sync_gateway/base"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Persistent storage hook that writes to a bucket. Implements `mqtt.Hook`
type persistHook struct {
	mochi.HookBase

	persist *persister
}

// Creates a new persistHook.
func newPersistHook(persist *persister) (*persistHook, error) {
	hook := &persistHook{persist: persist}
	if err := persist.initQueries(); err != nil {
		return nil, err
	}
	return hook, nil
}

func (h *persistHook) ID() string {
	return "Bucket"
}

var kPersistHookProvides = []byte{
	mochi.OnSessionEstablished,
	mochi.OnDisconnect,
	mochi.OnClientExpired,
	mochi.StoredClientByID,

	mochi.OnSubscribed,
	mochi.OnUnsubscribed,

	mochi.OnPublished,
	mochi.OnRetainMessage,
	mochi.OnRetainedExpired,

	mochi.OnQosPublish,
	mochi.OnQosComplete,
	mochi.OnQosDropped,
}

func (h *persistHook) Provides(b byte) bool {
	return bytes.Contains(kPersistHookProvides, []byte{b})
}

//======== CLIENTS:

func isPersistentClient(cl *mochi.Client) bool {
	return cl.Properties.Props.SessionExpiryInterval > 0 && !cl.Net.Inline
}

// `OnSessionEstablished` adds a client to the store when their session is established.
func (h *persistHook) OnSessionEstablished(cl *mochi.Client, pk packets.Packet) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) {
		h.Log.Info("OnSessionEstablished", "id", cl.ID)
		if err := h.persist.updateClient(cl); err != nil {
			h.Log.Error("failed to update client data", "error", err, "session", cl.ID)
		}
	} else {
		h.Log.Info("OnSessionEstablished -- non-persistent", "id", cl.ID)
	}
}

// `OnDisconnect` handles a client disconnection.
func (h *persistHook) OnDisconnect(cl *mochi.Client, _ error, expire bool) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) {
		if !expire || errors.Is(cl.StopCause(), packets.ErrSessionTakenOver) {
			// Bump the session doc's "updated" field and expiration time:
			h.Log.Info("OnDisconnect -- bump updated", "id", cl.ID)
			if err := h.persist.disconnectClient(cl); err != nil {
				h.Log.Error("failed to update disconnected client", "error", err, "session", cl.ID)
			}
		} else {
			// ...or if the session's expired, just delete the doc now:
			h.Log.Info("OnDisconnect -- expired", "id", cl.ID)
			h.persist.deleteClient(cl)
		}
	} else {
		h.Log.Info("OnDisconnect -- non-persistent", "id", cl.ID)
	}
}

// `OnClientExpired` deletes an expired client from the store.
func (h *persistHook) OnClientExpired(cl *mochi.Client) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) {
		h.Log.Info("OnClientExpired", "id", cl.ID)
		h.persist.deleteClient(cl)
	}
}

// `StoredClientByID` looks up a persistent client by ID and username,
// returning its saved subscriptions and any inflight messages to deliver.
func (h *persistHook) StoredClientByID(id string, cl *mochi.Client) (oldRemote string, subs []storage.Subscription, msgs []storage.Message, err error) {
	defer base.FatalPanicHandler()
	oldRemote, subs, msgs, err = h.persist.getStoredClient(id, cl.Properties.Username)
	if err == nil && oldRemote != "" {
		h.Log.Info("StoredClientByID -- got client data", "id", id, "subs", len(subs), "msgs", len(msgs))
	}
	return
}

//======== SUBSCRIPTIONS:

// `OnSubscribed` adds one or more client subscriptions to the store.
func (h *persistHook) OnSubscribed(cl *mochi.Client, pk packets.Packet, reasonCodes []byte) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) {
		h.Log.Info("OnSubscribed", "id", cl.ID, "num_topics", len(pk.Filters))
		if err := h.persist.updateClientSubscriptions(cl); err != nil {
			h.Log.Error("failed to update client subscriptions", "error", err, "session", cl.ID)
		}
	}
}

// `OnUnsubscribed` removes one or more client subscriptions from the store.
func (h *persistHook) OnUnsubscribed(cl *mochi.Client, pk packets.Packet) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) && !cl.Closed() {
		h.Log.Info("OnUnsubscribed", "id", cl.ID, "num_topics", len(pk.Filters))
		if err := h.persist.updateClientSubscriptions(cl); err != nil {
			h.Log.Error("failed to update client subscriptions", "error", err, "session", cl.ID)
		}
	}
}

//======== PUBLISHED MESSAGES:

// `OnPublished` notifies that a message has been published to a topic.
func (h *persistHook) OnPublished(cl *mochi.Client, pk packets.Packet) {
	defer base.FatalPanicHandler()
	if pk.FixedHeader.Qos >= 1 {
		h.persist.persistPublishedMessage(cl, pk)
	}
}

// `OnRetainMessage` adds a retained message for a topic to the store.
func (h *persistHook) OnRetainMessage(cl *mochi.Client, pk packets.Packet, result int64) {
	defer base.FatalPanicHandler()
	h.Log.Info("OnRetainMessage", "client", cl.ID, "topic", pk.TopicName, "result", result)
	if result > 0 {
		if err := h.persist.persistRetainedMessage(pk); err != nil {
			h.Log.Error("failed to save retained message data", "error", err)
		}
	} else if result < 0 {
		h.persist.deleteRetainedMessage(pk)
	}
}

//======== IN-FLIGHT (ACK) MESSAGES:

// `OnQosPublish` adds or updates an inflight message for a client.
func (h *persistHook) OnQosPublish(cl *mochi.Client, pk packets.Packet, sent int64, resends int) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) && pk.FixedHeader.Type != packets.Publish {
		h.Log.Info("OnQosPublish", "client", cl.ID, "type", packets.PacketNames[pk.FixedHeader.Type])
		if err := h.persist.updateClientInflights(cl); err != nil {
			h.Log.Error("failed to update client inflight messages", "error", err, "session", cl.ID)
		}
	}
}

// `OnQosComplete` removes a completed message (only called for acks, apparently)
func (h *persistHook) OnQosComplete(cl *mochi.Client, pk packets.Packet) {
	defer base.FatalPanicHandler()
	if isPersistentClient(cl) && pk.FixedHeader.Type != packets.Publish {
		h.Log.Info("OnQosComplete", "client", cl.ID, "type", packets.PacketNames[pk.FixedHeader.Type])
		if err := h.persist.updateClientInflights(cl); err != nil {
			h.Log.Error("failed to update client inflight messages", "error", err, "session", cl.ID)
		}
	}
}

// `OnQosDropped` removes a dropped inflight message from the store.
func (h *persistHook) OnQosDropped(cl *mochi.Client, pk packets.Packet) {
	h.OnQosComplete(cl, pk)
}
