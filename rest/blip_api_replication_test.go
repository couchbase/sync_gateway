// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlipClientPushAndPullReplication sets up a bidi replication for a BlipTesterClient, writes documents on SG and the client and ensures they replicate.
func TestBlipClientPushAndPullReplication(t *testing.T) {
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{}},
		GuestEnabled:   true,
	}
	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t,
			&rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer client.Close()

		btcRunner.StartPull(client.id)
		btcRunner.StartPush(client.id)

		// create doc1 on SG
		version := rt.PutDoc(docID, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)

		// wait for doc on client
		data := btcRunner.WaitForVersion(client.id, docID, version)
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

		// update doc1 on client
		newRev := btcRunner.AddRev(client.id, docID, &version, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))

		// wait for update to arrive on SG
		rt.WaitForVersion(docID, newRev)

		body := rt.GetDocVersion("doc1", newRev)
		require.Equal(t, "bob", body["greetings"].([]interface{})[2].(map[string]interface{})["howdy"])
	})
}
