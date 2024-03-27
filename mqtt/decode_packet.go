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
	"fmt"

	"github.com/mochi-mqtt/server/v2/packets"
)

// Decodes a binary MQTT packet to a `Packet` struct.
// NOTE: So far this only handles 'Publish' packets.
func decodePacket(data []byte, protocolVersion byte) (pk packets.Packet, err error) {
	// This is adapted from the source code of Client.ReadPacket() in mochi-server/clients.go
	in := bytes.NewBuffer(data)
	b, err := in.ReadByte()
	if err != nil {
		return
	}

	err = pk.FixedHeader.Decode(b)
	if err != nil {
		return
	}

	pk.FixedHeader.Remaining, _, err = packets.DecodeLength(in)
	if err != nil {
		return
	}

	pk.ProtocolVersion = protocolVersion
	if pk.FixedHeader.Remaining != len(in.Bytes()) {
		err = fmt.Errorf("fixedHeader.remaining disagrees with data size")
		return
	}

	switch pk.FixedHeader.Type {
	case packets.Publish:
		err = pk.PublishDecode(in.Bytes())
	default:
		err = fmt.Errorf("decodePacket doesn't handle packet type %v", pk.FixedHeader.Type)
	}
	return
}
