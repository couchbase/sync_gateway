//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMyAddress(t *testing.T) {
	addr, err := getIPAddress(false)
	require.NoError(t, err)
	require.NotEmpty(t, addr)
	log.Printf("IPv4 = %s", addr)

	addr, err = getIPAddress(true)
	require.NoError(t, err)
	require.NotEmpty(t, addr)
	log.Printf("IPv6 = %s", addr)
}

func TestGetRealIPAddress(t *testing.T) {

	getReal := func(addr string) (host string, port string) {
		addr, err := makeRealIPAddress(addr, true)
		require.NoError(t, err)
		if host, port, err = net.SplitHostPort(addr); err == nil {
			return host, port
		} else {
			return addr, ""
		}
	}

	// Unspecified addresses will be replaced with real ones:

	host, port := getReal(":12345")
	assert.NotEqual(t, "", host)
	assert.Equal(t, "12345", port)

	host, port = getReal("0.0.0.0")
	log.Printf("IPv4 = %s", host)
	assert.NotEqual(t, "0.0.0.0", host)
	assert.Empty(t, port)

	host, port = getReal("0.0.0.0:12345")
	assert.NotEqual(t, "0.0.0.0", host)
	assert.Equal(t, "12345", port)

	// Real addresses won't change:

	host, port = getReal("17.18.19.20")
	assert.Equal(t, "17.18.19.20", host)
	assert.Empty(t, port)

	host, port = getReal("17.18.19.20:12345")
	assert.Equal(t, "17.18.19.20", host)
	assert.Equal(t, "12345", port)

	// IPv6, unspecified addresses:

	host, port = getReal("::")
	log.Printf("IPv4 = %s", host)
	assert.NotEqual(t, "::", host)
	assert.Empty(t, port)

	host, port = getReal("[::]:12345")
	log.Printf("IPv4 = %s", host)
	assert.NotEqual(t, "::", host)
	assert.Equal(t, "12345", port)

	// IPv6, real addresses:

	host, port = getReal("2001:db8::1")
	assert.Equal(t, "2001:db8::1", host)
	assert.Empty(t, port)

	host, port = getReal("[2001:db8::1]:12345")
	assert.Equal(t, "2001:db8::1", host)
	assert.Equal(t, "12345", port)
}
