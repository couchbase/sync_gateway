//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertValidNodeUUID asserts that id is a 32-character lowercase hex string
// (the format produced by GenerateNodeUUID and GenerateRandomID).
func assertValidNodeUUID(t *testing.T, id string) {
	t.Helper()
	require.Lenf(t, id, nodeUUIDLength, "expected %d-char node UUID, got %q", nodeUUIDLength, id)
	for _, c := range id {
		require.Truef(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'), "invalid char %q in node UUID %q", c, id)
	}
}

func TestGenerateNodeUUID(t *testing.T) {
	ctx := TestCtx(t)

	id1, err := GenerateNodeUUID(ctx, "127.0.0.1:4984", "127.0.0.1:4985")
	require.NoError(t, err)
	assertValidNodeUUID(t, id1)

	id2, err := GenerateNodeUUID(ctx, "127.0.0.1:4984", "127.0.0.1:4985")
	require.NoError(t, err)
	assert.Equal(t, id1, id2, "expected deterministic UUID across calls on the same host with same listen addresses")

	id3, err := GenerateNodeUUID(ctx, "127.0.0.1:5984", "127.0.0.1:5985")
	require.NoError(t, err)
	assert.NotEqual(t, id1, id3, "different listen addresses on the same host should produce different UUIDs")
}

func TestDeterministicNodeUUID(t *testing.T) {
	const hostA = "alpha"
	const hostB = "beta"
	macs1 := []string{"02:42:ac:11:00:02", "02:42:ac:11:00:03"}
	macs2 := []string{"02:42:ac:11:00:04"}
	addrs1 := []string{"0.0.0.0:4984", "0.0.0.0:4985"}
	addrs2 := []string{"0.0.0.0:5984", "0.0.0.0:5985"}

	t.Run("same inputs produce same output", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, macs1, addrs1)
		b := deterministicNodeUUID(hostA, macs1, addrs1)
		assert.Equal(t, a, b)
		assertValidNodeUUID(t, a)
	})

	t.Run("different hostnames with identical MACs differ", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, macs1, addrs1)
		b := deterministicNodeUUID(hostB, macs1, addrs1)
		assert.NotEqual(t, a, b, "hostname should affect the UUID")
	})

	t.Run("identical hostnames with different MACs differ", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, macs1, addrs1)
		b := deterministicNodeUUID(hostA, macs2, addrs1)
		assert.NotEqual(t, a, b, "MAC set should disambiguate colliding hostnames")
	})

	t.Run("MAC ordering does not affect output", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, []string{"aa:aa:aa:aa:aa:aa", "bb:bb:bb:bb:bb:bb"}, addrs1)
		b := deterministicNodeUUID(hostA, []string{"bb:bb:bb:bb:bb:bb", "aa:aa:aa:aa:aa:aa"}, addrs1)
		assert.Equal(t, a, b)
	})

	t.Run("listen address ordering does not affect output", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, macs1, []string{"0.0.0.0:4984", "0.0.0.0:4985"})
		b := deterministicNodeUUID(hostA, macs1, []string{"0.0.0.0:4985", "0.0.0.0:4984"})
		assert.Equal(t, a, b)
	})

	t.Run("identical hostnames and MACs but different listen addresses differ", func(t *testing.T) {
		a := deterministicNodeUUID(hostA, macs1, addrs1)
		b := deterministicNodeUUID(hostA, macs1, addrs2)
		assert.NotEqual(t, a, b, "listen addresses should disambiguate two SG processes on the same host")
	})

	t.Run("empty MAC list and listen addresses with hostname still produces valid UUID", func(t *testing.T) {
		id := deterministicNodeUUID(hostA, nil, nil)
		assertValidNodeUUID(t, id)
	})
}
