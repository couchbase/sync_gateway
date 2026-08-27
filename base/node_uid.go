//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"net"
	"os"
	"sort"
)

// nodeUIDLength is the expected length of a valid node UID (128-bit value encoded as a 32-character lowercase hex string)
const nodeUIDLength = 32

// GenerateNodeUID derives a stable 32-character hex node UID from a fingerprint of the SG's process and its host.
// Components: hostname + NIC MAC addresses + API listen addresses.
// The same SG instance on the same hardware produces the same UID across restarts without any persistent state.
//
// Only if no fingerprint inputs can be collected do we fall back to a random ID.
func GenerateNodeUID(ctx context.Context, listenAddrs ...string) (string, error) {
	hostname, macs := nodeFingerprint(ctx)
	if hostname == "" && len(macs) == 0 && len(listenAddrs) == 0 {
		WarnfCtx(ctx, "Could not collect hostname, interface MACs, or listen addresses for deterministic node UID — falling back to random node UID")
		return GenerateRandomID()
	}

	return deterministicNodeUID(hostname, macs, listenAddrs), nil
}

// nodeFingerprint reads the hostname and non-empty hardware addresses of all network interfaces.
// Either component may be empty if the system call fails; both empty means we cannot fingerprint the host.
func nodeFingerprint(ctx context.Context) (hostname string, macs []string) {
	hostname, err := os.Hostname()
	if err != nil {
		WarnfCtx(ctx, "Could not read hostname for node UID fingerprint: %v", err)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		WarnfCtx(ctx, "Could not enumerate network interfaces for node UID fingerprint: %v", err)
		return hostname, nil
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if mac := iface.HardwareAddr.String(); mac != "" {
			macs = append(macs, mac)
		}
	}

	return hostname, macs
}

// deterministicNodeUID produces a 32-char lowercase hex digest for the given:
//  1. hostname
//  2. All network interface MACs
//  3. SG REST API listen addresses
//
// This ensures that we can calculate a unique identifier for a given Sync Gateway process even under these conditions:
//  1. Different hostnames but same listen addresses (common case)
//  2. Same host shared across multiple listen addresses (uncommon but often test setups ... '2-node' on ports 4984 and 5984 for example)
//  3. Same hostname different physical hardware (uncommon but default/unconfigured hostname setups may cause this)
func deterministicNodeUID(hostname string, macs []string, listenAddrs []string) string {
	sortedMACs := append([]string(nil), macs...)
	sort.Strings(sortedMACs)
	sortedAddrs := append([]string(nil), listenAddrs...)
	sort.Strings(sortedAddrs)

	h := sha256.New()
	writeLengthPrefixed(h, hostname)
	for _, mac := range sortedMACs {
		writeLengthPrefixed(h, mac)
	}
	for _, addr := range sortedAddrs {
		writeLengthPrefixed(h, addr)
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:nodeUIDLength/2])
}

// writeLengthPrefixed writes a uint32 little-endian length followed by the bytes of s,
// so that adjacent components in the hash input cannot run together regardless of their
// contents.
func writeLengthPrefixed(h hash.Hash, s string) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(s))
}
