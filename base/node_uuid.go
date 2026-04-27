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
	"encoding/hex"
	"net"
	"os"
	"sort"
	"strings"
)

// nodeUUIDLength is the expected length of a valid node UUID
// (128-bit value encoded as a 32-character lowercase hex string).
const nodeUUIDLength = 32

// GenerateNodeUUID derives a stable 32-character hex node UUID from a fingerprint of the
// current host (hostname + sorted non-loopback interface MAC addresses) combined with the
// supplied listen addresses. Including listen addresses (typically the public and admin
// API interfaces) lets two Sync Gateway processes running on the same host produce
// distinct UUIDs. The same instance produces the same UUID across restarts without any
// persistent state. Only if no fingerprint inputs can be collected do we fall back to a
// random UUID.
func GenerateNodeUUID(ctx context.Context, listenAddrs ...string) (string, error) {
	hostname, macs := collectNodeFingerprint(ctx)
	if hostname == "" && len(macs) == 0 && len(listenAddrs) == 0 {
		WarnfCtx(ctx, "Could not collect hostname, interface MACs, or listen addresses for deterministic node UUID — falling back to random")
		return GenerateRandomID()
	}
	return deterministicNodeUUID(hostname, macs, listenAddrs), nil
}

// collectNodeFingerprint reads the hostname and non-empty hardware addresses of all
// network interfaces (loopback and interfaces without a MAC are skipped). Either
// component may be empty if the system call fails; both empty means we cannot
// fingerprint the host.
func collectNodeFingerprint(ctx context.Context) (hostname string, macs []string) {
	if h, err := os.Hostname(); err == nil {
		hostname = h
	} else {
		WarnfCtx(ctx, "Could not read hostname for node UUID fingerprint: %v", err)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		WarnfCtx(ctx, "Could not enumerate network interfaces for node UUID fingerprint: %v", err)
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

// deterministicNodeUUID produces a 32-char lowercase hex digest over the canonical
// colon-separated fingerprint: hostname, then MACs sorted lexicographically, then listen
// addresses sorted lexicographically. The colon separator is safe because hostnames
// cannot contain ':' on Linux, macOS, or Windows; deterministic ordering of MACs and
// listen addresses keeps the input bytes stable across restarts. Pure function —
// exposed unexported for tests.
func deterministicNodeUUID(hostname string, macs []string, listenAddrs []string) string {
	sortedMACs := append([]string(nil), macs...)
	sort.Strings(sortedMACs)
	sortedAddrs := append([]string(nil), listenAddrs...)
	sort.Strings(sortedAddrs)

	parts := append([]string{hostname}, sortedMACs...)
	parts = append(parts, sortedAddrs...)
	sum := sha256.Sum256([]byte(strings.Join(parts, ":")))
	return hex.EncodeToString(sum[:nodeUUIDLength/2])
}
