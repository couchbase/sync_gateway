// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"
	"strconv"
	"strings"
)

type CBMobileSubprotocolVersion int

const (
	// CBMobileReplicationV2 the original subprotocol used by CBLite 2.x
	CBMobileReplicationV2 CBMobileSubprotocolVersion = iota + 2
	// CBMobileReplicationV3 minor changes to support revocation and ISGR
	CBMobileReplicationV3
	// Version Vectors/HLV
	CBMobileReplicationV4

	// _nextCBMobileSubprotocolVersions reserved for maxCBMobileSubprotocolVersion
	_nextCBMobileSubprotocolVersions

	// minCBMobileSubprotocolVersion is the minimum supported subprotocol version by SG
	minCBMobileSubprotocolVersion = CBMobileReplicationV2
	// maxCBMobileSubprotocolVersion is the maximum supported subprotocol version by SG
	maxCBMobileSubprotocolVersion = _nextCBMobileSubprotocolVersions - 1
)

const cbMobileBLIPSubprotocolPrefix = "CBMobile_"

// Format must match the AppProtocolId provided by the peer (CBLite / ISGR)
func (v CBMobileSubprotocolVersion) SubprotocolString() string {
	return cbMobileBLIPSubprotocolPrefix + strconv.Itoa(int(v))
}

// ParseSubprotocolString takes a 'CBMobile_' prefixed string and returns the subprotocol version.
func ParseSubprotocolString(s string) (CBMobileSubprotocolVersion, error) {
	vStr, ok := strings.CutPrefix(s, cbMobileBLIPSubprotocolPrefix)
	if !ok {
		return 0, fmt.Errorf("invalid subprotocol string: %q", s)
	}
	v, err := strconv.Atoi(vStr)
	if err != nil {
		return 0, fmt.Errorf("invalid subprotocol string: %q: %w", s, err)
	}
	if v < int(minCBMobileSubprotocolVersion) || v > int(maxCBMobileSubprotocolVersion) {
		return 0, fmt.Errorf("invalid subprotocol version: %q", s)
	}
	return CBMobileSubprotocolVersion(v), nil
}

// supportedSubprotocols returns a list of supported subprotocol versions, in order of most preferred first
func supportedSubprotocols() []string {
	numSubprotocols := maxCBMobileSubprotocolVersion - minCBMobileSubprotocolVersion + 1
	subProtocols := make([]string, 0, numSubprotocols)
	// iterate backwards so we prefer the latest protocol versions
	for i := maxCBMobileSubprotocolVersion; i >= minCBMobileSubprotocolVersion; i-- {
		subProtocols = append(subProtocols, i.SubprotocolString())
	}
	return subProtocols
}
