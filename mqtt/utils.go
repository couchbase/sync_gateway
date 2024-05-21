//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"time"
)

//======== IP ADDRESSES

// Returns a real IPv4 or IPv6 address for this computer.
// If none can be found, returns an empty string; an error is only returned if it was unable to
// get the list of network interfaces.
func getIPAddress(ipv6 bool) (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, i := range interfaces {
		// The interface must be up and running, but not loopback nor point-to-point:
		if (i.Flags&net.FlagUp != 0) && (i.Flags&net.FlagRunning != 0) && (i.Flags&net.FlagLoopback == 0) && (i.Flags&net.FlagPointToPoint == 0) {
			if addrs, err := i.Addrs(); err == nil {
				for _, addr := range addrs {
					if ip, _, err := net.ParseCIDR(addr.String()); err == nil {
						// The address must not be loopback, multicast, nor link-local:
						if !ip.IsLoopback() && !ip.IsLinkLocalMulticast() && !ip.IsLinkLocalUnicast() && !ip.IsMulticast() && !ip.IsUnspecified() {
							// If it matches the IP version, return it:
							if (ip.To4() == nil) == ipv6 {
								return ip.String(), nil
							}
						}
					}
				}
			}
		}
	}
	return "", nil
}

// Takes an address or address-and-port string, checks whether it contains an "unspecified"
// IP address ("0.0.0.0" or "::"), and if so replaces it with a real IPv4 or IPv6 address.
// If `keepPort` is false, any port number will be removed.
func makeRealIPAddress(addrStr string, keepPort bool) (string, error) {
	host, port, err := net.SplitHostPort(addrStr)
	if err != nil {
		host = addrStr
	}

	var addr net.IP
	if host != "" {
		if addr = net.ParseIP(host); addr == nil {
			return "", fmt.Errorf("%q is not a valid IP address", addrStr)
		}
	}

	if addr == nil || addr.IsUnspecified() {
		// The address is "0.0.0.0" or "::"; find a real address of the same type:
		ipv6 := addr != nil && (addr.To4() == nil)
		if host, err = getIPAddress(ipv6); err != nil {
			return "", err
		} else if keepPort && port != "" {
			host = net.JoinHostPort(host, port)
		}
		return host, nil
	} else if keepPort {
		return addrStr, nil
	} else {
		return host, nil
	}
}

//======== TIMESTAMPS

// simply coerces int64, float64 or json.Number to float64.
func decodeTimestamp(n any) (float64, bool) {
	switch n := n.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		if i, err := n.Float64(); err == nil {
			return i, true
		}
	}
	return 0.0, false
}

// Converts a float64 timestamp to a Time value.
func timeFromTimestamp(ts float64) time.Time {
	secs := math.Floor(ts)
	return time.Unix(int64(secs), int64((ts-secs)*1.0e9))
}
