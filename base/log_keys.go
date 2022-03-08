/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
)

// LogKeyMask is a bitfield of log keys.
type LogKeyMask uint64

// LogKeyMask is slice index for log keys.  Entries in LogKeySet are stored at 2^LogKeyMask
type LogKey uint8

// Values for log keys.
const (
	// KeyNone is shorthand for no log keys.
	KeyNone LogKey = iota

	// KeyAll is a wildcard for all log keys.
	KeyAll

	KeyAdmin
	KeyAccess
	KeyAuth
	KeyBucket
	KeyCache
	KeyChanges
	KeyCluster
	KeyConfig
	KeyCRUD
	KeyDCP
	KeyEvents
	KeyGoCB
	KeyHTTP
	KeyHTTPResp
	KeyImport
	KeyJavascript
	KeyMigrate
	KeyQuery
	KeyReplicate
	KeySync
	KeySyncMsg
	KeyWebSocket
	KeyWebSocketFrame
	KeySGTest

	LogKeyCount // Count for logKeyNames init
)

var (
	logKeyNames = [LogKeyCount]string{
		KeyNone:           "",
		KeyAll:            "*",
		KeyAdmin:          "Admin",
		KeyAccess:         "Access",
		KeyAuth:           "Auth",
		KeyBucket:         "Bucket",
		KeyCache:          "Cache",
		KeyChanges:        "Changes",
		KeyCluster:        "SGCluster",
		KeyConfig:         "Config",
		KeyCRUD:           "CRUD",
		KeyDCP:            "DCP",
		KeyEvents:         "Events",
		KeyGoCB:           "gocb",
		KeyHTTP:           "HTTP",
		KeyHTTPResp:       "HTTP+", // Infof printed as HTTP+
		KeyImport:         "Import",
		KeyJavascript:     "Javascript",
		KeyMigrate:        "Migrate",
		KeyQuery:          "Query",
		KeyReplicate:      "Replicate",
		KeySync:           "Sync",
		KeySyncMsg:        "SyncMsg",
		KeyWebSocket:      "WS",
		KeyWebSocketFrame: "WSFrame",
		KeySGTest:         "TEST",
	}
	logKeyNamesInverse = inverselogKeyNames(logKeyNames)
)

// KeyMaskValue converts a log key index to the bitfield position.
// e.g. 0->0, 1->1, 2->2, 3->4, 4->8, 5->16...
func (i LogKey) KeyMaskValue() uint64 {
	if i == 0 {
		return 0
	}
	return 1 << (i - 1)
}

// Enable will enable the given logKey in keyMask.
func (keyMask *LogKeyMask) Enable(logKey LogKey) {
	val := atomic.LoadUint64((*uint64)(keyMask))
	atomic.StoreUint64((*uint64)(keyMask), val|logKey.KeyMaskValue())
}

// Disable will disable the given logKey in keyMask.
func (keyMask *LogKeyMask) Disable(logKey LogKey) {
	val := atomic.LoadUint64((*uint64)(keyMask))
	atomic.StoreUint64((*uint64)(keyMask), val & ^logKey.KeyMaskValue())
}

// Set will override the keyMask with the given logKeyMask.
func (keyMask *LogKeyMask) Set(logKeyMask *LogKeyMask) {
	atomic.StoreUint64((*uint64)(keyMask), uint64(*logKeyMask))
}

// Enabled returns true if the given logKey is enabled in keyMask.
// Always returns true if KeyAll is enabled in keyMask.
func (keyMask *LogKeyMask) Enabled(logKey LogKey) bool {
	return keyMask.enabled(logKey, true)
}

// Enabled returns true if the given logKey is enabled in keyMask.
func (keyMask *LogKeyMask) EnabledExcludingWildcard(logKey LogKey) bool {
	return keyMask.enabled(logKey, false)
}

// enabled returns true if the given logKey is enabled in keyMask, with an optional wildcard check.
func (keyMask *LogKeyMask) enabled(logKey LogKey, checkWildcards bool) bool {
	if keyMask == nil {
		return false
	}

	flag := atomic.LoadUint64((*uint64)(keyMask))

	// If KeyAll is set, return true for everything.
	if checkWildcards && flag&KeyAll.KeyMaskValue() != 0 {
		return true
	}

	return flag&logKey.KeyMaskValue() != 0
}

// String returns the string representation of one or more log keys in a LogKeyMask
func (logKeyMask LogKeyMask) String() string {

	// Try to pretty-print a set of log keys.
	names := []string{}
	for keyIndex, keyName := range logKeyNames {
		if logKeyMask.Enabled(LogKey(keyIndex)) {
			names = append(names, keyName)
		}
	}
	if len(names) > 0 {
		return strings.Join(names, ", ")
	}

	// Fall back to a binary representation.
	return fmt.Sprintf("LogKeyMask(%b)", logKeyMask)
}

// String returns the string representation of one log key.
func (logKey LogKey) String() string {
	// No lock required to read concurrently, as long as nobody writes to logKeyNames.
	if int(logKey) < len(logKeyNames) {
		return logKeyNames[logKey]
	}

	// Fall back to a binary representation.
	return fmt.Sprintf("LogKeyMask(%b)", logKey)
}

// EnabledLogKeys returns a slice of enabled log key names.
func (keyMask *LogKeyMask) EnabledLogKeys() []string {
	if keyMask == nil {
		return []string{}
	}
	var logKeys = make([]string, 0, len(logKeyNames))
	for i := 0; i < len(logKeyNames); i++ {
		if keyMask.enabled(LogKey(i), false) {
			logKeys = append(logKeys, logKeyNames[i])
		}
	}
	return logKeys
}

// ToLogKey takes a slice of case-sensitive log key names and will return a LogKeyMask bitfield
// and a slice of deferred log functions for any warnings that may occurr.
func ToLogKey(keysStr []string) (logKeys LogKeyMask) {

	for _, key := range keysStr {
		// Take a copy of key, so we can use it in a closure outside the scope
		// of this loop (the warnings returned are logged asyncronously)
		originalKey := key

		// Some old log keys (like HTTP+), we want to handle slightly (map to a different key)
		if newLogKeys, ok := convertSpecialLogKey(key); ok {
			for _, newLogKey := range newLogKeys {
				logKeys.Enable(newLogKey)
			}
			continue
		}

		// Strip a single "+" suffix in log keys and warn (for backwards compatibility)
		if strings.HasSuffix(key, "+") {
			newLogKey := strings.TrimSuffix(key, "+")
			WarnfCtx(context.Background(), "Deprecated log key: %q found. Changing to: %q.", originalKey, newLogKey)
			key = newLogKey
		}

		if logKey, ok := logKeyNamesInverse[key]; ok {
			logKeys.Enable(logKey)
		} else {
			WarnfCtx(context.Background(), "Invalid log key: %v", originalKey)
		}
	}

	return logKeys
}

func inverselogKeyNames(in [LogKeyCount]string) map[string]LogKey {
	var out = make(map[string]LogKey, len(in))
	for i, v := range in {
		if v == "" && i != int(KeyNone) {
			// TODO: CBG-1948
			panic(fmt.Sprintf("Empty value for logKeyNames[%d]", i))
		}
		out[v] = LogKey(i)
	}
	return out
}

// convertSpecialLogKey handles the conversion of some legacy log keys we want to map to one or more different value.
func convertSpecialLogKey(oldLogKey string) ([]LogKey, bool) {

	var logKeys []LogKey
	switch oldLogKey {
	case "HTTP+":
		// HTTP+ Should enable both KeyHTTP and KeyHTTPResp
		logKeys = []LogKey{KeyHTTP, KeyHTTPResp}
	}

	return logKeys, logKeys != nil
}

// logKeyPtr is a convenience function that returns a LogKeyMask for the given logKeys
func logKeyMask(logKeys ...LogKey) *LogKeyMask {
	var mask LogKeyMask
	for _, logKey := range logKeys {
		mask.Enable(logKey)
	}
	return &mask
}
