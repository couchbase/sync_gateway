package base

import (
	"strings"
	"sync/atomic"
)

// LogKey is a bitfield of log keys.
type LogKey uint32

// Values for log keys.
const (
	// KeyNone is shorthand for no log keys.
	KeyNone LogKey = 0

	// KeyAll is a wildcard for all log keys.
	KeyAll LogKey = 1 << iota

	KeyAccess
	KeyAttach
	KeyAuth
	KeyBucket
	KeyCache
	KeyChanges
	KeyChannelIndex
	KeyChannelStorage
	KeyCRUD
	KeyDCP
	KeyDIndex
	KeyEvents
	KeyFeed
	KeyGoCB
	KeyHeartbeat
	KeyHTTP
	KeyImport
	KeyIndex
	KeyMigrate
	KeyOIDC
	KeyQuery
	KeyReplicate
	KeySequences
	KeyShadow
	KeySync
	KeySyncMsg
	KeyWebSocket
	KeyWebSocketMsg
)

var (
	logKeyNames = map[LogKey]string{
		KeyNone:           "",
		KeyAll:            "*",
		KeyAccess:         "Access",
		KeyAttach:         "Attach",
		KeyAuth:           "Auth",
		KeyBucket:         "Bucket",
		KeyCache:          "Cache",
		KeyChanges:        "Changes",
		KeyChannelIndex:   "ChannelIndex",
		KeyChannelStorage: "ChannelStorage",
		KeyCRUD:           "CRUD",
		KeyDCP:            "DCP",
		KeyDIndex:         "DIndex",
		KeyEvents:         "Events",
		KeyFeed:           "Feed",
		KeyGoCB:           "gocb",
		KeyHeartbeat:      "Heartbeat",
		KeyHTTP:           "HTTP",
		KeyImport:         "Import",
		KeyIndex:          "Index",
		KeyMigrate:        "Migrate",
		KeyOIDC:           "OIDC",
		KeyQuery:          "Query",
		KeyReplicate:      "Replicate",
		KeySequences:      "Sequences",
		KeyShadow:         "Shadow",
		KeySync:           "Sync",
		KeySyncMsg:        "SyncMsg",
		KeyWebSocket:      "WS",
		KeyWebSocketMsg:   "WS+", // backwards compatibility for WS++ logkey
	}

	// Inverse of the map above. Optimisation for string -> LogKey lookups in ToLogKey
	logKeyNamesInverse = inverselogKeyNames(logKeyNames)
)

// Enable will enable the given logKey in keyMask.
func (keyMask *LogKey) Enable(logKey LogKey) {
	val := atomic.LoadUint32((*uint32)(keyMask))
	atomic.StoreUint32((*uint32)(keyMask), val|uint32(logKey))
}

// Disable will disable the given logKey in keyMask.
func (keyMask *LogKey) Disable(logKey LogKey) {
	val := atomic.LoadUint32((*uint32)(keyMask))
	atomic.StoreUint32((*uint32)(keyMask), val & ^uint32(logKey))
}

// Enabled returns true if the given logKey is enabled in keyMask.
// Always returns true if KeyAll is enabled in keyMask.
func (keyMask *LogKey) Enabled(logKey LogKey) bool {
	return keyMask.enabled(logKey, true)
}

// Enabled returns true if the given logKey is enabled in keyMask.
func (keyMask *LogKey) EnabledExcludingWildcard(logKey LogKey) bool {
	return keyMask.enabled(logKey, false)
}

// enabled returns true if the given logKey is enabled in keyMask, with an optional wildcard check.
func (keyMask *LogKey) enabled(logKey LogKey, checkWildcards bool) bool {
	if keyMask == nil {
		return false
	}

	flag := atomic.LoadUint32((*uint32)(keyMask))

	// If KeyAll is set, return true for everything.
	if checkWildcards && flag&uint32(KeyAll) != 0 {
		return true
	}

	return flag&uint32(logKey) != 0
}

// LogKeyName returns the string representation of a single log key.
func LogKeyName(logKey LogKey) string {
	// No lock required to read concurrently, as long as nobody writes to logKeyNames.
	return logKeyNames[logKey]
}

// EnabledLogKeys returns a slice of enabled log key names.
func (keyMask *LogKey) EnabledLogKeys() []string {
	if keyMask == nil {
		return []string{}
	}
	var logKeys = make([]string, 0, len(logKeyNames))
	for i := 0; i < len(logKeyNames); i++ {
		logKey := LogKey(1) << uint32(i)
		if keyMask.enabled(logKey, false) {
			logKeys = append(logKeys, LogKeyName(logKey))
		}
	}
	return logKeys
}

// ToLogKey takes a slice of case-sensitive log key names and will return a LogKey bitfield.
func ToLogKey(keysStr []string) LogKey {
	var logKeys LogKey
	for _, name := range keysStr {
		// Ignore "+" in log keys (for backwards compatibility)
		name := strings.Replace(name, "+", "", -1)

		if logKey, ok := logKeyNamesInverse[name]; ok {
			logKeys.Enable(logKey)
		} else {
			Warnf(KeyAll, "Invalid log key: %v", name)
		}
	}
	return logKeys
}

func inverselogKeyNames(in map[LogKey]string) map[string]LogKey {
	var out = make(map[string]LogKey, len(in))
	for k, v := range in {
		out[v] = k
	}
	return out
}
