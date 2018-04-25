package base

import (
	"fmt"
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
	KeyHTTPResp
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
	KeyWebSocketFrame
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
		KeyHTTPResp:       "HTTP+", // Infof printed as HTTP+
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
		KeyWebSocketFrame: "WS+", // Debugf printed as WS++
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

// Set will override the keyMask with the given logKey.
func (keyMask *LogKey) Set(logKey LogKey) {
	atomic.StoreUint32((*uint32)(keyMask), uint32(logKey))
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

// String returns the string representation of a single log key.
func (logKey LogKey) String() string {
	// No lock required to read concurrently, as long as nobody writes to logKeyNames.
	if str, ok := logKeyNames[logKey]; ok {
		return str
	}
	return fmt.Sprintf("LogKey(%b)", logKey)
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
			logKeys = append(logKeys, logKey.String())
		}
	}
	return logKeys
}

// ToLogKey takes a slice of case-sensitive log key names and will return a LogKey bitfield.
func ToLogKey(keysStr []string) LogKey {
	var logKeys LogKey
	for _, name := range keysStr {

		// Some old log keys (like HTTP+, and WS++), we want to handle slightly differently.
		if newLogKey, ok := convertSpecialLogKey(name); ok {
			logKeys.Enable(*newLogKey)
			continue
		}

		// Strip a single "+" suffix in log keys and warn (for backwards compatibility)
		if strings.HasSuffix(name, "+") {
			newName := strings.TrimSuffix(name, "+")
			Warnf(KeyAll, "Deprecated log key: %q found. Changing to: %q.", name, newName)
			name = newName
		}

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

// convertSpecialLogKey handles the conversion of some legacy log keys we want to map to a different value.
func convertSpecialLogKey(oldLogKey string) (*LogKey, bool) {
	var logKey *LogKey

	switch oldLogKey {
	case "HTTP+":
		// HTTP+ Should enable both KeyHTTP and KeyHTTPResp
		logKey = logKeyPtr(KeyHTTP | KeyHTTPResp)
	case "WS++":
		// WS++ should enable both KeyWebSocket and KeyWebSocketFrame
		logKey = logKeyPtr(KeyWebSocket | KeyWebSocketFrame)
	}

	return logKey, logKey != nil
}

// logKeyPtr is a convinience funciton that returns a pointer to the given logKey
func logKeyPtr(logKey LogKey) *LogKey {
	return &logKey
}
