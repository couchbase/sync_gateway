package base

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// LogKey is a bitfield of log keys.
type LogKey uint64

// Values for log keys.
const (
	// KeyNone is shorthand for no log keys.
	KeyNone LogKey = 0

	// KeyAll is a wildcard for all log keys.
	KeyAll LogKey = 1 << iota

	KeyAdmin
	KeyAccel
	KeyAccess
	KeyAuth
	KeyBucket
	KeyCache
	KeyChanges
	KeyCRUD
	KeyDCP
	KeyEvents
	KeyGoCB
	KeyHTTP
	KeyHTTPResp
	KeyImport
	KeyMigrate
	KeyQuery
	KeyReplicate
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
		KeyAdmin:          "Admin",
		KeyAccel:          "Accel",
		KeyAccess:         "Access",
		KeyAuth:           "Auth",
		KeyBucket:         "Bucket",
		KeyCache:          "Cache",
		KeyChanges:        "Changes",
		KeyCRUD:           "CRUD",
		KeyDCP:            "DCP",
		KeyEvents:         "Events",
		KeyGoCB:           "gocb",
		KeyHTTP:           "HTTP",
		KeyHTTPResp:       "HTTP+", // Infof printed as HTTP+
		KeyImport:         "Import",
		KeyMigrate:        "Migrate",
		KeyQuery:          "Query",
		KeyReplicate:      "Replicate",
		KeyShadow:         "Shadow",
		KeySync:           "Sync",
		KeySyncMsg:        "SyncMsg",
		KeyWebSocket:      "WS",
		KeyWebSocketFrame: "WSFrame",
	}

	// Inverse of the map above. Optimisation for string -> LogKey lookups in ToLogKey
	logKeyNamesInverse = inverselogKeyNames(logKeyNames)
)

// Enable will enable the given logKey in keyMask.
func (keyMask *LogKey) Enable(logKey LogKey) {
	val := atomic.LoadUint64((*uint64)(keyMask))
	atomic.StoreUint64((*uint64)(keyMask), val|uint64(logKey))
}

// Disable will disable the given logKey in keyMask.
func (keyMask *LogKey) Disable(logKey LogKey) {
	val := atomic.LoadUint64((*uint64)(keyMask))
	atomic.StoreUint64((*uint64)(keyMask), val & ^uint64(logKey))
}

// Set will override the keyMask with the given logKey.
func (keyMask *LogKey) Set(logKey LogKey) {
	atomic.StoreUint64((*uint64)(keyMask), uint64(logKey))
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

	flag := atomic.LoadUint64((*uint64)(keyMask))

	// If KeyAll is set, return true for everything.
	if checkWildcards && flag&uint64(KeyAll) != 0 {
		return true
	}

	return flag&uint64(logKey) != 0
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
		logKey := LogKey(1) << uint64(i)
		if keyMask.enabled(logKey, false) {
			logKeys = append(logKeys, logKey.String())
		}
	}
	return logKeys
}

// ToLogKey takes a slice of case-sensitive log key names and will return a LogKey bitfield
// and a slice of deferred log functions for any warnings that may occurr.
func ToLogKey(keysStr []string) (logKeys LogKey, warnings []DeferredLogFn) {

	for _, key := range keysStr {
		// Copy the original key, so we can still refer to it after modification.
		originalKey := key

		// Some old log keys (like HTTP+), we want to handle slightly (map to a different key)
		if newLogKey, ok := convertSpecialLogKey(key); ok {
			logKeys.Enable(*newLogKey)
			continue
		}

		// Strip a single "+" suffix in log keys and warn (for backwards compatibility)
		if strings.HasSuffix(key, "+") {
			newLogKey := strings.TrimSuffix(key, "+")

			warnings = append(warnings, func() {
				Warnf(KeyAll, "Deprecated log key: %q found. Changing to: %q.", originalKey, newLogKey)
			})

			key = newLogKey
		}

		if logKey, ok := logKeyNamesInverse[key]; ok {
			logKeys.Enable(logKey)
		} else {
			warnings = append(warnings, func() {
				Warnf(KeyAll, "Invalid log key: %v", originalKey)
			})
		}
	}

	return logKeys, warnings
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
	}

	return logKey, logKey != nil
}

// logKeyPtr is a convenience function that returns a pointer to the given logKey
func logKeyPtr(logKey LogKey) *LogKey {
	return &logKey
}
