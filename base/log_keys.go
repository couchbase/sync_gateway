package base

import (
	"strings"
	"sync/atomic"
)

// LogKey is a bitfield of log keys.
type LogKey uint32

// Values for log keys.
const (
	// KEY_NONE is shorthand for no log keys.
	KEY_NONE LogKey = 1 << iota

	// KEY_ALL is a wildcard for all log keys.
	KEY_ALL

	KEY_ACCESS
	KEY_ATTACH
	KEY_AUTH
	KEY_BUCKET
	KEY_CACHE
	KEY_CHANGES
	KEY_CRUD
	KEY_DCP
	KEY_EVENTS
	KEY_FEED
	KEY_HTTP
	KEY_IMPORT
	KEY_REPLICATE
)

var (
	logKeyNames = map[LogKey]string{
		KEY_NONE:      "",
		KEY_ALL:       "*",
		KEY_ACCESS:    "Access",
		KEY_ATTACH:    "Attach",
		KEY_AUTH:      "Auth",
		KEY_BUCKET:    "Bucket",
		KEY_CACHE:     "Cache",
		KEY_CHANGES:   "Changes",
		KEY_CRUD:      "CRUD",
		KEY_DCP:       "DCP",
		KEY_EVENTS:    "Events",
		KEY_FEED:      "Feed",
		KEY_HTTP:      "HTTP",
		KEY_IMPORT:    "Import",
		KEY_REPLICATE: "Replicate",
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
// Always returns true if KEY_ALL is enabled in keyMask.
// Always returns false if KEY_NONE is enabled in keyMask.
func (keyMask *LogKey) Enabled(logKey LogKey) bool {
	return keyMask.enabled(logKey, true)
}

// enabled returns true if the given logKey is enabled in keyMask, with an optional wildcard check.
func (keyMask *LogKey) enabled(logKey LogKey, checkWildcards bool) bool {
	if keyMask == nil {
		return false
	}

	flag := atomic.LoadUint32((*uint32)(keyMask))

	if checkWildcards {
		// If KEY_NONE is set, return false for everything.
		if flag&uint32(KEY_NONE) != 0 {
			return false
		}
		// If KEY_ALL is set, return true for everything.
		if flag&uint32(KEY_ALL) != 0 {
			return true
		}
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
			Warnf(KEY_ALL, "Invalid log key: %v", name)
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
