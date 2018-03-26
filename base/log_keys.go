package base

import (
	"sync/atomic"
)

// LogKey is a bitfield of log keys.
type LogKey struct {
	flag uint32
}

// Values for log keys.
const (
	// KEY_NONE is shorthand for no log keys.
	KEY_NONE uint32 = 0

	// KEY_ALL is a wildcard for all log keys.
	KEY_ALL uint32 = 1 << iota

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

var logKeyNames = map[uint32]string{
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

// Enable will enable the given logKey in keyMask.
func (keyMask *LogKey) Enable(logKey uint32) {
	val := atomic.LoadUint32(&keyMask.flag)
	atomic.StoreUint32(&keyMask.flag, val|logKey)
}

// Disable will disable the given logKey in keyMask.
func (keyMask *LogKey) Disable(logKey uint32) {
	val := atomic.LoadUint32(&keyMask.flag)
	atomic.StoreUint32(&keyMask.flag, val & ^logKey)
}

// Enabled returns true if the given logKey, or KEY_ALL is enabled in keyMask.
func (keyMask *LogKey) Enabled(logKey uint32) bool {
	return keyMask.enabled(logKey, true)
}

// enabled returns true if the given logKey is enabled in keyMask, with an optional wildcard check.
func (keyMask *LogKey) enabled(logKey uint32, checkWildcard bool) bool {
	flag := atomic.LoadUint32(&keyMask.flag)
	return (checkWildcard && flag&KEY_ALL != 0) ||
		flag&logKey != 0
}

// LogKeyName returns the string representation of a single log key.
func LogKeyName(logKey uint32) string {
	// No lock required to read concurrently, as long as nobody writes to logKeyNames.
	return logKeyNames[logKey]
}

// ToLogKey takes a slice of case-sensitive log key names and will return a LogKey bitfield.
func ToLogKey(keysStr []string) LogKey {
	var logKeys = LogKey{}
	for _, name := range keysStr {
		for logKey, logKeyName := range logKeyNames {
			if logKeyName == name {
				logKeys.Enable(logKey)
			}
		}
	}
	return logKeys
}

// EnabledLogKeys returns a slice of enabled log key names.
func (keyMask LogKey) EnabledLogKeys() []string {
	var logKeys = make([]string, 0, len(logKeyNames))
	for i := 0; i < len(logKeyNames); i++ {
		logKey := uint32(1) << uint32(i)
		if keyMask.enabled(logKey, false) {
			logKeys = append(logKeys, LogKeyName(logKey))
		}
	}
	return logKeys
}
