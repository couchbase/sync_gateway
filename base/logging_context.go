package base

import "fmt"

// LogContextKey is used to key a LogContext value
type LogContextKey struct{}

// LogContext stores values which may be useful to include in logs
type LogContext struct {
	// CorrelationID is a pre-formatted identifier used to correlate logs.
	// E.g: Either blip context ID or HTTP Serial number.
	CorrelationID string
}

// addContext returns a string format with additional log context if present.
func (lc *LogContext) addContext(format string) string {
	if lc == nil {
		return ""
	}

	if lc.CorrelationID != "" {
		format = "c:" + lc.CorrelationID + " " + format
	}

	return format
}

func FormatBlipContextID(contextID string) string {
	return "[" + contextID + "]"
}

func FormatChangeCacheContextID(contextName string) string{
	return fmt.Sprintf("%s-ChangeCache", contextName)
}