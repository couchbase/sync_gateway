package base

import (
	"errors"
	"fmt"
	"strings"
)

// Redactor provides an interface for log redaction.
type Redactor interface {
	// Redact returns the given string in a redacted form. This may be tagged,
	// changed, hashed, or removed completely depending on desired behaviour.
	Redact() string
}

// redact performs an *in-place* redaction on the input slice, and returns it.
// This should only be consumed by logging funcs. E.g. fmt.Printf(fmt, redact(args))
func redact(args []interface{}) []interface{} {
	for i, v := range args {
		if r, ok := v.(Redactor); ok {
			args[i] = r.Redact()
		}
	}
	return args
}

type RedactionLevel int

const (
	REDACT_NONE RedactionLevel = iota
	REDACT_PARTIAL
	REDACT_FULL
)

func SetRedaction(redactionLevel RedactionLevel) {
	Logf("Log redaction level: %s", redactionLevel)
	switch redactionLevel {
	case REDACT_FULL:
		RedactUserData = true
	case REDACT_PARTIAL:
		RedactUserData = true
	case REDACT_NONE:
		RedactUserData = false
	default:
		RedactUserData = false
	}
}

// String returns a lower-case ASCII representation of the log redaction level.
func (l RedactionLevel) String() string {
	switch l {
	case REDACT_NONE:
		return "none"
	case REDACT_PARTIAL:
		return "partial"
	case REDACT_FULL:
		return "full"
	default:
		return fmt.Sprintf("RedactionLevel(%d)", l)
	}
}

// MarshalText marshals the RedactionLevel to text.
func (l *RedactionLevel) MarshalText() ([]byte, error) {
	if l == nil {
		return nil, errors.New("can't marshal a nil *RedactionLevel to text")
	}
	return []byte(l.String()), nil
}

// UnmarshalText unmarshals text to a RedactionLevel.
func (l *RedactionLevel) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	case "none":
		*l = REDACT_NONE
	case "partial":
		*l = REDACT_PARTIAL
	case "full":
		*l = REDACT_FULL
	default:
		return fmt.Errorf("unrecognized level: %v", string(text))
	}
	return nil
}
