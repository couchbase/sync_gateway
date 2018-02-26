package log

import (
	"errors"
	"fmt"
	"strings"
)

type RedactionLevel int

const (
	RedactNone RedactionLevel = iota
	RedactPartial
	RedactFull
)

// String returns a lower-case ASCII representation of the log redaction level.
func (l RedactionLevel) String() string {
	switch l {
	case RedactNone:
		return "none"
	case RedactPartial:
		return "partial"
	case RedactFull:
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
		*l = RedactNone
	case "partial":
		*l = RedactPartial
	case "full":
		*l = RedactFull
	default:
		return fmt.Errorf("unrecognized level: %v", string(text))
	}
	return nil
}
