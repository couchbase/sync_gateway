package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactErrorf(t *testing.T) {
	tests := []struct {
		name,
		expectedString,
		expectedRedact,
		fmt string
		args []interface{}
	}{
		{
			name:           "%s",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %s",
			args:           []interface{}{UD("Bob"), ErrNotFound},
		},
		{
			name:           "%w",
			expectedString: "Couldn't get user \"Bob\": Not Found",
			expectedRedact: "Couldn't get user \"<ud>Bob</ud>\": Not Found",
			fmt:            "Couldn't get user %q: %w",
			args:           []interface{}{UD("Bob"), ErrNotFound},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := RedactErrorf(test.fmt, test.args...)
			assert.Equal(t, test.expectedString, err.String())
			assert.Equal(t, test.expectedRedact, err.Redact())
		})
	}
}
