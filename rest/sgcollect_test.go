package rest

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestSgcollectFilename(t *testing.T) {
	filename := sgcollectFilename()

	// Check it doesn't have forbidden chars
	assert.False(t, strings.ContainsAny(filename, "\\/:*?\"<>|"))

	pattern := `^sgcollectinfo\-\d{4}\-\d{2}\-\d{2}t\d{6}\-sga?@(\d{1,3}\.){4}zip$`
	matched, err := regexp.Match(pattern, []byte(filename))
	assertNoError(t, err, "unexpected regexp error")
	assertTrue(t, matched, fmt.Sprintf("Filename: %s did not match pattern: %s", filename, pattern))
}
