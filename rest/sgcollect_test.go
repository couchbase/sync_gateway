package rest

import (
	"regexp"
	"strings"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestSgcollectFilename(t *testing.T) {
	filename := sgcollectFilename()

	// Check the product name has been set
	assert.False(t, strings.Contains(filename, "ProductName"))

	// Check it doesn't have forbidden chars
	assert.False(t, strings.ContainsAny(filename, "\\/:*?\"<>|"))

	matched, err := regexp.Match(`^sgcollectinfo\-\d{4}\-\d{2}\-\d{2}t\d{6}\-sga?@(\d{1,3}\.){4}zip$`, []byte(filename))
	assertNoError(t, err, "unexpected regexp error")
	assert.True(t, matched)
}
