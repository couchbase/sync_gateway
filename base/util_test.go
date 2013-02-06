// util_test.go

package base

import (
	"github.com/sdegutis/go.assert"
	"testing"
)

func TestFixJSONNumbers(t *testing.T) {
	assert.DeepEquals(t, FixJSONNumbers(1), 1)
	assert.DeepEquals(t, FixJSONNumbers(float64(1.23)), float64(1.23))
	assert.DeepEquals(t, FixJSONNumbers(float64(123456)), int64(123456))
	assert.DeepEquals(t, FixJSONNumbers(float64(123456789)), int64(123456789))
	assert.DeepEquals(t, FixJSONNumbers(float64(12345678901234567890)),
		float64(12345678901234567890))
	assert.DeepEquals(t, FixJSONNumbers("foo"), "foo")
	assert.DeepEquals(t, FixJSONNumbers([]interface{}{1, float64(123456)}),
		[]interface{}{1, int64(123456)})
	assert.DeepEquals(t, FixJSONNumbers(map[string]interface{}{"foo": float64(123456)}),
		map[string]interface{}{"foo": int64(123456)})
}
