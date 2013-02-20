package channels

import (
	"github.com/sdegutis/go.assert"
	"sort"
	"testing"
)

func TestSet(t *testing.T) {
	set := SetFromArray(nil)
	assert.Equals(t, len(set), 0)
	assert.DeepEquals(t, set.ToArray(), []string{})

	set = SetFromArray([]string{})
	assert.Equals(t, len(set), 0)

	set = SetFromArray([]string{"foo"})
	assert.Equals(t, len(set), 1)
	assert.True(t, set["foo"])
	assert.False(t, set["bar"])

	values := []string{"bar", "foo", "zog"}
	set = SetFromArray(values)
	assert.Equals(t, len(set), 3)
	asArray := set.ToArray()
	sort.Strings(asArray)
	assert.DeepEquals(t, asArray, values)

	set["foo"] = false
	asArray = set.ToArray()
	sort.Strings(asArray)
	assert.DeepEquals(t, asArray, []string{"bar", "zog"})
}
