package channels

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/sdegutis/go.assert"
)

func TestIsValidChannel(t *testing.T) {
	valid := []string{"*", "a", "FOO", "123", "-z", "foo_bar", "Éclær", "z7_"}
	for _, ch := range valid {
		if !IsValidChannel(ch) {
			t.Errorf("IsValidChannel(%q) should be true", ch)
		}
	}
	invalid := []string{"", "**", "a*", "a ", "b?", ",", "Z∫•"}
	for _, ch := range invalid {
		if IsValidChannel(ch) {
			t.Errorf("IsValidChannel(%q) should be false", ch)
		}
	}
}

func TestSetFromArray(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{"*"}, {}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"a", "b"}},
	}
	for _, cas := range cases {
		channels, err := SetFromArray(cas[0], RemoveStar)
		assertNoError(t, err, "SetFromArray failed")
		assert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSetFromArrayWithStar(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{"*"}, {"*"}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"*"}},
	}
	for _, cas := range cases {
		channels, err := SetFromArray(cas[0], ExpandStar)
		assertNoError(t, err, "SetFromArray failed")
		assert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSetFromArrayError(t *testing.T) {
	_, err := SetFromArray([]string{""}, RemoveStar)
	assertTrue(t, err != nil, "SetFromArray didn't return an error")
	_, err = SetFromArray([]string{"chan1", "chan2", "bogus name", "chan3"}, RemoveStar)
	assertTrue(t, err != nil, "SetFromArray didn't return an error")
}

func TestSet(t *testing.T) {
	set, err := SetFromArray(nil, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 0)
	assert.DeepEquals(t, set.ToArray(), []string{})

	set, err = SetFromArray([]string{}, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 0)

	set, err = SetFromArray([]string{"foo"}, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 1)
	assert.True(t, set.Contains("foo"))
	assert.False(t, set.Contains("bar"))

	values := []string{"bar", "foo", "zog"}
	set, err = SetFromArray(values, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 3)
	asArray := set.ToArray()
	sort.Strings(asArray)
	assert.DeepEquals(t, asArray, values)

	set2 := set.copy()
	assert.DeepEquals(t, set2, set)
}

func TestSetUnmarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	err := json.Unmarshal([]byte(`{"channels":["foo"]}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels.ToArray(), []string{"foo"})
}
