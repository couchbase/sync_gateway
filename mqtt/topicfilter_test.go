//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicFilterWithoutWildcards(t *testing.T) {
	r, err := MakeTopicFilter("fo.oo/bar")
	assert.NoError(t, err)
	assert.True(t, r.Matches("fo.oo/bar"))
	assert.False(t, r.Matches("foxoo/bar"))
	assert.False(t, r.Matches("fo.oo"))
	assert.False(t, r.Matches("xxfo.oo/barxx"))
	assert.Equal(t, []string{}, r.FindStringSubmatch("fo.oo/bar"))
	assert.Equal(t, []string(nil), r.FindStringSubmatch("fooo/ba"))
}

func TestTopicFilterWithPlus(t *testing.T) {
	r, err := MakeTopicFilter("+/bar")
	assert.NoError(t, err)
	assert.True(t, r.Matches("something/bar"))
	assert.False(t, r.Matches("bar"))
	assert.False(t, r.Matches("/bar"))
	assert.False(t, r.Matches("/something/bar"))
	assert.False(t, r.Matches("/something/else/bar"))
	assert.Equal(t, []string{"something"}, r.FindStringSubmatch("something/bar"))

	r, err = MakeTopicFilter("foo/+")
	assert.NoError(t, err)
	assert.True(t, r.Matches("foo/bar"))
	assert.False(t, r.Matches("foo"))
	assert.False(t, r.Matches("foo/bar/baz"))
	assert.Equal(t, []string{"bar"}, r.FindStringSubmatch("foo/bar"))

	r, err = MakeTopicFilter("fo.oo/+/bar")
	assert.NoError(t, err)
	assert.True(t, r.Matches("fo.oo/something/bar"))
	assert.False(t, r.Matches("fo.oo/bar"))
	assert.False(t, r.Matches("fo.oo//bar"))
	assert.False(t, r.Matches("fo.oo/something/else/bar"))
	assert.Equal(t, []string{"something"}, r.FindStringSubmatch("fo.oo/something/bar"))
}

func TestTopicFilterWithOctothorpe(t *testing.T) {
	r, err := MakeTopicFilter("fo.oo/bar/#")
	assert.NoError(t, err)
	assert.True(t, r.Matches("fo.oo/bar"))
	assert.True(t, r.Matches("fo.oo/bar/baz/yow"))
	assert.False(t, r.Matches("fo.oo/barbecue"))
	assert.Equal(t, []string{"baz/yow"}, r.FindStringSubmatch("fo.oo/bar/baz/yow"))
}

func TestTopicFilterWithMultipleWildcards(t *testing.T) {
	r, err := MakeTopicFilter("+/foo/+/bar/#")
	assert.NoError(t, err)
	assert.True(t, r.Matches("a/foo/b/bar"))
	assert.True(t, r.Matches("a/foo/b/bar/c/d"))
	assert.False(t, r.Matches("a/foo/bar/b"))
	assert.Equal(t, []string{"a", "b", "c/d"}, r.FindStringSubmatch("a/foo/b/bar/c/d"))
	assert.Equal(t, []string(nil), r.FindStringSubmatch("foo/b/bar/c/d"))
}

func TestTopicFilterAllTopics(t *testing.T) {
	r, err := MakeTopicFilter("#")
	assert.NoError(t, err)
	assert.True(t, r.Matches("a/foo/b/bar"))
	assert.True(t, r.Matches("z"))
	assert.True(t, r.Matches("b/c"))
	assert.Equal(t, []string{"a/foo/b/bar/c/d"}, r.FindStringSubmatch("a/foo/b/bar/c/d"))
}

func TestTopicFilterBadPattern(t *testing.T) {
	_, err := MakeTopicFilter("foo/pl+us/bar")
	assert.Error(t, err)
	_, err = MakeTopicFilter("foo/++/bar")
	assert.Error(t, err)
	_, err = MakeTopicFilter("foo/#/bar")
	assert.Error(t, err)
	_, err = MakeTopicFilter("foo/bar#")
	assert.Error(t, err)
	_, err = MakeTopicFilter("foo/bar/#/")
	assert.Error(t, err)
}

func TestTopicMap(t *testing.T) {
	input := map[string]int{
		"foo/bar":       1,
		"temp/#":        2,
		"user/+/weight": 3,
	}
	topics, err := MakeTopicMap(input)
	assert.NoError(t, err)

	assert.Equal(t, topics.Get("foo/bar"), 1)
	assert.Equal(t, topics.Get("foo/barrrr"), 0)
	assert.Equal(t, topics.Get("temp"), 2)
	assert.Equal(t, topics.Get("temp/outdoor/garden"), 2)
	assert.Equal(t, topics.Get("user/jens/weight"), 3)
	assert.Equal(t, topics.Get("user/jens/weight/kg"), 0)
}
