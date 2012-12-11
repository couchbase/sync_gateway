package channelsync


import (
	"testing"
	"github.com/sdegutis/go.assert"
)

// Just verify that the calls to the sync() fn show up in the output channel list.
func TestSyncFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync("foo", "bar"); sync("baz")}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.callMapper(`{"channels": []}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo", "bar", "baz"})
}

// Just verify that the calls to the sync() fn show up in the output channel list.
func TestSyncFunctionTakesArray(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(["foo", "bar","baz"])}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.callMapper(`{"channels": []}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo", "bar", "baz"})
}

// Now just make sure the input comes through intact
func TestInputParse(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channel);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.callMapper(`{"channel": "foo"}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo"})
}

// A more realistic example
func TestChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.callMapper(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo", "bar", "baz"})
	
	channels, err = mapper.callMapper(`{"x": "y"}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{})
}

// Test the public API
func TestPublicChannelMapper(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.MapToChannels(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"foo", "bar", "baz"})
	mapper.Stop()
}

// Test changing the function
func TestSetFunction(t *testing.T) {
	mapper, err := NewChannelMapper(`function(doc) {sync(doc.channels);}`)
	assertNoError(t, err, "Couldn't create mapper")
	channels, err := mapper.MapToChannels(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	changed, err := mapper.SetFunction(`function(doc) {sync("all");}`)
	assertTrue(t, changed, "SetFunction failed")
	assertNoError(t, err, "SetFunction failed")
	channels, err = mapper.MapToChannels(`{"channels": ["foo", "bar", "baz"]}`)
	assertNoError(t, err, "callMapper failed")
	assert.DeepEquals(t, channels, []string{"all"})
	mapper.Stop()
}
