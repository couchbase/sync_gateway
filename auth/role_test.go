package auth

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
)

func TestInitRole(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	// Check initializing role with legal role name.
	role := &roleImpl{}
	assert.NoError(t, role.initRole("Music", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Equal(t, "Music", role.Name_)
	assert.Equal(t, channels.TimedSet{
		"Spotify": channels.NewVbSimpleSequence(0x1),
		"Youtube": channels.NewVbSimpleSequence(0x1)}, role.ExplicitChannels_)

	// Check initializing role with illegal role name.
	role = &roleImpl{}
	assert.Error(t, role.initRole("Mu$ic", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole("Musi[", channels.SetOf(t, "Spotify", "Youtube")))
	assert.Error(t, role.initRole("Music~", channels.SetOf(t, "Spotify", "Youtube")))
}
