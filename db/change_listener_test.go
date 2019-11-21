package db

import (
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserWaiter(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyCache)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Create user
	username := "bob"
	authenticator := db.Authenticator()
	require.NotNil(t, authenticator, "db.Authenticator() returned nil")
	user, err := authenticator.NewUser(username, "letmein", channels.SetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")

	// Create the user waiter (note: user hasn't been saved yet)
	log.Printf("Saved user")
	userDb := &Database{
		user:            user,
		DatabaseContext: db.DatabaseContext,
	}
	userWaiter := userDb.NewUserWaiter()
	assert.False(t, userWaiter.RefreshUserCount())

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant new channel
	updatedUser := PrincipalConfig{
		Name:     &username,
		Channels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, err = db.UpdatePrincipal(updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notification from grant
	require.True(t, WaitForUserWaiterChange(userWaiter))

}

func TestUserWaiterForRoleChange(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyCache)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Create role
	roleName := "good_egg"
	authenticator := db.Authenticator()
	require.NotNil(t, authenticator, "db.Authenticator() returned nil")
	role, err := authenticator.NewRole(roleName, channels.SetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new role")
	authenticator.Save(role)

	// Create user
	username := "bob"
	require.NotNil(t, authenticator, "db.Authenticator() returned nil")
	user, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err, "Error creating new user")

	// Create the user waiter (note: user hasn't been saved yet)
	userDb := &Database{
		user:            user,
		DatabaseContext: db.DatabaseContext,
	}
	userWaiter := userDb.NewUserWaiter()
	isChanged := userWaiter.RefreshUserCount()
	assert.False(t, isChanged)

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant role
	updatedUser := PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: []string{roleName},
	}
	_, err = db.UpdatePrincipal(updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notify from updated user
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Retrieve the user.  This will trigger a user update to move ExplicitRoles->roles
	userRefresh, err := authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")

	// Wait for notify from retrieval
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the waiter with the current user (adds role to waiter.UserKeys)
	userWaiter.RefreshUserKeys(userRefresh)

	// Update the role to grant a new channel
	updatedRole := PrincipalConfig{
		Name:     &roleName,
		Channels: base.SetFromArray([]string{"ABC"}),
	}
	_, err = db.UpdatePrincipal(updatedRole, false, true)
	require.NoError(t, err, "Error updating role")

	// Wait for user notification of updated role
	require.True(t, WaitForUserWaiterChange(userWaiter))
}

func BenchmarkNotify(b *testing.B) {

	notifyBenchmarks := []struct {
		name       string
		numWaiters int
	}{
		{
			"Notify_10",
			10,
		},
		{
			"Notify_100",
			100,
		},
		{
			"Notify_1000",
			1000,
		},
		{
			"Notify_10000",
			10000,
		},
		{
			"Notify_35000",
			35000,
		},
	}

	for _, bm := range notifyBenchmarks {

		listener := &changeListener{}
		listener.Init("mybucket")

		b.Run(bm.name, func(b *testing.B) {
			b.StopTimer()
			terminator := make(chan struct{})
			for i := 0; i < bm.numWaiters; i++ {
				waiter := listener.NewWaiter([]string{fmt.Sprintf("user/user%d", i)})
				go func() {
					for {
						select {
						case <-terminator:
							return
						default:
						}
						waiter.Wait()
					}
				}()
			}
			b.StartTimer()
			var keys base.Set
			for i := 0; i < b.N; i++ {
				userIndex := rand.Intn(bm.numWaiters)
				keys = base.SetFromArray([]string{fmt.Sprintf("user/user%d", userIndex)})
				listener.Notify(keys)
			}
			close(terminator)
			listener.Stop()
		})
	}
}

func BenchmarkNotifyWithUsers(b *testing.B) {

	notifyBenchmarks := []struct {
		name       string
		numWaiters int
		numUsers   int
	}{
		{
			"Notify_100t_10000u",
			100,
			10000,
		},
		{
			"Notify_100t_35000u",
			100,
			35000,
		},
		{
			"Notify_800t_100u",
			800,
			100,
		},
		{
			"Notify_800t_1000u",
			800,
			1000,
		},
		{
			"Notify_800t_10000u",
			800,
			10000,
		},
		{
			"Notify_800t_35000u",
			800,
			35000,
		},
		{
			"Notify_800t_100000u",
			800,
			100000,
		},
		{
			"Notify_2000t_10000u",
			2000,
			10000,
		},
	}

	for _, bm := range notifyBenchmarks {

		listener := &changeListener{}
		listener.Init("mybucket")

		b.Run(bm.name, func(b *testing.B) {
			b.StopTimer()
			terminator := make(chan struct{})
			for i := 0; i < bm.numWaiters; i++ {
				go func() {
					notifyCount := 0
					for {
						select {
						case <-terminator:
							return
						default:
						}
						// Start a new waiter for a random user
						key := fmt.Sprintf("user/user%d", rand.Intn(bm.numUsers))
						waiter := listener.NewWaiter([]string{key})
						waiter.Wait()
						notifyCount++
					}
				}()
			}
			b.StartTimer()
			var keys base.Set
			for i := 0; i < b.N; i++ {
				userIndex := rand.Intn(bm.numUsers)
				keys = base.SetFromArray([]string{fmt.Sprintf("user/user%d", userIndex)})
				listener.Notify(keys)
			}
			close(terminator)
			listener.Stop()
		})
	}
}

func BenchmarkNotifyWithUsersBatched(b *testing.B) {

	notifyBenchmarks := []struct {
		name       string
		numWaiters int
		numUsers   int
	}{
		{
			"Notify_100t_10000u",
			100,
			10000,
		},
		{
			"Notify_100t_35000u",
			100,
			35000,
		},
		{
			"Notify_800t_100u",
			800,
			100,
		},
		{
			"Notify_800t_1000u",
			800,
			1000,
		},
		{
			"Notify_800t_10000u",
			800,
			10000,
		},
		{
			"Notify_800t_35000u",
			800,
			35000,
		},
		{
			"Notify_800t_100000u",
			800,
			100000,
		},
		{
			"Notify_2000t_10000u",
			2000,
			10000,
		},
	}

	for _, bm := range notifyBenchmarks {

		listener := &changeListener{}
		listener.Init("mybucket")

		b.Run(bm.name, func(b *testing.B) {
			b.StopTimer()
			terminator := make(chan struct{})
			for i := 0; i < bm.numWaiters; i++ {
				go func() {
					for {
						select {
						case <-terminator:
							return
						default:
						}
						// Start a new waiter for a random user
						key := fmt.Sprintf("user/user%d", rand.Intn(bm.numUsers))
						waiter := listener.NewWaiter([]string{key})
						waiter.Wait()
					}
				}()
			}
			b.StartTimer()
			var keys base.Set
			var batchedKeys base.Set
			for i := 0; i < b.N; i++ {
				userIndex := rand.Intn(bm.numUsers)
				keys = base.SetFromArray([]string{fmt.Sprintf("user/user%d", userIndex)})
				batchedKeys = batchedKeys.Union(keys)
				if len(batchedKeys) > 100 {
					listener.Notify(batchedKeys)
					batchedKeys.Clear()
				}
			}
			close(terminator)
			listener.Stop()
		})
	}
}
