//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	"github.com/couchbaselabs/go.assert"
	"math/rand"
)

func canSeeAllChannels(princ Principal, channels base.Set) bool {
	for channel := range channels {
		if !princ.CanSeeChannel(channel) {
			return false
		}
	}
	return true
}

func TestValidateGuestUser(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	bucket := gTestBucket.Bucket
	auth := NewAuthenticator(bucket, nil)
	user, err := auth.NewUser("", "", nil)
	assert.True(t, user != nil)
	assert.True(t, err == nil)
}

func TestValidateUser(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.NewUser("invalid:name", "", nil)
	assert.Equals(t, user, (User)(nil))
	assert.True(t, err != nil)
	user, err = auth.NewUser("ValidName", "", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
	user, err = auth.NewUser("ValidName", "letmein", nil)
	assert.True(t, user != nil)
	assert.Equals(t, err, nil)
}

func TestValidateRole(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, err := auth.NewRole("invalid:name", nil)
	assert.Equals(t, role, (User)(nil))
	assert.True(t, err != nil)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
	role, err = auth.NewRole("ValidName", nil)
	assert.True(t, role != nil)
	assert.Equals(t, err, nil)
}

func TestValidateUserEmail(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	badEmails := []string{"", "foo", "foo@", "@bar", "foo @bar", "foo@.bar"}
	for _, e := range badEmails {
		assert.False(t, IsValidEmail(e))
	}
	goodEmails := []string{"foo@bar", "foo.99@bar.com", "f@bar.exampl-3.com."}
	for _, e := range goodEmails {
		assert.True(t, IsValidEmail(e))
	}
	user, _ := auth.NewUser("ValidName", "letmein", nil)
	assert.False(t, user.SetEmail("foo") == nil)
	assert.Equals(t, user.SetEmail("foo@example.com"), nil)
}

func TestUserPasswords(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, _ := auth.NewUser("me", "letmein", nil)
	assert.True(t, user.Authenticate("letmein"))
	assert.False(t, user.Authenticate("password"))
	assert.False(t, user.Authenticate(""))

	guest, _ := auth.NewUser("", "", nil)
	assert.True(t, guest.Authenticate(""))
	assert.False(t, guest.Authenticate("123456"))

	// Create a second user with the same password
	user2, _ := auth.NewUser("me", "letmein", nil)
	assert.True(t, user2.Authenticate("letmein"))
	assert.False(t, user2.Authenticate("password"))
	assert.True(t, user.Authenticate("letmein"))
	assert.False(t, user.Authenticate("password"))
}

func TestSerializeUser(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, _ := auth.NewUser("me", "letmein", ch.SetOf("me", "public"))
	user.SetEmail("foo@example.com")
	encoded, _ := json.Marshal(user)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled User as: %s", encoded)

	resu := &userImpl{}
	err := json.Unmarshal(encoded, resu)
	assert.True(t, err == nil)
	assert.DeepEquals(t, resu.Name(), user.Name())
	assert.DeepEquals(t, resu.Email(), user.Email())
	assert.DeepEquals(t, resu.ExplicitChannels(), user.ExplicitChannels())
	assert.True(t, resu.Authenticate("letmein"))
	assert.False(t, resu.Authenticate("123456"))
}

func TestSerializeRole(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, _ := auth.NewRole("froods", ch.SetOf("hoopy", "public"))
	encoded, _ := json.Marshal(role)
	assert.True(t, encoded != nil)
	log.Printf("Marshaled Role as: %s", encoded)
	elor := &roleImpl{}
	err := json.Unmarshal(encoded, elor)

	assert.True(t, err == nil)
	assert.DeepEquals(t, elor.Name(), role.Name())
	assert.DeepEquals(t, elor.ExplicitChannels(), role.ExplicitChannels())
}

func TestUserAccess(t *testing.T) {

	// User with no access:
	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, _ := auth.NewUser("foo", "password", nil)
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("!"))
	assert.False(t, user.CanSeeChannel("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("*")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)

	// User with access to one channel:
	user.setChannels(ch.AtSequence(ch.SetOf("x"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf("y")) == nil)
	assert.False(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)

	// User with access to one channel and one derived channel:
	user.setChannels(ch.AtSequence(ch.SetOf("x", "z"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with access to two channels:
	user.setChannels(ch.AtSequence(ch.SetOf("x", "z"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "z"))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("x")), ch.SetOf("x"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	user.setChannels(ch.AtSequence(ch.SetOf("x", "y"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("x", "y"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.False(t, canSeeAllChannels(user, ch.SetOf("x", "y", "z")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.False(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)

	// User with wildcard access:
	user.setChannels(ch.AtSequence(ch.SetOf("*", "q"), 1))
	assert.DeepEquals(t, user.ExpandWildCardChannel(ch.SetOf("*")), ch.SetOf("*", "q"))
	assert.True(t, user.CanSeeChannel("*"))
	assert.True(t, canSeeAllChannels(user, ch.SetOf()))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x")))
	assert.True(t, canSeeAllChannels(user, ch.SetOf("x", "y")))
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("x", "y")) == nil)
	assert.True(t, user.AuthorizeAllChannels(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("x")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf("*")) == nil)
	assert.True(t, user.AuthorizeAnyChannel(ch.SetOf()) == nil)
}

func TestGetMissingUser(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.GetUser("noSuchUser")
	assert.Equals(t, err, nil)
	assert.True(t, user == nil)
	user, err = auth.GetUserByEmail("noreply@example.com")
	assert.Equals(t, err, nil)
	assert.True(t, user == nil)
}

func TestGetMissingRole(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, err := auth.GetRole("noSuchRole")
	assert.Equals(t, err, nil)
	assert.True(t, role == nil)
}

func TestGetGuestUser(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.GetUser("")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user, auth.defaultGuestUser())

}

func TestSaveUsers(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user2, user)
}

// Create a user with an expiry
// Wait until the expiry expires and the user is removed
// Try to get the user, assert they have been removed
func TestSaveUsersWithExpiry(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since walrus doesn't support doc expiry")
	}

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	username := "testUser"
	expiryOffset := time.Second
	authOptions := &AuthenticatorOptions{
		InactivityExpiryOffset: expiryOffset,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	user, _ := auth.NewUser(username, "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	getExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())
	expiresAt := time.Unix(int64(getExpiry), 0)
	delta := time.Until(expiresAt)

	// This should be approximately 1 second, but to allow for a lot of clock skew, set to 10 seconds
	assert.True(t, delta < (time.Second*10))

}


type Operation func()

// Create a user with an expiry
// Before expiry expires, perform an op that should extend the expiry, such as:
// - Get the user
// - Update the user
// And verify that the expiry is indeed extended
func TestExtendUserExpiryViaOperations(t *testing.T) {

	base.EnableLogKey("DCP")
	base.EnableLogKey("CRUD+")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since walrus doesn't support doc expiry")
	}

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	authOptions := &AuthenticatorOptions{
		InactivityExpiryOffset: time.Second * time.Duration(60*60*24), // one day
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	testUsername := "testUser"
	user, _ := auth.NewUser(testUsername, "password", ch.SetOf("test"))

	err := auth.Save(user)
	assert.Equals(t, err, nil)

	// At this point, the doc metadata expiry should be identical to the inline LastUpdateExpiry value
	docMetaExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())
	userFromBucket := &userImpl{}
	_, err = gTestBucket.Bucket.Get(user.DocID(), &userFromBucket)
	assert.Equals(t, err, nil)
	assert.Equals(t, userFromBucket.GetLastUpdateExpiry(), docMetaExpiry)

	// A slice of operations which should all cause the User TTL expiry to get extended
	opsThatExtendUserTTL := []Operation{}
	opsThatExtendUserTTL = append(opsThatExtendUserTTL, func() {
		user, err = auth.GetUser("testUser")
		assert.True(t, user != nil)
		assert.True(t, err == nil)
	})
	opsThatExtendUserTTL = append(opsThatExtendUserTTL, func() {
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		user.SetPassword(fmt.Sprintf("password-%d", r1.Intn(100)))
		err := auth.Save(user)
		assert.Equals(t, err, nil)

	})

	// Ensure that after every "GetUser()" that the user TTL expiry is extended
	for i := 0; i < len(opsThatExtendUserTTL); i++ {

		// Get current expiry
		docMetaExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())

		// Must wait a bit, otherwise the GetAndTouch() will be a no-op
		time.Sleep(time.Second * 2)

		// Get the next op and execute it
		op := opsThatExtendUserTTL[i]
		op()

		// Get updated expiry
		updatedDocMetaExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())

		// Make sure the expiry has been increased
		assert.True(t, updatedDocMetaExpiry > docMetaExpiry)

	}

}

// Create a user with an expiry > 30 days
// Make sure user is not immediately deleted
//
// Test motivation: if SG naively tries to use expiry offset, will be unix timestamp that will cause immediate deletion
func TestUserExpiryLargeExpiry(t *testing.T) {

	base.EnableLogKey("DCP")
	base.EnableLogKey("CRUD+")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since walrus doesn't support doc expiry")
	}

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	// Create an authenticator
	expiryOffset := time.Second * time.Duration(60*60*24*60) // 60 days
	authOptions := &AuthenticatorOptions{
		InactivityExpiryOffset: expiryOffset,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)

	// Create a user
	testUsername := "testUser"
	user, _ := auth.NewUser(testUsername, "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	// Assert that doc meta expiry value is in the right ballpark, and > 59 days
	docMetaExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())
	expiresAt := time.Unix(int64(docMetaExpiry), 0)
	deltaUntilExpires := time.Until(expiresAt)
	sixtyDays := base.MaxDeltaTtlDuration * time.Duration(2)
	oneDay := time.Second * time.Duration(60*60*24)
	fiftyNineDays := sixtyDays - oneDay
	assert.True(t, deltaUntilExpires > fiftyNineDays)

}

// Create user with no expiry
// Wait for a while and make sure the user hasn't been deleted
func TestUserExpiryPermanentUser(t *testing.T) {

	base.EnableLogKey("DCP")
	base.EnableLogKey("CRUD+")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since walrus doesn't support doc expiry")
	}

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	testUsername := "testUser"
	user, _ := auth.NewUser(testUsername, "password", ch.SetOf("test"))
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	// Make sure the expiry value is 0 (never expires)
	getExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())
	assert.True(t, getExpiry == 0)


}

// Verifies the CAS retry behavior in getPrincipal() and bucket.Update().
// The motiviation of this test was to increase code coverage in those two functions
// so that they can be safely changed for the User TTL changes.
func TestUserExpiryCASRetry(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server, since the walrus bucket doesn't invoke test callback")
	}

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	sixtyDays := base.MaxDeltaTtlDuration * time.Duration(2)

	authOptions := &AuthenticatorOptions{
		InactivityExpiryOffset: sixtyDays, // 60 days
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)

	// Add a user
	testUsername := "testUser"
	user, _ := auth.NewUser(testUsername, "password", ch.SetOf("test"))

	err := auth.Save(user)
	assert.Equals(t, err, nil)

	//// Modify the user by setting disabled = true
	user.SetDisabled(true)
	err = auth.Save(user)
	assert.Equals(t, err, nil)

	// This will force the next GetUser() to update the user doc
	err = auth.InvalidateChannels(user)
	assert.True(t, err == nil)

	// Trigger a CAS failure/retry by removing the doc expiry.  This will be called back during the
	// GoCB bucket update() invocation between the Get and the Set operation.
	testCallbackInvoked := false
	testCallback := func() {
		if testCallbackInvoked {
			// only want to invoke once
			return
		}
		// Update the user doc directly by setting the expiry to expire in 1 second
		if err := auth.bucket.Set(user.DocID(), 1, user); err != nil {
			t.Fatalf("Error trying to trigger a CAS failure.  Bucket.Set() error: %v", err)
		}
		testCallbackInvoked = true
	}
	gTestBucket.Bucket.SetTestCallback(testCallback)

	// Get the user, and make sure the user has the modifications
	user, err = auth.GetUser("testUser")
	assert.True(t, user != nil)
	assert.True(t, err == nil)
	assert.True(t, user.Disabled() == true)

	// Make sure the test callback was actually invoked
	assert.True(t, testCallbackInvoked)

	// Get the expiry value on the bucket doc
	getExpiry := getUserDocMetaExpiry(t, gTestBucket.Bucket, user.DocID())

	// Assert that the expiry value is the expected value.
	// Just makes sure it's in the right ballbark within a 1 day margin of error.
	expiresAt := time.Unix(int64(getExpiry), 0)
	deltaUntilExpires := time.Until(expiresAt)
	oneDay := time.Second * time.Duration(60*60*24)
	fiftyNineDays := sixtyDays - oneDay
	sixtyOneDays := sixtyDays + oneDay
	assert.True(t, deltaUntilExpires > fiftyNineDays)
	assert.True(t, deltaUntilExpires < sixtyOneDays)

}

func getUserDocMetaExpiry(t *testing.T, bucket base.Bucket, userDocId string) uint32 {
	// Get the expiry value on the bucket doc
	gocbBucket := bucket.(*base.CouchbaseBucketGoCB)
	getExpiry, getExpiryErr := gocbBucket.GetExpiry(userDocId)
	if getExpiryErr != nil {
		assert.True(t, getExpiryErr == nil)
	}
	return getExpiry
}

func TestSaveRoles(t *testing.T) {
	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, _ := auth.NewRole("testRole", ch.SetOf("test"))
	err := auth.Save(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, role2, role)
}

type mockComputer struct {
	channels     ch.TimedSet
	roles        ch.TimedSet
	roleChannels ch.TimedSet
	err          error
}

func (self *mockComputer) ComputeChannelsForPrincipal(p Principal) (ch.TimedSet, error) {
	switch p.(type) {
	case User:
		return self.channels, self.err
	case Role:
		return self.roleChannels, self.err
	default:
		return nil, self.err
	}
}

func (self *mockComputer) ComputeRolesForUser(User) (ch.TimedSet, error) {
	return self.roles, self.err
}

func (self *mockComputer) UseGlobalSequence() bool {
	return true
}

func TestRebuildUserChannels(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	computer := mockComputer{channels: ch.AtSequence(ch.SetOf("derived1", "derived2"), 1)}
	authOptions := &AuthenticatorOptions{
		ChannelComputer: &computer,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	user, _ := auth.NewUser("testUser", "password", ch.SetOf("explicit1"))
	user.setChannels(nil)
	err := auth.Save(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf("explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildRoleChannels(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	computer := mockComputer{roleChannels: ch.AtSequence(ch.SetOf("derived1", "derived2"), 1)}
	authOptions := &AuthenticatorOptions{
		ChannelComputer: &computer,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	role, _ := auth.NewRole("testRole", ch.SetOf("explicit1"))
	err := auth.InvalidateChannels(role)
	assert.Equals(t, err, nil)

	role2, err := auth.GetRole("testRole")
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, role2.Channels(), ch.AtSequence(ch.SetOf("explicit1", "derived1", "derived2", "!"), 1))
}

func TestRebuildChannelsError(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	computer := mockComputer{}
	authOptions := &AuthenticatorOptions{
		ChannelComputer: &computer,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	role, err := auth.NewRole("testRole2", ch.SetOf("explicit1"))
	assert.Equals(t, err, nil)
	assert.Equals(t, auth.InvalidateChannels(role), nil)

	computer.err = errors.New("I'm sorry, Dave.")

	role2, err := auth.GetRole("testRole2")
	assert.Equals(t, role2, nil)
	assert.DeepEquals(t, err, computer.err)
}

func TestRebuildUserRoles(t *testing.T) {

	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	computer := mockComputer{roles: ch.AtSequence(base.SetOf("role1", "role2"), 3)}
	authOptions := &AuthenticatorOptions{
		ChannelComputer: &computer,
	}
	auth := NewAuthenticator(gTestBucket.Bucket, authOptions)
	user, _ := auth.NewUser("testUser", "letmein", nil)
	user.SetExplicitRoles(ch.TimedSet{"role3": ch.NewVbSimpleSequence(1), "role1": ch.NewVbSimpleSequence(1)})
	err := auth.InvalidateRoles(user)
	assert.Equals(t, err, nil)

	user2, err := auth.GetUser("testUser")
	assert.Equals(t, err, nil)
	expected := ch.AtSequence(base.SetOf("role1", "role3"), 1)
	expected.AddChannel("role2", 3)
	assert.DeepEquals(t, user2.RoleNames(), expected)
}

func TestRoleInheritance(t *testing.T) {
	// Create some roles:
	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	role, _ := auth.NewRole("square", ch.SetOf("dull", "duller", "dullest"))
	assert.Equals(t, auth.Save(role), nil)
	role, _ = auth.NewRole("frood", ch.SetOf("hoopy", "hoopier", "hoopiest"))
	assert.Equals(t, auth.Save(role), nil)

	user, _ := auth.NewUser("arthur", "password", ch.SetOf("britain"))
	user.(*userImpl).setRolesSince(ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	assert.DeepEquals(t, user.RoleNames(), ch.TimedSet{"square": ch.NewVbSimpleSequence(0x3), "nonexistent": ch.NewVbSimpleSequence(0x42), "frood": ch.NewVbSimpleSequence(0x4)})
	auth.Save(user)

	user2, err := auth.GetUser("arthur")
	assert.Equals(t, err, nil)
	log.Printf("Channels = %s", user2.Channels())
	assert.DeepEquals(t, user2.Channels(), ch.AtSequence(ch.SetOf("!", "britain"), 1))
	assert.DeepEquals(t, user2.InheritedChannels(),
		ch.TimedSet{"!": ch.NewVbSimpleSequence(0x1), "britain": ch.NewVbSimpleSequence(0x1), "dull": ch.NewVbSimpleSequence(0x3), "duller": ch.NewVbSimpleSequence(0x3), "dullest": ch.NewVbSimpleSequence(0x3), "hoopy": ch.NewVbSimpleSequence(0x4), "hoopier": ch.NewVbSimpleSequence(0x4), "hoopiest": ch.NewVbSimpleSequence(0x4)})
	assert.True(t, user2.CanSeeChannel("britain"))
	assert.True(t, user2.CanSeeChannel("duller"))
	assert.True(t, user2.CanSeeChannel("hoopy"))
	assert.Equals(t, user2.AuthorizeAllChannels(ch.SetOf("britain", "dull", "hoopiest")), nil)
}

func TestRegisterUser(t *testing.T) {
	gTestBucket := base.GetTestBucketOrPanic()
	defer gTestBucket.Close()

	// Register user based on name, email
	auth := NewAuthenticator(gTestBucket.Bucket, nil)
	user, err := auth.RegisterNewUser("ValidName", "foo@example.com")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, user.Email(), "foo@example.com")
	assert.Equals(t, err, nil)

	// verify retrieval by username
	user, err = auth.GetUser("ValidName")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, err, nil)

	// verify retrieval by email
	user, err = auth.GetUserByEmail("foo@example.com")
	assert.Equals(t, user.Name(), "ValidName")
	assert.Equals(t, err, nil)

	// Register user based on email, retrieve based on username, email
	user, err = auth.RegisterNewUser("bar@example.com", "bar@example.com")
	assert.Equals(t, user.Name(), "bar@example.com")
	assert.Equals(t, user.Email(), "bar@example.com")
	assert.Equals(t, err, nil)

	user, err = auth.GetUser("UnknownName")
	assert.Equals(t, user, nil)
	assert.Equals(t, err, nil)

	user, err = auth.GetUserByEmail("bar@example.com")
	assert.Equals(t, user.Name(), "bar@example.com")
	assert.Equals(t, err, nil)

	// Register user without an email address
	user, err = auth.RegisterNewUser("01234567890", "")
	assert.Equals(t, user.Name(), "01234567890")
	assert.Equals(t, user.Email(), "")
	assert.Equals(t, err, nil)
	// Get above user by username.
	user, err = auth.GetUser("01234567890")
	assert.Equals(t, user.Name(), "01234567890")
	assert.Equals(t, user.Email(), "")
	assert.Equals(t, err, nil)
	// Make sure we can't retrieve 01234567890 by supplying empty email.
	user, err = auth.GetUserByEmail("")
	assert.Equals(t, user, nil)
	assert.Equals(t, err, nil)
}

// 8 cases
// C: Channel grant
// R: Role grant
// RC: Channel grant to role
// C R RC |                    AllBefore
// C R    | RC
// C RC   | R
// C      | R RC
// R RC   | C
// RC     | C R
// R      | C RC
//        | C R RC

func TestFilterToAvailableSince(t *testing.T) {

	tests := []struct {
		name                     string
		syncGrantChannels        ch.TimedSet
		syncGrantRoles           ch.TimedSet
		syncGrantRoleChannels    ch.TimedSet
		expectedResult           ch.VbSequence
		expectedSecondaryTrigger ch.VbSequence
	}{
		{"AllBeforeSince",
			ch.TimedSet{"A": ch.NewVbSequence(10, 50)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 60)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 70)},
			ch.NewVbSequence(10, 50),
			ch.VbSequence{},
		},
		{"UserBeforeSince",
			ch.TimedSet{"A": ch.NewVbSequence(10, 50)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 170)},
			ch.NewVbSequence(10, 50),
			ch.VbSequence{},
		},
		{"RoleGrantBeforeSince",
			ch.TimedSet{"A": ch.NewVbSequence(10, 150)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 60)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 170)},
			ch.NewVbSequence(10, 150),
			ch.VbSequence{},
		},
		{"RoleGrantAfterSince",
			ch.TimedSet{},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 70)},
			ch.NewVbSequence(20, 160),
			ch.VbSequence{},
		},
		{"RoleAndRoleChannelGrantAfterSince_ChannelGrantFirst",
			ch.TimedSet{},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(10, 170)},
			ch.NewVbSequence(20, 160),
			ch.NewVbSequence(10, 170),
		},
		{"RoleAndRoleChannelGrantAfterSince_RoleGrantFirst",
			ch.TimedSet{},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(10, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(20, 170)},
			ch.NewVbSequence(20, 170),
			ch.NewVbSequence(10, 160),
		},
		{"UserGrantOnly",
			ch.TimedSet{"A": ch.NewVbSequence(10, 50)},
			ch.TimedSet{},
			ch.TimedSet{},
			ch.NewVbSequence(10, 50),
			ch.VbSequence{},
		},
		{"RoleGrantBeforeSince",
			ch.TimedSet{"A": ch.NewVbSequence(10, 150)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 60)},
			ch.TimedSet{},
			ch.NewVbSequence(10, 150),
			ch.VbSequence{},
		},
		{"AllAfterSince",
			ch.TimedSet{"A": ch.NewVbSequence(10, 150)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 170)},
			ch.NewVbSequence(10, 150),
			ch.VbSequence{},
		},
		{"AllAfterSinceRoleGrantFirst",
			ch.TimedSet{"A": ch.NewVbSequence(30, 150)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(10, 170)},
			ch.NewVbSequence(20, 160),
			ch.NewVbSequence(10, 170),
		},
		{"AllAfterSinceRoleChannelGrantFirst",
			ch.TimedSet{"A": ch.NewVbSequence(30, 150)},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(10, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(20, 170)},
			ch.NewVbSequence(20, 170),
			ch.NewVbSequence(10, 160),
		},
		{"AllAfterSinceNoUserGrant",
			ch.TimedSet{},
			ch.TimedSet{"ROLE_1": ch.NewVbSequence(20, 160)},
			ch.TimedSet{"A": ch.NewVbSequence(30, 170)},
			ch.NewVbSequence(30, 170),
			ch.NewVbSequence(20, 160),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gTestBucket := base.GetTestBucketOrPanic()
			defer gTestBucket.Close()

			sinceClock := NewTestingClockAtSequence(100)
			computer := mockComputer{channels: tc.syncGrantChannels, roles: tc.syncGrantRoles, roleChannels: tc.syncGrantRoleChannels}
			authOptions := &AuthenticatorOptions{
				ChannelComputer: &computer,
			}
			auth := NewAuthenticator(gTestBucket.Bucket, authOptions)

			// Set up roles, user
			role, _ := auth.NewRole("ROLE_1", nil)
			assert.Equals(t, auth.Save(role), nil)

			user, _ := auth.NewUser("testUser", "password", ch.SetOf("explicit1"))
			user.setChannels(nil)
			err := auth.Save(user)
			assert.Equals(t, err, nil)

			user2, err := auth.GetUser("testUser")
			assert.Equals(t, err, nil)

			channelsSinceStar, secondaryTriggersStar := user2.FilterToAvailableChannelsForSince(ch.SetOf("*"), sinceClock)
			channelA_Star, ok := channelsSinceStar["A"]
			log.Printf("channelA_Star: %s", channelA_Star)
			assert.True(t, ok)
			assert.True(t, channelA_Star.Equals(tc.expectedResult))
			secondaryTriggerStar, ok := secondaryTriggersStar["A"]
			log.Printf("secondaryTrigger %v, expected %v", secondaryTriggerStar, tc.expectedSecondaryTrigger)
			assert.True(t, secondaryTriggerStar.Equals(tc.expectedSecondaryTrigger))

			channelsSince, secondaryTriggersSince := user2.FilterToAvailableChannelsForSince(ch.SetOf("A"), sinceClock)
			channelA_Single, ok := channelsSince["A"]
			log.Printf("channelA_Single: %s", channelA_Single)
			assert.True(t, ok)
			assert.True(t, channelA_Single.Equals(tc.expectedResult))
			secondaryTriggerSince, ok := secondaryTriggersSince["A"]
			log.Printf("secondaryTrigger %v, expected %v", secondaryTriggerSince, tc.expectedSecondaryTrigger)
			assert.True(t, secondaryTriggerSince.Equals(tc.expectedSecondaryTrigger))

			channelBSince, secondaryTriggersB := user2.FilterToAvailableChannelsForSince(ch.SetOf("B"), sinceClock)
			log.Printf("channelBSince: %s", channelBSince)
			assert.True(t, len(channelBSince) == 0)
			assert.True(t, len(secondaryTriggersB) == 0)

			channelsSinceMulti, secondaryTriggersMulti := user2.FilterToAvailableChannelsForSince(ch.SetOf("A", "B"), sinceClock)
			log.Printf("syncGrant1Multi: %s", channelsSinceMulti)
			assert.True(t, len(channelsSinceMulti) == 1)
			channelA_Multi, ok := channelsSinceMulti["A"]
			assert.True(t, ok)
			assert.True(t, channelA_Multi.Equals(tc.expectedResult))
			secondaryTriggerMulti, ok := secondaryTriggersMulti["A"]
			log.Printf("secondaryTrigger %v, expected %v", secondaryTriggerMulti, tc.expectedSecondaryTrigger)
			assert.True(t, secondaryTriggerMulti.Equals(tc.expectedSecondaryTrigger))

		})
	}

}

func NewTestingClockAtSequence(sequence uint64) *base.SequenceClockImpl {
	clock := base.NewSequenceClockImpl()
	for k, _ := range clock.Value() {
		clock.SetSequence(uint16(k), sequence)
	}
	return clock

}
