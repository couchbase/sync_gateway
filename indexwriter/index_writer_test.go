package indexwriter

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
)

func tearDownIndexTest(dbContext *db.DatabaseContext, indexWriter *kvChangeIndexWriter) {
	indexWriter.Stop()
	dbContext.Close()
}

func TestIndexChangesBasic(t *testing.T) {
	base.EnableLogKey("DIndex+")
	indexWriter, _, _ := testKvIndexWriter(t)
	dbContext := indexWriter.context
	defer tearDownIndexTest(dbContext, indexWriter)
	dbContext.ChannelMapper = channels.NewDefaultChannelMapper()

	// Write an entry to the bucket
	adminDB, _ := db.GetDatabase(dbContext, nil)
	_, err := adminDB.Put("1c856b5724dcf4273c3993619900ce7f", db.Body{"channels": []string{"ABC", "PBS"}})
	assertNoError(t, err, "Error putting doc")

	// Create a user with access to channel ABC
	userDB := testUserDB(dbContext, "naomi", "letmein", []string{"ABC", "PBS", "NBC", "TBS"})

	time.Sleep(200 * time.Millisecond)
	changes, err := userDB.GetChanges(base.SetOf("*"), db.ChangesOptions{Since: SimpleClockSequence(0)})
	printChanges(changes)
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 2)

	// Write a few more entries to the bucket
	adminDB.Put("12389b182ababd12fff662848edeb908", db.Body{"channels": []string{"PBS"}})
	time.Sleep(200 * time.Millisecond)
	changes, err = userDB.GetChanges(base.SetOf("*"), db.ChangesOptions{Since: SimpleClockSequence(0)})
	assert.True(t, err == nil)
	assert.Equals(t, len(changes), 3)
}

func TestIndexChangesAdminBackfill(t *testing.T) {
	base.EnableLogKey("DIndex+")
	indexWriter, _, _ := testKvIndexWriter(t)
	dbContext := indexWriter.context
	defer tearDownIndexTest(dbContext, indexWriter)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	dbContext.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create docs on multiple channels:
	adminDB, _ := db.GetDatabase(dbContext, nil)
	adminDB.Put("both_1", db.Body{"channels": []string{"ABC", "PBS"}})
	adminDB.Put("doc0000609", db.Body{"channels": []string{"PBS"}})
	adminDB.Put("doc0000799", db.Body{"channels": []string{"ABC"}})
	time.Sleep(100 * time.Millisecond)

	// Create a user with access to channel ABC
	userDB := testUserDB(dbContext, "naomi", "letmein", []string{"ABC"})

	// Check the _changes feed:
	changes, err := userDB.GetChanges(base.SetOf("*"), getZeroSequence(userDB))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)

	// Modify user to have access to both channels:
	log.Println("Get Principal")
	userInfo, err := dbContext.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = dbContext.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")
	time.Sleep(100 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	adminDB.Put("doc_nobackfill_1", db.Body{"channels": []string{"PBS"}})
	adminDB.Put("doc_nobackfill_2", db.Body{"channels": []string{"PBS"}})
	time.Sleep(100 * time.Millisecond)

	// Reinit user database to pick up user changes
	log.Println("Get User")
	user, _ := dbContext.Authenticator().GetUser("naomi")
	userDB, _ = db.GetDatabase(dbContext, user)
	// need waitForIndexing here
	//dbContext.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = dbContext.ParseSequenceID(lastSeq.String())
	changes, err = userDB.GetChanges(base.SetOf("*"), db.ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)
}

func TestIndexChangesRestartBackfill(t *testing.T) {
	base.EnableLogKey("DIndex+")
	indexWriter, _, _ := testKvIndexWriter(t)
	dbContext := indexWriter.context
	defer tearDownIndexTest(dbContext, indexWriter)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	dbContext.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := dbContext.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	user.SetSequence(1)
	authenticator.Save(user)

	// Create docs on multiple channels:
	adminDB, _ := db.GetDatabase(dbContext, nil)
	adminDB.Put("both_1", db.Body{"channels": []string{"ABC", "PBS"}})
	adminDB.Put("doc0000609", db.Body{"channels": []string{"PBS"}})
	adminDB.Put("doc0000799", db.Body{"channels": []string{"ABC"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	userDB, _ := db.GetDatabase(dbContext, user)
	changes, err := userDB.GetChanges(base.SetOf("*"), getZeroSequence(userDB))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)

	// Modify user to have access to both channels:
	log.Println("Get Principal")
	userInfo, err := dbContext.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = dbContext.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")
	time.Sleep(100 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	adminDB.Put("doc_nobackfill_1", db.Body{"channels": []string{"PBS"}})
	adminDB.Put("doc_nobackfill_2", db.Body{"channels": []string{"PBS"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	user, _ = authenticator.GetUser("naomi")
	userDB, _ = db.GetDatabase(dbContext, user)
	time.Sleep(100 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = dbContext.ParseSequenceID(lastSeq.String())
	log.Println("Get Changes")
	changes, err = userDB.GetChanges(base.SetOf("*"), db.ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

	// Request the changes from midway through backfill
	lastSeq, _ = userDB.ParseSequenceID("14-0:504.3")
	log.Println("Get Changes")
	changes, err = userDB.GetChanges(base.SetOf("*"), db.ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

}

/*
func CouchbaseTestIndexChangesAccessBackfill(t *testing.T) {

	// Not walrus compatible, until we add support for meta.vb and meta.vbseq to walrus views.
	dbContext := setupdbContextForChangeIndex(t)
	defer dbContext.Close()
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	dbContext.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		if (doc.accessGrant) {
			console.log("access grant to " + doc.accessGrant);
			access(doc.accessGrant, "PBS");
		}
		channel(doc.channels);
	}`)

	// Create a user with access to channel ABC
	authenticator := dbContext.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Create docs on multiple channels:
	_, err := dbContext.Put("both_1", Body{"channels": []string{"ABC", "PBS"}})
	assertNoError(t, err, "Put failed")
	_, err = dbContext.Put("doc0000609", Body{"channels": []string{"PBS"}})
	assertNoError(t, err, "Put failed")
	_, err = dbContext.Put("doc0000799", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put failed")

	time.Sleep(2000 * time.Millisecond)

	// Check the _changes feed:
	dbContext.user, _ = authenticator.GetUser("naomi")
	changes, err := dbContext.GetChanges(base.SetOf("*"), getZeroSequence(dbContext))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 2)

	// Write a doc to grant user access to PBS:
	dbContext.Put("doc_grant", Body{"accessGrant": "naomi"})
	time.Sleep(1000 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	dbContext.Put("doc_nobackfill_1", Body{"channels": []string{"PBS"}})
	dbContext.Put("doc_nobackfill_2", Body{"channels": []string{"PBS"}})
	time.Sleep(1000 * time.Millisecond)

	// Check the _changes feed:
	log.Println("Get User")
	dbContext.user, _ = authenticator.GetUser("naomi")
	dbContext.changeCache.waitForSequence(1)
	time.Sleep(1000 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = dbContext.ParseSequenceID(lastSeq.String())
	log.Println("Get Changes")
	changes, err = dbContext.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

}

func TestIndexChangesAfterChannelAdded(t *testing.T) {
	dbContext := setupdbContextForChangeIndex(t)
	defer dbContext.Close()
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	dbContext.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := dbContext.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	user.SetSequence(1)
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	_, err := dbContext.Put("doc1", Body{"channels": []string{"ABC", "PBS"}})
	assertNoError(t, err, "Put failed")
	dbContext.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := dbContext.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = dbContext.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	// Check the _changes feed:
	dbContext.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)
	dbContext.Bucket.Dump()
	if changeCache, ok := dbContext.changeCache.(*kvChangeIndex); ok {
		changeCache.reader.indexReadBucket.Dump()
	}
	dbContext.user, _ = authenticator.GetUser("naomi")
	changes, err := dbContext.GetChanges(base.SetOf("*"), getZeroSequence(dbContext))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(250 * time.Millisecond)
	assert.Equals(t, len(changes), 2)
	verifyChange(t, changes, "_user/naomi", false)
	verifyChange(t, changes, "doc1", false)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = dbContext.ParseSequenceID(lastSeq.String())

	// Add a new doc (sequence 3):
	_, err = dbContext.Put("doc2", Body{"channels": []string{"PBS"}})
	assertNoError(t, err, "Put failed")

	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	dbContext.changeCache.waitForSequence(3)
	// changes, err = dbContext.GetChanges(base.SetOf("*"), ChangesOptions{Since: dbContext.ParseSequenceID(lastSeq)})
	changes, err = dbContext.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	printChanges(changes)
	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)

	verifyChange(t, changes, "doc2", false)

	// validate from zero
	log.Println("From zero:")
	//changes, err = dbContext.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 1, TriggeredBy: 2}})
	changes, err = dbContext.GetChanges(base.SetOf("*"), getZeroSequence(dbContext))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
}
*/

// Verifies that a doc is present in the ChangeEntry array, and whether it's sent as part of a backfill (based on triggeredBy value in the sequence)
func verifyChange(t *testing.T, changes []*db.ChangeEntry, docID string, isBackfill bool) {
	for _, change := range changes {
		if change.ID == docID {
			hasBackfill := change.Seq.TriggeredBy > 0
			if isBackfill != hasBackfill {
				assertFailed(t, fmt.Sprintf("VerifyChange failed for docID %s: expected backfill: %v, actual backfill: %v", docID, isBackfill, hasBackfill))
			}
			return
		}
	}
	assertFailed(t, fmt.Sprintf("VerifyChange failed - DocID %s not found in set", docID))
}

func getZeroSequence(database *db.Database) db.ChangesOptions {
	if database.SequenceType == db.IntSequenceType {
		return db.ChangesOptions{Since: db.SequenceID{Seq: 0}}
	} else {
		return db.ChangesOptions{Since: db.SequenceID{Clock: base.NewSequenceClockImpl()}}
	}
}

func printChanges(changes []*db.ChangeEntry) {
	for _, change := range changes {
		log.Printf("Change:%+v", change)
	}
}

func getLastSeq(changes []*db.ChangeEntry) db.SequenceID {
	if len(changes) > 0 {
		return changes[len(changes)-1].Seq
	}
	return db.SequenceID{}
}

func testUserDB(dbContext *db.DatabaseContext, username string, password string, channelNames []string) *db.Database {

	authenticator := dbContext.Authenticator()
	channelSet, _ := channels.SetFromArray(channelNames, channels.KeepStar)
	log.Printf("channel set:%v", channelSet)
	user, _ := authenticator.NewUser(username, password, channelSet)
	user.SetSequence(1)
	authenticator.Save(user)

	// Check the _changes feed:
	userDB, _ := db.GetDatabase(dbContext, user)
	return userDB
}
