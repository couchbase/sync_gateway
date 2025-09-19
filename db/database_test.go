//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/robertkrimen/otto/underscore"
	"github.com/stretchr/testify/assert"
)

func init() {
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

// Note: It is important to call db.Close() on the returned database.
func setupTestDB(t testing.TB) (*Database, context.Context) {
	return setupTestDBWithCacheOptions(t, DefaultCacheOptions())
}

func setupTestDBAllowConflicts(t testing.TB) (*Database, context.Context) {
	dbcOptions := DatabaseContextOptions{
		AllowConflicts: base.Ptr(true),
		CacheOptions:   base.Ptr(DefaultCacheOptions()),
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

func setupTestDBForBucket(t testing.TB, bucket base.Bucket) (*Database, context.Context) {
	cacheOptions := DefaultCacheOptions()
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &cacheOptions,
	}
	return SetupTestDBForBucketWithOptions(t, bucket, dbcOptions)
}

func setupTestDBForBucketDefaultCollection(t testing.TB, bucket *base.TestBucket) (*Database, context.Context) {
	cacheOptions := DefaultCacheOptions()
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &cacheOptions,
		Scopes:       GetScopesOptionsDefaultCollectionOnly(t),
	}
	return SetupTestDBForBucketWithOptions(t, bucket, dbcOptions)
}

func setupTestDBWithOptionsAndImport(t testing.TB, tBucket *base.TestBucket, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	ctx := base.TestCtx(t)
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	if tBucket == nil {
		tBucket = base.GetTestBucket(t)
	}
	if dbcOptions.Scopes == nil {
		dbcOptions.Scopes = GetScopesOptions(t, tBucket, 1)
	}
	if dbcOptions.GroupID == "" && base.IsEnterpriseEdition() {
		dbcOptions.GroupID = t.Name()
	}
	dbCtx, err := NewDatabaseContext(ctx, "db", tBucket, true, dbcOptions)
	require.NoError(t, err, "Couldn't create context for database 'db'")

	ctx = dbCtx.AddDatabaseLogContext(ctx)
	err = dbCtx.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	db, err := CreateDatabase(dbCtx)
	require.NoError(t, err, "Couldn't create database 'db'")
	return db, addDatabaseAndTestUserContext(ctx, db)
}

func setupTestDBWithCacheOptions(t testing.TB, options CacheOptions) (*Database, context.Context) {

	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

// Forces UseViews:true in the database context.  Useful for testing w/ views while running
// tests against Couchbase Server
func setupTestDBWithViewsEnabled(t testing.TB) (*Database, context.Context) {
	if !base.TestsDisableGSI() {
		t.Skip("GSI is not compatible with views")
	}
	dbcOptions := DatabaseContextOptions{
		UseViews: true,
		Scopes:   GetScopesOptionsDefaultCollectionOnly(t),
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

// Sets up a test bucket with _sync:seq initialized to a high value prior to database creation.  Used to test
// issues with custom _sync:seq values without triggering skipped sequences between 0 and customSeq
func setupTestDBWithCustomSyncSeq(t testing.TB, customSeq uint64) (*Database, context.Context) {

	ctx := base.TestCtx(t)
	tBucket := base.GetTestBucket(t)
	dbcOptions := DatabaseContextOptions{
		Scopes: GetScopesOptions(t, tBucket, 1),
	}

	AddOptionsFromEnvironmentVariables(&dbcOptions)

	// This may need to change when we move to a non-default metadata collection...
	metadataStore := tBucket.GetMetadataStore()

	log.Printf("Initializing test %s to %d", base.DefaultMetadataKeys.SyncSeqKey(), customSeq)
	_, incrErr := metadataStore.Incr(base.DefaultMetadataKeys.SyncSeqKey(), customSeq, customSeq, 0)
	assert.NoError(t, incrErr, fmt.Sprintf("Couldn't increment %s by %d", base.DefaultMetadataKeys.SyncSeqKey(), customSeq))

	dbCtx, err := NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")

	ctx = dbCtx.AddDatabaseLogContext(ctx)
	err = dbCtx.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	db, err := CreateDatabase(dbCtx)
	assert.NoError(t, err, "Couldn't create database 'db'")

	atomic.StoreUint32(&dbCtx.State, DBOnline)

	return db, addDatabaseAndTestUserContext(ctx, db)
}

func setupTestLeakyDBWithCacheOptions(t *testing.T, options CacheOptions, leakyOptions base.LeakyBucketConfig) (*Database, context.Context) {
	testBucket := base.GetTestBucket(t)
	leakyBucket := base.NewLeakyBucket(testBucket, leakyOptions)
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	return SetupTestDBForBucketWithOptions(t, leakyBucket, dbcOptions)
}

func setupTestDBDefaultCollection(t testing.TB) (*Database, context.Context) {
	cacheOptions := DefaultCacheOptions()
	dbcOptions := DatabaseContextOptions{
		Scopes:       GetScopesOptionsDefaultCollectionOnly(t),
		CacheOptions: &cacheOptions,
	}
	return SetupTestDBWithOptions(t, dbcOptions)
}

func assertHTTPError(t *testing.T, err error, status int) bool {
	var httpErr *base.HTTPError
	return assert.Error(t, err) &&
		assert.ErrorAs(t, err, &httpErr) &&
		assert.Equal(t, status, httpErr.Status)
}

func TestDatabase(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, doc, err := collection.Put(ctx, "doc1", body)
	body[BodyId] = doc.ID
	body[BodyRev] = rev1id
	assert.NoError(t, err, "Couldn't create document")
	assert.Equal(t, "1-cb0c9a22be0e5a1b01084ec019defa81", rev1id)

	log.Printf("Create rev 2...")
	body["key1"] = "new value"
	body["key2"] = int64(4321)
	rev2id, _, err := collection.Put(ctx, "doc1", body)
	body[BodyId] = "doc1"
	body[BodyRev] = rev2id
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "2-488724414d0ed6b398d6d2aeb228d797", rev2id)

	// Retrieve the document:
	log.Printf("Retrieve doc...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

	log.Printf("Retrieve rev 1...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev1id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev 1")
	expectedResult := Body{"key1": "value1", "key2": 1234, BodyId: "doc1", BodyRev: rev1id}
	AssertEqualBodies(t, expectedResult, gotbody)

	log.Printf("Retrieve rev 2...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev2id, false, nil)
	assert.NoError(t, err, "Couldn't get document with rev")
	AssertEqualBodies(t, body, gotbody)

	gotbody, err = collection.Get1xRevBody(ctx, "doc1", "bogusrev", false, nil)
	status, _ := base.ErrorAsHTTPStatus(err)
	assert.Equal(t, 404, status)
	require.Nil(t, gotbody)

	// Test the _revisions property:
	log.Printf("Check _revisions...")
	gotbody, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	require.NoError(t, err)
	revisions := gotbody[BodyRevisions].(Revisions)
	assert.Equal(t, 2, revisions[RevisionsStart])
	assert.Equal(t, []string{"488724414d0ed6b398d6d2aeb228d797",
		"cb0c9a22be0e5a1b01084ec019defa81"}, revisions[RevisionsIds])

	// Test RevDiff:
	log.Printf("Check RevDiff...")
	missing, possible := collection.RevDiff(ctx, "doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"2-488724414d0ed6b398d6d2aeb228d797"})
	assert.True(t, missing == nil)
	assert.True(t, possible == nil)

	missing, possible = collection.RevDiff(ctx, "doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.Equal(t, []string{"3-foo"}, missing)
	assert.Equal(t, []string{"2-488724414d0ed6b398d6d2aeb228d797"}, possible)

	missing, possible = collection.RevDiff(ctx, "nosuchdoc",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.Equal(t, []string{"1-cb0c9a22be0e5a1b01084ec019defa81",
		"3-foo"}, missing)

	assert.True(t, possible == nil)

	// Test CheckProposedRev:
	log.Printf("Check CheckProposedRev...")
	proposedStatus, current := collection.CheckProposedRev(ctx, "doc1", "3-foo",
		"2-488724414d0ed6b398d6d2aeb228d797")
	assert.Equal(t, ProposedRev_OK, proposedStatus)
	assert.Equal(t, "", current)

	proposedStatus, current = collection.CheckProposedRev(ctx, "doc1",
		"2-488724414d0ed6b398d6d2aeb228d797",
		"1-xxx")
	assert.Equal(t, ProposedRev_Exists, proposedStatus)
	assert.Equal(t, "", current)

	proposedStatus, current = collection.CheckProposedRev(ctx, "doc1",
		"3-foo",
		"2-bogus")
	assert.Equal(t, ProposedRev_Conflict, proposedStatus)
	assert.Equal(t, "2-488724414d0ed6b398d6d2aeb228d797", current)

	proposedStatus, current = collection.CheckProposedRev(ctx, "doc1",
		"3-foo",
		"")
	assert.Equal(t, ProposedRev_Conflict, proposedStatus)
	assert.Equal(t, "2-488724414d0ed6b398d6d2aeb228d797", current)

	proposedStatus, current = collection.CheckProposedRev(ctx, "nosuchdoc", "3-foo", "")
	assert.Equal(t, ProposedRev_OK_IsNew, proposedStatus)
	assert.Equal(t, "", current)

	// Test PutExistingRev:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", body, history, false, ExistingVersionWithUpdateToHLV)
	body[BodyId] = doc.ID
	body[BodyRev] = newRev
	assert.NoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	log.Printf("Check Get...")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	AssertEqualBodies(t, body, gotbody)

}

// TestCheckProposedVersion ensures that a given CV will return the appropriate status based on the information present in the HLV.
func TestCheckProposedVersion(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a doc
	body := Body{"key1": "value1", "key2": 1234}
	_, doc, err := collection.Put(ctx, "doc1", body)
	require.NoError(t, err)
	cvSource, cvValue := doc.HLV.GetCurrentVersion()
	currentVersion := Version{cvSource, cvValue}

	testCases := []struct {
		name            string
		newVersion      Version
		previousVersion *Version
		expectedStatus  ProposedRevStatus
		expectedRev     string
	}{
		{
			// proposed version matches the current server version
			//  Already known
			name:            "version exists",
			newVersion:      currentVersion,
			previousVersion: nil,
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// proposed version is newer than server cv (same source), and previousVersion matches server cv
			//  Not a conflict
			name:            "new version,same source,prev matches",
			newVersion:      Version{cvSource, incrementCas(cvValue, 100)},
			previousVersion: &currentVersion,
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// proposed version is newer than server cv (same source), and previousVersion is not specified.
			//  Not a conflict, even without previousVersion, because of source match
			name:            "new version,same source,prev not specified",
			newVersion:      Version{cvSource, incrementCas(cvValue, 100)},
			previousVersion: nil,
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// proposed version is from a source not present in server HLV, and previousVersion matches server cv
			//  Not a conflict, due to previousVersion match
			name:            "new version,new source,prev matches",
			newVersion:      Version{"other", incrementCas(cvValue, 100)},
			previousVersion: &currentVersion,
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// proposed version is newer than server cv (same source), but previousVersion does not match server cv.
			//  Not a conflict, regardless of previousVersion mismatch, because of source match between proposed
			//  version and cv
			name:            "new version,prev mismatch,new matches cv",
			newVersion:      Version{cvSource, incrementCas(cvValue, 100)},
			previousVersion: &Version{"other", incrementCas(cvValue, 50)},
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// proposed version is already known, source matches cv
			name:           "proposed version already known, no prev version",
			newVersion:     Version{cvSource, incrementCas(cvValue, -100)},
			expectedStatus: ProposedRev_Exists,
			expectedRev:    "",
		},
		{
			// conflict - previous version is older than CV
			name:            "conflict,same source,server updated",
			newVersion:      Version{"other", incrementCas(cvValue, -100)},
			previousVersion: &Version{cvSource, incrementCas(cvValue, -50)},
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     Version{cvSource, cvValue}.String(),
		},
		{
			// conflict - previous version is older than CV
			name:            "conflict,new source,server updated",
			newVersion:      Version{"other", incrementCas(cvValue, 100)},
			previousVersion: &Version{"other", incrementCas(cvValue, -50)},
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     Version{cvSource, cvValue}.String(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			previousVersionStr := ""
			if tc.previousVersion != nil {
				previousVersionStr = tc.previousVersion.String()
			}
			status, rev := collection.CheckProposedVersion(ctx, "doc1", tc.newVersion.String(), previousVersionStr, previousVersionStr)
			assert.Equal(t, tc.expectedStatus, status)
			assert.Equal(t, tc.expectedRev, rev)
		})
	}

	t.Run("invalid hlv", func(t *testing.T) {
		hlvString := ""
		status, _ := collection.CheckProposedVersion(ctx, "doc1", hlvString, "", "")
		assert.Equal(t, ProposedRev_Error, status)
	})

	// New doc cases - standard insert
	t.Run("new doc", func(t *testing.T) {
		newVersion := Version{"other", 100}.String()
		status, _ := collection.CheckProposedVersion(ctx, "doc2", newVersion, "", "")
		assert.Equal(t, ProposedRev_OK_IsNew, status)
	})

	// New doc cases - insert with prev version (previous version purged from SGW)
	t.Run("new doc with prev version", func(t *testing.T) {
		newVersion := Version{"other", 100}.String()
		prevVersion := Version{"another other", 50}.String()
		status, _ := collection.CheckProposedVersion(ctx, "doc2", newVersion, prevVersion, prevVersion)
		assert.Equal(t, ProposedRev_OK_IsNew, status)
	})

}

func TestUpsertTestDocVersion(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create singleVersionDoc with cv:1000@abc
	body := Body{"key1": "value1", "key2": 1234}
	singleVersionDoc := collection.UpsertTestDocWithVersion(ctx, t, "singleVersionDoc", body, "1000@abc", "")
	log.Printf("created singleVersionDoc doc successfully with HLV: %#v", singleVersionDoc.HLV)

	// Attempt to upsert the same version, nil doc indicates no update
	singleVersionDoc = collection.UpsertTestDocWithVersion(ctx, t, "singleVersionDoc", body, "1000@abc", "")
	require.Nil(t, singleVersionDoc)
}

// TestCheckProposedVersionWithHLVRev tests CheckProposedVersion when the full HLV is provided in the rev element of the proposeChanges message
func TestCheckProposedVersionWithHLVRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create singleVersionDoc with cv:1000@abc
	body := Body{"key1": "value1", "key2": 1234}
	singleVersionDoc := collection.UpsertTestDocWithVersion(ctx, t, "singleVersionDoc", body, "1000@abc", "")
	require.NotNil(t, singleVersionDoc)
	log.Printf("created singleVersionDoc doc successfully with HLV: %#v", singleVersionDoc.HLV)

	// create multiVersionDoc with cv:1000@abc, pv:900@def
	collection.UpsertTestDocWithVersion(ctx, t, "multiVersionDoc", body, "900@def", "")
	multiVersionDoc := collection.UpsertTestDocWithVersion(ctx, t, "multiVersionDoc", body, "1000@abc", "")
	require.NotNil(t, multiVersionDoc)
	log.Printf("created multiVersionDoc doc successfully with HLV: %#v", multiVersionDoc.HLV)

	// create multiVersionDoc with cv:1000@abc, mv:900@abc, 900@def, pv:900@ghi
	collection.UpsertTestDocWithVersion(ctx, t, "mergeVersionDoc", body, "900@ghi", "")
	docWithMV := collection.UpsertTestDocWithVersion(ctx, t, "mergeVersionDoc", body, "1000@abc", "900@abc, 900@def")
	require.NotNil(t, docWithMV)
	log.Printf("created mergeVersionDoc doc successfully with HLV: %#v", docWithMV.HLV)

	testCases := []struct {
		name            string // test name
		key             string
		proposedVersion string            // proposed version in CBL string format
		previousRev     string            // previous revisions in CBL string format
		proposedHLV     string            // proposed HLV in CBL string format
		expectedStatus  ProposedRevStatus // Expected status from CheckProposedVersion call
		expectedRev     string            // Expected rev from CheckProposedVersion call (for conflict cases)
	}{
		/*
			Tests for existing doc with cv only (1000@abc)
		*/
		{
			// already known, matches version
			name:            "exists same version",
			key:             "singleVersionDoc",
			proposedVersion: "1000@abc",
			previousRev:     "",
			proposedHLV:     "1000@abc",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// already known, older version
			name:            "exists older version",
			key:             "singleVersionDoc",
			proposedVersion: "900@abc",
			previousRev:     "",
			proposedHLV:     "1000@abc",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// conflict, HLV has same source but is older than current
			name:            "conflict HLV older",
			key:             "singleVersionDoc",
			proposedVersion: "1100@def",
			previousRev:     "900@abc",
			proposedHLV:     "1100@def;900@abc",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		{
			// conflict with cv only
			name:            "conflict cv only",
			key:             "singleVersionDoc",
			proposedVersion: "1000@def",
			previousRev:     "",
			proposedHLV:     "1000@def",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		{
			// conflict, HLV has previous versions but not cv.source
			name:            "conflict HLV no source",
			key:             "singleVersionDoc",
			proposedVersion: "1100@def",
			previousRev:     "1000@ghi",
			proposedHLV:     "1100@def;1000@ghi,900@jkl",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		{
			// ok, new version for same source
			name:            "ok, cv only, same source",
			key:             "singleVersionDoc",
			proposedVersion: "1100@abc",
			previousRev:     "",
			proposedHLV:     "1100@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// ok, new version for new source, current cv dominated by pv
			name:            "ok, new source",
			key:             "singleVersionDoc",
			proposedVersion: "1100@def",
			previousRev:     "1000@abc",
			proposedHLV:     "1100@def;1000@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// ok, previous rev not known but current cv dominated by pv
			name:            "ok, new source and unknown previous rev",
			key:             "singleVersionDoc",
			proposedVersion: "1200@def",
			previousRev:     "1100@ghi",
			proposedHLV:     "1200@def;1100@ghi,1000@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		/*
			Tests for existing doc with cv:1000@abc, pv:900@def
		*/
		{
			// ok, proposed HLV doesn't include server PV
			name:            "ok, new source dominates cv",
			key:             "multiVersionDoc",
			proposedVersion: "1100@abc",
			previousRev:     "1000@abc",
			proposedHLV:     "1100@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// ok, newer version for pv source, cv in HLV
			name:            "ok pv source",
			key:             "multiVersionDoc",
			proposedVersion: "1000@def",
			previousRev:     "1000@abc",
			proposedHLV:     "1000@def;1000@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// exists matches cv - previous in pv
			name:            "exists matches cv previous pv",
			key:             "multiVersionDoc",
			proposedVersion: "1000@abc",
			previousRev:     "900@def",
			proposedHLV:     "1000@abc;900@def",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// exists matches cv - previous same source
			name:            "exists matches cv same source",
			key:             "multiVersionDoc",
			proposedVersion: "1000@abc",
			previousRev:     "900@abc",
			proposedHLV:     "1000@abc;900@def",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// exists matches cv - previous same source
			name:            "exists pv dominates incoming",
			key:             "multiVersionDoc",
			proposedVersion: "900@def",
			previousRev:     "800@abc",
			proposedHLV:     "900@def;800@abc",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// exists matches cv - previous same source
			name:            "exists pv dominates incoming",
			key:             "multiVersionDoc",
			proposedVersion: "900@def",
			previousRev:     "800@def",
			proposedHLV:     "900@def",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// conflict with pv ignored because cv dominates
			name:            "ok pv conflict",
			key:             "multiVersionDoc",
			proposedVersion: "1100@abc",
			previousRev:     "1000@abc",
			proposedHLV:     "1100@abc;800@def",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
		{
			// conflict current cv not in incoming HLV history
			name:            "conflict pv",
			key:             "multiVersionDoc",
			proposedVersion: "1100@ghi",
			previousRev:     "900@abc",
			proposedHLV:     "1100@ghi;900@abc,800@def",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		{
			// conflict, proposed HLV doesn't include server cv
			name:            "conflict with cv",
			key:             "multiVersionDoc",
			proposedVersion: "1100@def",
			previousRev:     "900@def",
			proposedHLV:     "1100@def",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		{
			// conflict, proposed HLV doesn't include server cv
			name:            "conflict with cv and pv",
			key:             "multiVersionDoc",
			proposedVersion: "1000@def",
			previousRev:     "900@abc",
			proposedHLV:     "1000@def;900@abc",
			expectedStatus:  ProposedRev_Conflict,
			expectedRev:     "1000@abc",
		},
		/*
			Tests for existing doc with cv:1000@abc, mv:900@abc, 900@def; pv:900@ghi
		*/
		{
			// exists, mv is newer than proposed version for source
			name:            "exists based on mv",
			key:             "mergeVersionDoc",
			proposedVersion: "800@def",
			previousRev:     "700@def",
			proposedHLV:     "800@def;700@abc",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// exists, pv is newer than proposed version for source
			name:            "exists based on pv",
			key:             "mergeVersionDoc",
			proposedVersion: "800@ghi",
			previousRev:     "700@def",
			proposedHLV:     "800@ghi;700@def",
			expectedStatus:  ProposedRev_Exists,
			expectedRev:     "",
		},
		{
			// ok, proposed HLV doesn't include server PV
			name:            "ok, new source dominates cv",
			key:             "mergeVersionDoc",
			proposedVersion: "1100@abc",
			previousRev:     "1000@abc",
			proposedHLV:     "1100@abc",
			expectedStatus:  ProposedRev_OK,
			expectedRev:     "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.key+"-"+tc.name, func(t *testing.T) {
			// Test with previous rev present
			status, rev := collection.CheckProposedVersion(ctx, tc.key, tc.proposedVersion, tc.previousRev, tc.proposedHLV)
			assert.Equal(t, tc.expectedStatus, status, "expected status mismatch when previous rev present")
			assert.Equal(t, tc.expectedRev, rev, "expected rev mismatch when previous rev omitted")

			// Test with previous rev omitted
			status, rev = collection.CheckProposedVersion(ctx, tc.key, tc.proposedVersion, "", tc.proposedHLV)
			assert.Equal(t, tc.expectedStatus, status, "expected status mismatch when previous rev omitted")
			assert.Equal(t, tc.expectedRev, rev, "expected rev mismatch when previous rev omitted")
		})
	}
}

func incrementCas(cas uint64, delta int) (casOut uint64) {
	cas = cas + uint64(delta)
	return cas
}

func TestGetDeleted(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	body := Body{"key1": 1234}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Put")

	rev2id, _, err := collection.DeleteDoc(ctx, "doc1", DocVersion{RevTreeID: rev1id})
	assert.NoError(t, err, "DeleteDoc")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	expectedResult := Body{
		BodyId:        "doc1",
		BodyRev:       rev2id,
		BodyDeleted:   true,
		BodyRevisions: Revisions{RevisionsStart: 2, RevisionsIds: []string{"bc6d97f6e97c0d034a34f8aac2bf8b44", "dfd5e19813767eeddd08270fc5f385cd"}},
	}
	AssertEqualBodies(t, expectedResult, body)

	// Get the raw doc and make sure the sync data has the current revision
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Err getting doc")
	assert.Equal(t, rev2id, doc.SyncData.GetRevTreeID())

	// Try again but with a user who doesn't have access to this revision (see #179)
	authenticator := auth.NewAuthenticator(db.MetadataStore, db, db.AuthenticatorOptions(ctx))
	collection.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")
	collection.user.SetExplicitChannels(nil, 1)

	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	AssertEqualBodies(t, expectedResult, body)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAsUser(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)
	backingStoreMap := CreateTestSingleBackingStoreMap(collection, collection.GetCollectionID())

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, docRev1, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, docRev2, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStat := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses, db.DatabaseContext.DbStats.Cache().RevisionCacheNumItems, db.DatabaseContext.DbStats.Cache().RevisionCacheTotalMemory
	cacheOptions := &RevisionCacheOptions{
		MaxBytes:     0,
		MaxItemCount: DefaultRevisionCacheSize,
		ShardCount:   DefaultRevisionCacheShardCount,
	}
	collection.dbCtx.revisionCache = NewShardedLRURevisionCache(cacheOptions, backingStoreMap, cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStat)
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	cv := docRev2.HLV.GetCurrentVersionString()
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(cv)))
	assert.NoError(t, err, "Purge old revision JSON")

	// Try again with a user who doesn't have access to this revision
	authenticator := auth.NewAuthenticator(db.MetadataStore, db, db.AuthenticatorOptions(ctx))
	collection.user, err = authenticator.GetUser("")
	assert.NoError(t, err, "GetUser")

	var chans channels.TimedSet
	chans = channels.AtSequence(base.SetOf("ABC"), 1)
	collection.user.SetExplicitChannels(chans, 1)

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assertHTTPError(t, err, 404)
	assert.Nil(t, body)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	cv = docRev1.HLV.GetCurrentVersionString()
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(cv)))
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

// Test removal handling for unavailable multi-channel revisions.
func TestGetRemovalMultiChannel(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	auth := db.Authenticator(ctx)

	// Create a user who have access to both channel ABC and NBC.
	userAlice, err := auth.NewUser("alice", "pass", base.SetOf("ABC", "NBC"))
	require.NoError(t, err, "Error creating user")

	// Create a user who have access to channel NBC.
	userBob, err := auth.NewUser("bob", "pass", base.SetOf("NBC"))
	require.NoError(t, err, "Error creating user")

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Create the first revision of doc1.
	rev1Body := Body{
		"k1":       "v1",
		"channels": []string{"ABC", "NBC"},
	}
	rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
	require.NoError(t, err, "Error creating doc")

	// Create the second revision of doc1 on channel ABC as removal from channel NBC.
	rev2Body := Body{
		"k2":       "v2",
		"channels": []string{"ABC"},
		BodyRev:    rev1ID,
	}
	rev2ID, docRev2, err := collection.Put(ctx, "doc1", rev2Body)
	require.NoError(t, err, "Error creating doc")

	// Create the third revision of doc1 on channel ABC.
	rev3Body := Body{
		"k3":       "v3",
		"channels": []string{"ABC"},
		BodyRev:    rev2ID,
	}
	rev3ID, _, err := collection.Put(ctx, "doc1", rev3Body)
	require.NoError(t, err, "Error creating doc")
	require.NotEmpty(t, rev3ID, "Error creating doc")

	// Get rev2 of the doc as a user who have access to this revision.
	collection.user = userAlice
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	require.NoError(t, err, "Error getting 1x rev body")

	_, rev1Digest := ParseRevID(ctx, rev1ID)
	_, rev2Digest := ParseRevID(ctx, rev2ID)

	var interfaceListChannels []interface{}
	interfaceListChannels = append(interfaceListChannels, "ABC")
	bodyExpected := Body{
		"k2":       "v2",
		"channels": interfaceListChannels,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2Digest, rev1Digest},
		},
		BodyId:  "doc1",
		BodyRev: rev2ID,
	}
	require.Equal(t, bodyExpected, body)

	// Get rev2 of the doc as a user who doesn't have access to this revision.
	collection.user = userBob
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	require.NoError(t, err, "Error getting 1x rev body")
	bodyExpected = Body{
		BodyRemoved: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2Digest, rev1Digest},
		},
		BodyId:  "doc1",
		BodyRev: rev2ID,
	}
	require.Equal(t, bodyExpected, body)

	// Flush the revision cache and purge the old revision backup.
	db.FlushRevisionCacheForTest()
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(docRev2.HLV.GetCurrentVersionString())))
	require.NoError(t, err, "Error purging old revision JSON")

	// Try with a user who has access to this revision.
	collection.user = userAlice
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	assertHTTPError(t, err, 404)
	require.Nil(t, body)

	// Get rev2 of the doc as a user who doesn't have access to this revision.
	collection.user = userBob
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2ID, true, nil)
	assertHTTPError(t, err, 404)
	assert.Nil(t, body)
}

func TestDeltaSyncWhenFromRevIsLegacyRevTreeID(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync only supported in EE")
	}

	db, ctx := setupTestDB(t)
	db.Options.DeltaSyncOptions = DeltaSyncOptions{
		Enabled:          true,
		RevMaxAgeSeconds: 300,
	}
	db.Options.StoreLegacyRevTreeData = true

	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	require.NoError(t, db.DbStats.InitDeltaSyncStats())

	rev1, _, err := collection.Put(ctx, "doc1", Body{"foo": "bar", "bar": "buzz", "quux": "quax"})
	require.NoError(t, err, "Error creating doc1")
	rev2, _, err := collection.Put(ctx, "doc1", Body{"foo": "bar", "quux": "fuzz", BodyRev: rev1})
	require.NoError(t, err, "Error updating doc1")

	// force retrieval from backing store
	db.FlushRevisionCacheForTest()

	// get delta using legacy RevTree IDs - this should force a lookup for CV1 via the pointer backup revision doc
	delta, _, err := collection.GetDelta(ctx, "doc1", rev1, rev2)
	require.NoErrorf(t, err, "Error getting delta for doc %q from rev %q to %q", "doc1", rev1, rev2)
	require.NotNil(t, delta)
	assert.Equal(t, rev2, delta.ToRevID)
	assert.Equal(t, []byte(`{"bar":[],"quux":"fuzz"}`), delta.DeltaBytes)
}

// Test delta sync behavior when the fromRevision is a channel removal.
func TestDeltaSyncWhenFromRevIsChannelRemoval(t *testing.T) {
	testCases := []struct {
		name          string
		versionVector bool
	}{
		{
			name:          "revTree test",
			versionVector: false,
		},
		{
			name:          "versionVector test",
			versionVector: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

			// Create the first revision of doc1.
			rev1Body := Body{
				"k1":       "v1",
				"channels": []string{"ABC", "NBC"},
			}
			rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
			require.NoError(t, err, "Error creating doc")

			// Create the second revision of doc1 on channel ABC as removal from channel NBC.
			rev2Body := Body{
				"k2":       "v2",
				"channels": []string{"ABC"},
				BodyRev:    rev1ID,
			}
			rev2ID, docRev2, err := collection.Put(ctx, "doc1", rev2Body)
			require.NoError(t, err, "Error creating doc")

			// Create the third revision of doc1 on channel ABC.
			rev3Body := Body{
				"k3":       "v3",
				"channels": []string{"ABC"},
				BodyRev:    rev2ID,
			}
			rev3ID, docRev3, err := collection.Put(ctx, "doc1", rev3Body)
			require.NoError(t, err, "Error creating doc")
			require.NotEmpty(t, rev3ID, "Error creating doc")

			// Flush the revision cache and purge the old revision backup.
			db.FlushRevisionCacheForTest()
			if testCase.versionVector {
				cvStr := docRev2.HLV.GetCurrentVersionString()
				err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(cvStr)))
				require.NoError(t, err, "Error purging old revision JSON")
			} else {
				err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2ID)
				_ = err
				// TODO: CBG-4840 - Requires restoration of RevTree ID-based old revision storage
				//require.NoError(t, err, "Error purging old revision JSON")
			}

			// Request delta between rev2ID and rev3ID (toRevision "rev2ID" is channel removal)
			// as a user who doesn't have access to the removed revision via any other channel.
			authenticator := db.Authenticator(ctx)
			user, err := authenticator.NewUser("alice", "pass", base.SetOf("NBC"))
			require.NoError(t, err, "Error creating user")

			collection.user = user
			require.NoError(t, db.DbStats.InitDeltaSyncStats())

			if testCase.versionVector {
				rev2 := docRev2.HLV.ExtractCurrentVersionFromHLV()
				rev3 := docRev3.HLV.ExtractCurrentVersionFromHLV()
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2.String(), rev3.String())
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			} else {
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2ID, rev3ID)
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			}

			// Request delta between rev2ID and rev3ID (toRevision "rev2ID" is channel removal)
			// as a user who has access to the removed revision via another channel.
			user, err = authenticator.NewUser("bob", "pass", base.SetOf("ABC"))
			require.NoError(t, err, "Error creating user")

			collection.user = user
			require.NoError(t, db.DbStats.InitDeltaSyncStats())

			if testCase.versionVector {
				rev2 := docRev2.HLV.ExtractCurrentVersionFromHLV()
				rev3 := docRev3.HLV.ExtractCurrentVersionFromHLV()
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2.String(), rev3.String())
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			} else {
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2ID, rev3ID)
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			}
		})
	}
}

// Test delta sync behavior when the toRevision is a channel removal.
func TestDeltaSyncWhenToRevIsChannelRemoval(t *testing.T) {
	t.Skip("Pending work for channel removal at rev cache CBG-3814")
	testCases := []struct {
		name          string
		versionVector bool
	}{
		{
			name:          "revTree test",
			versionVector: false,
		},
		{
			name:          "versionVector test",
			versionVector: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

			// Create the first revision of doc1.
			rev1Body := Body{
				"k1":       "v1",
				"channels": []string{"ABC", "NBC"},
			}
			rev1ID, _, err := collection.Put(ctx, "doc1", rev1Body)
			require.NoError(t, err, "Error creating doc")

			// Create the second revision of doc1 on channel ABC as removal from channel NBC.
			rev2Body := Body{
				"k2":       "v2",
				"channels": []string{"ABC"},
				BodyRev:    rev1ID,
			}
			rev2ID, docRev2, err := collection.Put(ctx, "doc1", rev2Body)
			require.NoError(t, err, "Error creating doc")

			// Create the third revision of doc1 on channel ABC.
			rev3Body := Body{
				"k3":       "v3",
				"channels": []string{"ABC"},
				BodyRev:    rev2ID,
			}
			rev3ID, docRev3, err := collection.Put(ctx, "doc1", rev3Body)
			require.NoError(t, err, "Error creating doc")
			require.NotEmpty(t, rev3ID, "Error creating doc")

			// Flush the revision cache and purge the old revision backup.
			db.FlushRevisionCacheForTest()
			err = collection.PurgeOldRevisionJSON(ctx, "doc1", rev2ID)
			require.NoError(t, err, "Error purging old revision JSON")

			// Request delta between rev1ID and rev2ID (toRevision "rev2ID" is channel removal)
			// as a user who doesn't have access to the removed revision via any other channel.
			authenticator := db.Authenticator(ctx)
			user, err := authenticator.NewUser("alice", "pass", base.SetOf("NBC"))
			require.NoError(t, err, "Error creating user")

			collection.user = user
			require.NoError(t, db.DbStats.InitDeltaSyncStats())

			if testCase.versionVector {
				rev2 := docRev2.HLV.ExtractCurrentVersionFromHLV()
				rev3 := docRev3.HLV.ExtractCurrentVersionFromHLV()
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2.String(), rev3.String())
				require.NoError(t, err)
				assert.Nil(t, delta)
				assert.Equal(t, `{"_removed":true}`, string(redactedRev.BodyBytes))
			} else {
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev1ID, rev2ID)
				require.NoError(t, err)
				assert.Nil(t, delta)
				assert.Equal(t, `{"_removed":true}`, string(redactedRev.BodyBytes))
			}

			// Request delta between rev1ID and rev2ID (toRevision "rev2ID" is channel removal)
			// as a user who has access to the removed revision via another channel.
			user, err = authenticator.NewUser("bob", "pass", base.SetOf("ABC"))
			require.NoError(t, err, "Error creating user")

			collection.user = user
			require.NoError(t, db.DbStats.InitDeltaSyncStats())
			if testCase.versionVector {
				rev2 := docRev2.HLV.ExtractCurrentVersionFromHLV()
				rev3 := docRev3.HLV.ExtractCurrentVersionFromHLV()
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev2.String(), rev3.String())
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			} else {
				delta, redactedRev, err := collection.GetDelta(ctx, "doc1", rev1ID, rev2ID)
				require.Equal(t, base.HTTPErrorf(404, "missing"), err)
				assert.Nil(t, delta)
				assert.Nil(t, redactedRev)
			}
		})
	}
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemoved(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	backingStoreMap := CreateTestSingleBackingStoreMap(collection, collection.GetCollectionID())

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, docRev1, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRev:    rev1id,
	}
	rev2id, docRev2, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":     1234,
		"channels": []string{"NBC"},
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStat := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses, db.DatabaseContext.DbStats.Cache().RevisionCacheNumItems, db.DatabaseContext.DbStats.Cache().RevisionCacheTotalMemory
	cacheOptions := &RevisionCacheOptions{
		MaxBytes:     0,
		MaxItemCount: DefaultRevisionCacheSize,
		ShardCount:   DefaultRevisionCacheShardCount,
	}
	collection.dbCtx.revisionCache = NewShardedLRURevisionCache(cacheOptions, backingStoreMap, cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStat)
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(docRev2.HLV.GetCurrentVersionString())))
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the removal revision with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assertHTTPError(t, err, 404)
	require.Nil(t, body)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(docRev1.HLV.GetCurrentVersionString())))
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

// Test retrieval of a channel removal revision, when the revision is not otherwise available
func TestGetRemovedAndDeleted(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	backingStoreMap := CreateTestSingleBackingStoreMap(collection, collection.GetCollectionID())

	rev1body := Body{
		"key1":     1234,
		"channels": []string{"ABC"},
	}
	rev1id, docRev1, err := collection.Put(ctx, "doc1", rev1body)
	assert.NoError(t, err, "Put")

	rev2body := Body{
		"key1":      1234,
		BodyDeleted: true,
		BodyRev:     rev1id,
	}
	rev2id, docRev2, err := collection.Put(ctx, "doc1", rev2body)
	assert.NoError(t, err, "Put Rev 2")

	// Add another revision, so that rev 2 is obsolete
	rev3body := Body{
		"key1":     12345,
		"channels": []string{"NBC"},
		BodyRev:    rev2id,
	}
	_, _, err = collection.Put(ctx, "doc1", rev3body)
	assert.NoError(t, err, "Put Rev 3")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true, while still resident in the rev cache
	body, err := collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assert.NoError(t, err, "Get1xRevBody")
	rev2digest := rev2id[2:]
	rev1digest := rev1id[2:]
	expectedResult := Body{
		"key1":      1234,
		BodyDeleted: true,
		BodyRevisions: Revisions{
			RevisionsStart: 2,
			RevisionsIds:   []string{rev2digest, rev1digest}},
		BodyId:  "doc1",
		BodyRev: rev2id,
	}
	AssertEqualBodies(t, expectedResult, body)

	// Manually remove the temporary backup doc from the bucket
	// Manually flush the rev cache
	// After expiry from the rev cache and removal of doc backup, try again
	cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStats := db.DatabaseContext.DbStats.Cache().RevisionCacheHits, db.DatabaseContext.DbStats.Cache().RevisionCacheMisses, db.DatabaseContext.DbStats.Cache().RevisionCacheNumItems, db.DatabaseContext.DbStats.Cache().RevisionCacheTotalMemory
	cacheOptions := &RevisionCacheOptions{
		MaxBytes:     0,
		MaxItemCount: DefaultRevisionCacheSize,
		ShardCount:   DefaultRevisionCacheShardCount,
	}
	collection.dbCtx.revisionCache = NewShardedLRURevisionCache(cacheOptions, backingStoreMap, cacheHitCounter, cacheMissCounter, cacheNumItems, memoryCacheStats)
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(docRev2.HLV.GetCurrentVersionString())))
	assert.NoError(t, err, "Purge old revision JSON")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = collection.Get1xRevBody(ctx, "doc1", rev2id, true, nil)
	assertHTTPError(t, err, 404)
	require.Nil(t, body)

	// Ensure revision is unavailable for a non-leaf revision that isn't available via the rev cache, and wasn't a channel removal
	// TODO: CBG-4840 - Revs are backed only up by hash of CV (not legacy rev IDs) for non-delta sync cases
	err = collection.PurgeOldRevisionJSON(ctx, "doc1", base.Crc32cHashString([]byte(docRev1.HLV.GetCurrentVersionString())))
	assert.NoError(t, err, "Purge old revision JSON")

	_, err = collection.Get1xRevBody(ctx, "doc1", rev1id, true, nil)
	assertHTTPError(t, err, 404)
}

type AllDocsEntry struct {
	IDRevAndSequence
	Channels []string
}

func (e AllDocsEntry) Equal(e2 AllDocsEntry) bool {
	return e.DocID == e2.DocID && e.RevID == e2.RevID && e.Sequence == e2.Sequence &&
		base.SetFromArray(e.Channels).Equals(base.SetFromArray(e2.Channels))
}

var options ForEachDocIDOptions

func allDocIDs(ctx context.Context, collection *DatabaseCollection) (docs []AllDocsEntry, err error) {
	err = collection.ForEachDocID(ctx, func(doc IDRevAndSequence, channels []string) (bool, error) {
		docs = append(docs, AllDocsEntry{
			IDRevAndSequence: doc,
			Channels:         channels,
		})
		return true, nil
	}, options)
	return
}

func TestAllDocsOnly(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache)

	// Lower the log max length so no more than 50 items will be kept.
	cacheOptions := DefaultCacheOptions()
	cacheOptions.ChannelCacheMaxLength = 50

	db, ctx := setupTestDBWithCacheOptions(t, cacheOptions)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	collectionID := collection.GetCollectionID()

	// Trigger creation of the channel cache for channel "all"
	_, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, channels.NewID("all", collectionID))
	require.NoError(t, err)

	ids := make([]AllDocsEntry, 100)
	for i := 0; i < 100; i++ {
		channels := []string{"all"}
		if i%10 == 0 {
			channels = append(channels, "KFJC")
		}
		body := Body{"serialnumber": int64(i), "channels": channels}
		ids[i].DocID = fmt.Sprintf("alldoc-%02d", i)
		revid, _, err := collection.Put(ctx, ids[i].DocID, body)
		ids[i].RevID = revid
		ids[i].Sequence = uint64(i + 1)
		ids[i].Channels = channels
		assert.NoError(t, err, "Couldn't create document")
	}

	alldocs, err := allDocIDs(ctx, collection.DatabaseCollection)
	assert.NoError(t, err, "AllDocIDs failed")
	require.Len(t, alldocs, 100)
	for i, entry := range alldocs {
		assert.True(t, entry.Equal(ids[i]))
	}

	// Now delete one document and try again:
	_, _, err = collection.DeleteDoc(ctx, ids[23].DocID, DocVersion{RevTreeID: ids[23].RevID})
	assert.NoError(t, err, "Couldn't delete doc 23")

	alldocs, err = allDocIDs(ctx, collection.DatabaseCollection)
	assert.NoError(t, err, "AllDocIDs failed")
	require.Len(t, alldocs, 99)
	for i, entry := range alldocs {
		j := i
		if i >= 23 {
			j++
		}
		assert.True(t, entry.Equal(ids[j]))
	}

	// Inspect the channel log to confirm that it's only got the last 50 sequences.
	// There are 101 sequences overall, so the 1st one it has should be #52.
	err = db.changeCache.waitForSequence(ctx, 101, base.DefaultWaitForSequence)
	require.NoError(t, err)

	changeLog, err := collection.GetChangeLog(ctx, channels.NewID("all", collectionID), 0)
	require.NoError(t, err)
	require.Len(t, changeLog, 50)
	assert.Equal(t, "alldoc-51", changeLog[0].DocID)

	// Now check the changes feed:
	var options ChangesOptions
	changesCtx, changesCtxCancel := context.WithCancel(base.TestCtx(t))
	options.ChangesCtx = changesCtx
	defer changesCtxCancel()
	changes := getChanges(t, collection, channels.BaseSetOf(t, "all"), options)
	require.Len(t, changes, 100)

	for i, change := range changes {
		docIndex := i
		if i >= 23 {
			docIndex++
		}
		if i == len(changes)-1 {
			// The last entry in the changes response should be the deleted document
			assert.True(t, change.Deleted)
			assert.Equal(t, "alldoc-23", change.ID)
			assert.Equal(t, channels.BaseSetOf(t, "all"), change.Removed)
		} else {
			// Verify correct ordering for all other documents
			assert.Equal(t, fmt.Sprintf("alldoc-%02d", docIndex), change.ID)
		}
	}
	// Check whether sequences are ascending for all entries in the changes response
	sortedSeqAsc := func(changes []*ChangeEntry) bool {
		return sort.SliceIsSorted(changes, func(i, j int) bool {
			return changes[i].Seq.Seq < changes[j].Seq.Seq
		})
	}
	assert.True(t, sortedSeqAsc(changes), "Sequences should be ascending for all entries in the changes response")

	options.IncludeDocs = true
	changes = getChanges(t, collection, channels.BaseSetOf(t, "KFJC"), options)
	assert.Len(t, changes, 10)
	for i, change := range changes {
		assert.Equal(t, ids[10*i].DocID, change.ID)
		assert.False(t, change.Deleted)
		assert.Equal(t, base.Set(nil), change.Removed)
		var changeBody Body
		require.NoError(t, changeBody.Unmarshal(change.Doc))
		// unmarshalled as json.Number, so just compare the strings
		assert.Equal(t, strconv.FormatInt(int64(10*i), 10), changeBody["serialnumber"].(json.Number).String())
	}
	assert.True(t, sortedSeqAsc(changes), "Sequences should be ascending for all entries in the changes response")
}

// Unit test for bug #673
func TestUpdatePrincipal(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	// use default collection based on use of GetPrincipalForTest
	db, ctx := setupTestDBDefaultCollection(t)
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	assert.NoError(t, authenticator.Save(user))

	// Validate that a call to UpdatePrincipals with no changes to the user doesn't allocate a sequence
	userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
	require.NoError(t, err)
	userInfo.ExplicitChannels = base.SetOf("ABC")
	_, _, err = db.UpdatePrincipal(ctx, userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err := db.sequences.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), nextSeq)

	// Validate that a call to UpdatePrincipals with changes to the user does allocate a sequence
	userInfo, err = db.GetPrincipalForTest(t, "naomi", true)
	require.NoError(t, err)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, _, err = db.UpdatePrincipal(ctx, userInfo, true, true)
	assert.NoError(t, err, "Unable to update principal")

	nextSeq, err = db.sequences.nextSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), nextSeq)
}

func TestUpdatePrincipalCASRetry(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyCRUD)

	// ensure we don't batch sequences so that the number of released sequences is deterministic
	defer SuspendSequenceBatching()()

	tb := base.GetTestBucket(t)
	defer tb.Close(base.TestCtx(t))

	tests := []struct {
		numCASRetries int32
		expectError   bool
	}{
		{numCASRetries: 0},
		{numCASRetries: 1},
		{numCASRetries: 2},
		{numCASRetries: 5},
		{numCASRetries: 10},
		{numCASRetries: auth.PrincipalUpdateMaxCasRetries - 1},
		{numCASRetries: auth.PrincipalUpdateMaxCasRetries, expectError: true},
		{numCASRetries: auth.PrincipalUpdateMaxCasRetries + 1, expectError: true},
	}

	var (
		casRetryCount   atomic.Int32
		totalCASRetries atomic.Int32
		enableCASRetry  base.AtomicBool
	)

	lb := base.NewLeakyBucket(tb, base.LeakyBucketConfig{
		UpdateCallback: func(key string) {
			casRetryCountInt, totalCASRetriesInt := casRetryCount.Load(), totalCASRetries.Load()
			if enableCASRetry.IsTrue() && casRetryCountInt < totalCASRetriesInt {
				casRetryCount.Add(1)
				casRetryCountInt = casRetryCount.Load()
				t.Logf("foreceCASRetry %d/%d: Forcing CAS retry for key: %q", casRetryCountInt, totalCASRetriesInt, key)
				var body []byte
				originalCAS, err := tb.GetMetadataStore().Get(key, &body)
				require.NoError(t, err)
				err = tb.GetMetadataStore().Set(key, 0, nil, body)
				require.NoError(t, err)
				newCAS, err := tb.GetMetadataStore().Get(key, &body)
				require.NoError(t, err)
				t.Logf("foreceCASRetry %d/%d: Doc %q CAS changed from %d to %d", casRetryCountInt, totalCASRetriesInt, key, originalCAS, newCAS)
			}
		},
		IgnoreClose: true,
	})

	db, ctx := setupTestDBForBucket(t, lb)
	defer db.Close(ctx)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	for i, test := range tests {
		t.Run(fmt.Sprintf("numCASRetries=%d", test.numCASRetries), func(t *testing.T) {
			// Write an update that'll be forced into a CAS retry from the leaky bucket callback
			userInfo, err := db.GetPrincipalForTest(t, "naomi", true)
			require.NoError(t, err)
			userInfo.ExplicitChannels = base.SetOf("ABC", "PBS", fmt.Sprintf("testi:%d", i))

			// reset callback for subtest
			enableCASRetry.Set(true)
			casRetryCount.Store(0)
			totalCASRetries.Store(test.numCASRetries)
			sequenceReleasedCountBefore := db.sequences.dbStats.SequenceReleasedCount.Value()

			_, _, err = db.UpdatePrincipal(ctx, userInfo, true, true)
			if test.expectError {
				require.ErrorContains(t, err, "cas mismatch")
			} else {
				require.NoError(t, err, "Unable to update principal")
			}

			// cap to max retries if we're doing more
			expectedReleasedSequences := test.numCASRetries
			if test.numCASRetries > auth.PrincipalUpdateMaxCasRetries {
				expectedReleasedSequences = auth.PrincipalUpdateMaxCasRetries
			}

			// Ensure we released the sequences for all the CAS retries we expected to make
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				sequenceReleasedCountAfter := db.sequences.dbStats.SequenceReleasedCount.Value()
				assert.Equal(c, int64(expectedReleasedSequences), sequenceReleasedCountAfter-sequenceReleasedCountBefore)
			}, 5*time.Second, 100*time.Millisecond)
		})
	}
}

// Re-apply one of the conflicting changes to make sure that PutExistingRevWithBody() treats it as a no-op (SG Issue #3048)
func TestRepeatedConflict(t *testing.T) {

	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")

	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, newRev, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")

	// Get the _rev that was set in the body by PutExistingRevWithBody() and make assertions on it
	revGen, _ := ParseRevID(ctx, newRev)
	assert.Equal(t, 2, revGen)

	// Remove the _rev key from the body, and call PutExistingRevWithBody() again, which should re-add it
	delete(body, BodyRev)
	_, newRev, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err)

	// The _rev should pass the same assertions as before, since PutExistingRevWithBody() should re-add it
	revGen, _ = ParseRevID(ctx, newRev)
	assert.Equal(t, 2, revGen)

}

func TestConflicts(t *testing.T) {

	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Instantiate channel cache for channel 'all'
	collectionID := collection.GetCollectionID()

	allChannel := channels.NewID("all", collectionID)
	_, err := db.changeCache.getChannelCache().getSingleChannelCache(ctx, allChannel)
	require.NoError(t, err)

	cacheWaiter := db.NewDCPCachingCountWaiter(t)

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Wait for rev to be cached
	cacheWaiter.AddAndWait(1)

	changeLog, err := collection.GetChangeLog(ctx, channels.NewID("all", collectionID), 0)
	require.NoError(t, err)
	assert.Len(t, changeLog, 1)

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")

	cacheWaiter.Add(2)

	rawBody, _, _ := collection.dataStore.GetRaw("doc")

	log.Printf("got raw body: %s", rawBody)

	// Verify the change with the higher revid won:
	gotBody, err := collection.Get1xBody(ctx, "doc")
	require.NoError(t, err)
	expectedResult := Body{BodyId: "doc", BodyRev: "2-b", "n": 2, "channels": []string{"all", "2b"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify we can still get the other two revisions:
	gotBody, err = collection.Get1xRevBody(ctx, "doc", "1-a", false, nil)
	require.NoError(t, err)
	expectedResult = Body{BodyId: "doc", BodyRev: "1-a", "n": 1, "channels": []string{"all", "1"}}
	AssertEqualBodies(t, expectedResult, gotBody)
	gotBody, err = collection.Get1xRevBody(ctx, "doc", "2-a", false, nil)
	require.NoError(t, err)
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify the change-log of the "all" channel:
	cacheWaiter.Wait()
	changeLog, err = collection.GetChangeLog(ctx, allChannel, 0)
	require.NoError(t, err)
	assert.Len(t, changeLog, 1)
	assert.Equal(t, uint64(3), changeLog[0].Sequence)
	assert.Equal(t, "doc", changeLog[0].DocID)
	assert.Equal(t, "2-b", changeLog[0].RevID)
	assert.Equal(t, uint8(channels.Hidden|channels.Branched|channels.Conflict), changeLog[0].Flags)

	// Verify the _changes feed:
	options := ChangesOptions{
		Conflicts:  true,
		ChangesCtx: t.Context(),
	}

	changes := getChanges(t, collection, channels.BaseSetOf(t, "all"), options)

	assert.Len(t, changes, 1)
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 3},
		ID:           "doc",
		Changes:      []ChangeByVersionType{{ChangesVersionTypeRevTreeID: "2-b"}, {ChangesVersionTypeRevTreeID: "2-a"}},
		branched:     true,
		collectionID: collectionID,
	}, changes[0],
	)

	// Delete 2-b; verify this makes 2-a current:
	rev3, _, err := collection.DeleteDoc(ctx, "doc", DocVersion{RevTreeID: "2-b"})
	assert.NoError(t, err, "delete 2-b")

	rawBody, _, _ = collection.dataStore.GetRaw("doc")
	log.Printf("post-delete, got raw body: %s", rawBody)

	gotBody, err = collection.Get1xBody(ctx, "doc")
	require.NoError(t, err)
	expectedResult = Body{BodyId: "doc", BodyRev: "2-a", "n": 3, "channels": []string{"all", "2a"}}
	AssertEqualBodies(t, expectedResult, gotBody)

	// Verify channel assignments are correct for channels defined by 2-a:
	doc, _ := collection.GetDocument(ctx, "doc", DocUnmarshalAll)
	chan2a, found := doc.Channels["2a"]
	assert.True(t, found)
	assert.True(t, chan2a == nil)             // currently in 2a
	assert.True(t, doc.Channels["2b"] != nil) // has been removed from 2b

	// Wait for delete mutation to arrive over feed
	cacheWaiter.AddAndWait(1)

	// Verify the _changes feed:
	changes = getChanges(t, collection, channels.BaseSetOf(t, "all"), options)
	assert.Len(t, changes, 1)
	assert.Equal(t, &ChangeEntry{
		Seq:          SequenceID{Seq: 4},
		ID:           "doc",
		Changes:      []ChangeByVersionType{{ChangesVersionTypeRevTreeID: "2-a"}, {ChangesVersionTypeRevTreeID: rev3}},
		branched:     true,
		collectionID: collectionID,
	}, changes[0])

}

func TestConflictRevLimitDefault(t *testing.T) {

	// Test Default Is the higher of the two
	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
}

func TestConflictRevLimitAllowConflictsTrue(t *testing.T) {
	// Test AllowConflicts
	dbOptions := DatabaseContextOptions{
		AllowConflicts: base.Ptr(true),
	}

	db, ctx := SetupTestDBWithOptions(t, dbOptions)
	defer db.Close(ctx)
	assert.Equal(t, uint32(DefaultRevsLimitConflicts), db.RevsLimit)
}

func TestConflictRevLimitAllowConflictsFalse(t *testing.T) {
	dbOptions := DatabaseContextOptions{
		AllowConflicts: base.Ptr(false),
	}

	db, ctx := SetupTestDBWithOptions(t, dbOptions)
	defer db.Close(ctx)
	assert.Equal(t, uint32(DefaultRevsLimitNoConflicts), db.RevsLimit)
}

func TestNoConflictsMode(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	// Strictly speaking, this flag should be set before opening the database, but it only affects
	// Put operations and replication, so it doesn't make a difference if we do it afterwards.
	db.Options.AllowConflicts = base.Ptr(false)

	// Create revs 1 and 2 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")
	body["n"] = 2
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")

	// Try to create a conflict branching from rev 1:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no common ancestor:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-c", "1-c"}, false, ExistingVersionWithUpdateToHLV)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with a longer history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-d", "3-d", "2-d", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assertHTTPError(t, err, 409)

	// Try to create a conflict with no history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-e"}, false, ExistingVersionWithUpdateToHLV)
	assertHTTPError(t, err, 409)

	// Create a non-conflict with a longer history, ending in a deletion:
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 4-a")
	delete(body, BodyDeleted)

	// Try to resurrect the document with a conflicting branch
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"4-f", "3-a"}, false, ExistingVersionWithUpdateToHLV)
	assertHTTPError(t, err, 409)

	// Resurrect the tombstoned document with a disconnected branch):
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"1-f"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-f")

	// Tombstone the resurrected branch
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"2-f", "1-f"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-f")
	delete(body, BodyDeleted)

	// Resurrect the tombstoned document with a valid history (descendents of leaf)
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc", body, []string{"5-f", "4-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 5-f")
	delete(body, BodyDeleted)

	// Create a new document with a longer history:
	_, _, err = collection.PutExistingRevWithBody(ctx, "COD", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add COD")
	delete(body, BodyDeleted)

	// Now use Put instead of PutExistingRev:

	// Successfully add a new revision:
	_, _, err = collection.Put(ctx, "doc", Body{BodyRev: "5-f", "foo": -1})
	assert.NoError(t, err, "Put rev after 1-f")

	// Try to create a conflict:
	_, _, err = collection.Put(ctx, "doc", Body{BodyRev: "3-a", "foo": 7})
	assertHTTPError(t, err, 409)

	// Conflict with no ancestry:
	_, _, err = collection.Put(ctx, "doc", Body{"foo": 7})
	assertHTTPError(t, err, 409)
}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via Put
func TestAllowConflictsFalseTombstoneExistingConflict(t *testing.T) {
	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.Ptr(false)

	// Attempt to tombstone a non-leaf node of a conflicted document
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-c", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	tombstoneRev, _, putErr := collection.Put(ctx, "doc1", body)
	assert.NoError(t, putErr, "tombstone 2-a")
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.GetRevTreeID())

	// Attempt to add a tombstone rev w/ the previous tombstone as parent
	body[BodyRev] = tombstoneRev
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc1", body)
	assert.True(t, putErr != nil, "Expect error tombstoning a tombstone")

	// Tombstone the winning branch of a conflicted document
	body[BodyRev] = "2-b"
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc2", body)
	assert.NoError(t, putErr, "tombstone 2-b")
	doc, err = collection.GetDocument(ctx, "doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-a", doc.GetRevTreeID())

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	db.RevsLimit = uint32(1)
	body[BodyRev] = "2-a"
	body[BodyDeleted] = true
	_, _, putErr = collection.Put(ctx, "doc3", body)
	assert.NoError(t, putErr, "tombstone 2-a w/ revslimit=1")
	doc, err = collection.GetDocument(ctx, "doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.GetRevTreeID())

	log.Printf("tombstoned conflicts: %+v", doc)

}

// Test tombstoning of existing conflicts after AllowConflicts is set to false via PutExistingRev
func TestAllowConflictsFalseTombstoneExistingConflictNewEditsFalse(t *testing.T) {
	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create documents with multiple non-deleted branches
	log.Printf("Creating docs")
	body := Body{"n": 1}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create two conflicting changes:
	body["n"] = 2
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-b")
	body["n"] = 3
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")

	// Set AllowConflicts to false
	db.Options.AllowConflicts = base.Ptr(false)
	delete(body, "n")

	// Attempt to tombstone a non-leaf node of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-c", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.True(t, err != nil, "expected error tombstoning non-leaf")

	// Tombstone the non-winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.GetRevTreeID())

	// Tombstone the winning branch of a conflicted document
	body[BodyDeleted] = true
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc2", body, []string{"3-b", "2-b"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-b (tombstone)")
	doc, err = collection.GetDocument(ctx, "doc2", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-a", doc.GetRevTreeID())

	// Set revs_limit=1, then tombstone non-winning branch of a conflicted document.  Validate retrieval still works.
	body[BodyDeleted] = true
	db.RevsLimit = uint32(1)
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc3", body, []string{"3-a", "2-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-a (tombstone)")
	doc, err = collection.GetDocument(ctx, "doc3", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.GetRevTreeID())

	log.Printf("tombstoned conflicts: %+v", doc)
}

func TestSyncFnOnPush(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err := collection.UpdateSyncFun(ctx, `function(doc, oldDoc) {
		log("doc _id = "+doc._id+", _rev = "+doc._rev);
		if (oldDoc)
			log("oldDoc _id = "+oldDoc._id+", _rev = "+oldDoc._rev);
		channel(doc.channels);
	}`)
	require.NoError(t, err)

	// Create first revision:
	body := Body{"key1": "value1", "key2": 1234, "channels": []string{"public"}}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	// Add several revisions at once to a doc, as on a push:
	log.Printf("Check PutExistingRev...")
	body[BodyRev] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	body["channels"] = "clibup"
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		rev1id}
	newDoc, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, history, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "PutExistingRev failed")

	// Check that the doc has the correct channel (test for issue #300)
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, channels.ChannelMap{
		"clibup": nil,
		"public": &channels.ChannelRemoval{Seq: 2, Rev: channels.RevAndVersion{RevTreeID: "4-four", CurrentSource: newDoc.HLV.SourceID, CurrentVersion: base.CasToString(newDoc.HLV.Version)}},
	}, doc.Channels)

	// We no longer store channels for the winning revision in the RevTree,
	// so don't expect it to be in doc.History like it used to be...
	// The above assertion ensured the doc was *actually* in the correct channel.
	assert.Nil(t, doc.History["4-four"].Channels)
}

func TestInvalidChannel(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	body := Body{"channels": []string{"bad,name"}}
	_, _, err := collection.Put(ctx, "doc", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionValidation(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err := collection.UpdateSyncFun(ctx, `function(doc){access(doc.users,doc.userChannels);}`)
	require.NoError(t, err)

	body := Body{"users": []string{"username"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc2", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"bad,username"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc3", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"role:bad:rolename"}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc4", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{",.,.,.,.,."}, "userChannels": []string{"BBC1"}}
	_, _, err = collection.Put(ctx, "doc5", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"username"}, "userChannels": []string{"bad,name"}}
	_, _, err = collection.Put(ctx, "doc6", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionDb(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "Netflix"))
	require.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"animefan": channels.NewVbSimpleSequence(1), "tumblr": channels.NewVbSimpleSequence(1)}, 1)
	assert.NoError(t, authenticator.Save(user), "Save")

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err = collection.UpdateSyncFun(ctx, `function(doc){access(doc.users,doc.userChannels);}`)
	require.NoError(t, err)

	body := Body{"users": []string{"naomi"}, "userChannels": []string{"Hulu"}}
	_, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "")

	body = Body{"users": []string{"role:animefan"}, "userChannels": []string{"CrunchyRoll"}}
	_, _, err = collection.Put(ctx, "doc2", body)
	assert.NoError(t, err, "")

	// Create the role _after_ creating the documents, to make sure the previously-indexed access
	// privileges are applied.
	role, _ := authenticator.NewRole("animefan", nil)
	assert.NoError(t, authenticator.Save(role))

	user, err = authenticator.GetUser("naomi")
	assert.NoError(t, err, "GetUser")
	expected := channels.AtSequence(channels.BaseSetOf(t, "Hulu", "Netflix", "!"), 1)
	assert.Equal(t, expected, user.CollectionChannels(collection.ScopeName, collection.Name))

	expected.AddChannel("CrunchyRoll", 2)
	assert.Equal(t, expected, user.InheritedCollectionChannels(collection.ScopeName, collection.Name))
}

func TestDocIDs(t *testing.T) {
	tests := []struct {
		name  string
		docID string
		valid bool
	}{
		{name: "normal doc ID", docID: "foo", valid: true},
		{name: "non-prefix underscore", docID: "foo_", valid: true},

		{name: "spaces", docID: "foo bar", valid: true},
		{name: "symbols", docID: "foo!", valid: true},

		{name: "symbols (ASCII hex)", docID: "foo\x21", valid: true},
		{name: "control chars (NUL)", docID: "\x00foo", valid: true},
		{name: "control chars (BEL)", docID: "foo\x07", valid: true},

		{name: "empty", docID: ""}, // disallow empty doc IDs

		// disallow underscore prefixes
		{name: "underscore prefix", docID: "_"},
		{name: "underscore prefix", docID: "_foo"},
		{name: "underscore prefix", docID: "_design/foo"},
		{name: "underscore prefix", docID: base.RevPrefix + "x"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := ""
			if test.valid {
				expected = test.docID
			}
			assert.Equal(t, expected, realDocID(test.docID))
		})
	}
}

func TestUpdateDesignDoc(t *testing.T) {

	db, ctx := setupTestDBWithViewsEnabled(t)
	defer db.Close(ctx)

	mapFunction := `function (doc, meta) { emit(); }`
	err := db.PutDesignDoc(ctx, "official", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"TestView": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "add design doc as admin")

	// Validate retrieval of the design doc by admin
	var result sgbucket.DesignDoc
	result, err = db.GetDesignDoc("official")
	log.Printf("design doc: %+v", result)

	assert.NoError(t, err, "retrieve design doc as admin")
	retrievedView, ok := result.Views["TestView"]
	assert.True(t, ok)
	assert.True(t, strings.Contains(retrievedView.Map, "emit()"))
	assert.NotEqual(t, mapFunction, retrievedView.Map) // SG should wrap the map function, so they shouldn't be equal

	authenticator := auth.NewAuthenticator(db.MetadataStore, db, db.AuthenticatorOptions(ctx))
	db.user, _ = authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "Netflix"))
	err = db.PutDesignDoc(ctx, "_design/pwn3d", sgbucket.DesignDoc{})
	assertHTTPError(t, err, 403)
}

func TestPostWithExistingId(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	assert.True(t, rev1id != "")
	assert.True(t, docid == customDocId)

	// Test retrieval
	doc, err := collection.GetDocument(ctx, customDocId, DocUnmarshalAll)
	assert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that standard UUID creation still works:
	log.Printf("Create document with existing id...")
	body = Body{"notAnId": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err = collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	assert.True(t, rev1id != "")
	assert.True(t, docid != customDocId)

	// Test retrieval
	doc, err = collection.GetDocument(ctx, docid, DocUnmarshalAll)
	assert.True(t, doc != nil)
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")

}

// Unit test for issue #976
func TestWithNullPropertyKey(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Test creating a document with null property key
	customDocId := "customIdValue"
	log.Printf("Create document with empty property key")
	body := Body{BodyId: customDocId, "": "value1"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err)
	assert.True(t, rev1id != "")
	assert.True(t, docid != "")
}

// Unit test for issue #507, modified for CBG-1995 (allowing special properties)
func TestPostWithUserSpecialProperty(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{BodyId: customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, _, err := collection.Post(ctx, body)
	require.NoError(t, err, "Couldn't create document")
	require.NotEqual(t, "", rev1id)
	require.Equal(t, customDocId, docid)

	// Test retrieval
	doc, err := collection.GetDocument(ctx, customDocId, DocUnmarshalAll)
	require.NotNil(t, doc)
	assert.NoError(t, err, "Unable to retrieve doc using custom id")

	// Test that posting an update with a user special property does update the document
	log.Printf("Update document with existing id...")
	body = Body{BodyId: customDocId, BodyRev: rev1id, "_special": "value", "key1": "value1", "key2": "existing"}
	rev2id, _, err := collection.Put(ctx, docid, body)
	assert.NoError(t, err)

	// Test retrieval gets rev2
	doc, err = collection.GetDocument(ctx, docid, DocUnmarshalAll)
	require.NotNil(t, doc)
	assert.Equal(t, rev2id, doc.GetRevTreeID())
	assert.Equal(t, "value", doc.Body(ctx)["_special"])
	assert.NoError(t, err, "Unable to retrieve doc using generated uuid")
}

func TestRecentSequenceHandlingForSkippedSequences(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it writes directly to the xattr")
	}
	defer SuspendSequenceBatching()() // turn off sequence batching to avoid unused sequence(s) being released

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	opts := DefaultCacheOptions()
	opts.CachePendingSeqMaxNum = 1
	opts.CachePendingSeqMaxWait = 10 * time.Nanosecond
	db, ctx := setupTestDBWithCacheOptions(t, opts)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := t.Name() + "_doc1"
	docID2 := t.Name() + "_doc2"

	// add a couple of docs and wait for them to be cached
	body := Body{"val": "one"}
	_, _, err := collection.Put(ctx, docID, body)
	require.NoError(t, err)
	_, _, err = collection.Put(ctx, docID2, body)
	require.NoError(t, err)
	err = db.changeCache.waitForSequence(ctx, 2, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// grab doc and alter sync data of one to artificially create gap in sequences at cache
	xattrs, cas, err := collection.dataStore.GetXattrs(ctx, docID, []string{base.SyncXattrName})
	require.NoError(t, err)
	var retrievedXattr map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(6)
	retrievedXattr["recent_sequences"] = []uint64{1, 6}
	newXattrVal := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}
	_, err = collection.dataStore.UpdateXattrs(ctx, docID, 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// assert that sequence 6 is seen over caching feed
	err = db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence)
	require.NoError(t, err)
	// assert that skipped is filled + stable sequence and high sequence is as expected
	require.NoError(t, db.changeCache.InsertPendingEntries(ctx)) // empty pending
	db.UpdateCalculatedStats(ctx)
	assert.Equal(t, int64(3), db.DbStats.Cache().NumCurrentSeqsSkipped.Value())
	assert.Equal(t, int64(0), db.DbStats.Cache().PendingSeqLen.Value())
	assert.Equal(t, int64(6), db.DbStats.Cache().HighSeqCached.Value())
	assert.Equal(t, int64(2), db.DbStats.Cache().HighSeqStable.Value())
	assert.Equal(t, uint64(7), db.changeCache.getNextSequence())

	// alter sync data on doc2 to create recent sequence history to plug gap in sequences that have been pushed to skipped
	xattrs, cas, err = collection.dataStore.GetXattrs(ctx, docID2, []string{base.SyncXattrName})
	require.NoError(t, err)
	retrievedXattr = map[string]interface{}{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(5)
	retrievedXattr["recent_sequences"] = []uint64{2, 3, 4, 5}
	newXattrVal = map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}
	_, err = collection.dataStore.UpdateXattrs(ctx, docID2, 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// assert that skipped emptied + stable sequence is moved to high seq cached
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		db.UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(0), db.DbStats.Cache().NumCurrentSeqsSkipped.Value())
	}, time.Second*10, time.Millisecond*100)
	assert.Equal(t, int64(0), db.DbStats.Cache().PendingSeqLen.Value())
	highCachedSeq := db.DbStats.Cache().HighSeqCached.Value()
	assert.Equal(t, highCachedSeq, db.DbStats.Cache().HighSeqStable.Value())
}

func TestRecentSequenceHandlingForDeduplication(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it writes directly to the xattr")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	docID := t.Name()

	// add a doc and wait for it to be cached
	body := Body{"val": "one"}
	_, _, err := collection.Put(ctx, docID, body)
	require.NoError(t, err)
	err = db.changeCache.waitForSequence(ctx, 1, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// grab doc and alter sync data as if it had been rapidly updated and deduplicated over dcp
	xattrs, cas, err := collection.dataStore.GetXattrs(ctx, docID, []string{base.SyncXattrName})
	require.NoError(t, err)

	var retrievedXattr map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(6)
	retrievedXattr["recent_sequences"] = []uint64{1, 2, 3, 4, 5, 6}
	newXattrVal := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}
	_, err = collection.dataStore.UpdateXattrs(ctx, docID, 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// assert that sequence 6 is seen over caching feed, no pending changes or skipped changes
	err = db.changeCache.waitForSequence(ctx, 6, base.DefaultWaitForSequence)
	require.NoError(t, err)
	db.UpdateCalculatedStats(ctx)
	assert.Equal(t, int64(0), db.DbStats.Cache().NumCurrentSeqsSkipped.Value())
	assert.Equal(t, int64(0), db.DbStats.Cache().PendingSeqLen.Value())
}

func TestRecentSequenceHistory(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	seqTracker := uint64(0)

	// Validate recent sequence is written
	body := Body{"val": "one"}
	revid, doc, err := collection.Put(ctx, "doc1", body)
	require.NoError(t, err)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++

	expectedRecent := make([]uint64, 0)
	assert.True(t, revid != "")
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	expectedRecent = append(expectedRecent, seqTracker)
	assert.True(t, err == nil)
	assert.Equal(t, expectedRecent, doc.RecentSequences)

	// Add up to kMaxRecentSequences revisions - validate they are retained when total is less than max
	for i := 1; i < kMaxRecentSequences; i++ {
		revid, doc, err = collection.Put(ctx, "doc1", body)
		require.NoError(t, err)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		expectedRecent = append(expectedRecent, seqTracker)
	}

	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	assert.Equal(t, expectedRecent, doc.RecentSequences)

	// Recent sequence pruning only prunes entries older than what's been seen over DCP
	// (to ensure it's not pruning something that may still be coalesced).  Because of this, test waits
	// for caching before attempting to trigger pruning.
	err = db.changeCache.waitForSequence(ctx, seqTracker, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// Add another sequence to validate pruning when past max (20)
	revid, doc, err = collection.Put(ctx, "doc1", body)
	require.NoError(t, err)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	seqTracker++
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	log.Printf("recent:%d, max:%d", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

	// Ensure pruning works when sequences aren't sequential
	doc2Body := Body{"val": "two"}
	for i := 0; i < kMaxRecentSequences; i++ {
		revid, doc, err = collection.Put(ctx, "doc1", body)
		require.NoError(t, err)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		seqTracker++
		revid, doc, err = collection.Put(ctx, "doc2", doc2Body)
		require.NoError(t, err)
		doc2Body[BodyId] = doc.ID
		doc2Body[BodyRev] = revid
		seqTracker++
	}

	err = db.changeCache.waitForSequence(ctx, seqTracker, base.DefaultWaitForSequence) //
	require.NoError(t, err)
	revid, doc, err = collection.Put(ctx, "doc1", body)
	require.NoError(t, err)
	body[BodyId] = doc.ID
	body[BodyRev] = revid
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.True(t, err == nil)
	log.Printf("Recent sequences: %v (shouldn't exceed %v)", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

}

func TestMaintainMinimumRecentSequences(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	const docID = "doc1"
	allocSeq := uint64(0)

	// Add 20 revisions of a single document to fill recent sequences up on the document
	body := Body{"val": "one"}
	for i := 0; i < 20; i++ {
		revid, doc, err := collection.Put(ctx, docID, body)
		require.NoError(t, err)
		body[BodyId] = doc.ID
		body[BodyRev] = revid
		allocSeq++
	}
	// wait for the latest allocated seq to arrive at cache to move stable seq in place for recent sequence compaction
	err := db.changeCache.waitForSequence(ctx, allocSeq, base.DefaultWaitForSequence)
	require.NoError(t, err)

	// assert that we have 20 entries in recent sequences for the above doc updates
	doc, err := collection.GetDocument(ctx, docID, DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, 20, len(doc.RecentSequences))

	// update the original doc to trigger recent sequence compaction on the doc
	_, _, err = collection.Put(ctx, docID, body)
	require.NoError(t, err)

	// Validate that the recent sequences are pruned to the minimum + recently assigned sequence
	doc, err = collection.GetDocument(ctx, docID, DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, 6, len(doc.RecentSequences))
}

func TestChannelView(t *testing.T) {

	db, ctx := setupTestDBWithViewsEnabled(t)
	defer db.Close(ctx)
	collection, err := db.GetDefaultDatabaseCollectionWithUser()
	require.NoError(t, err)
	ctx = collection.AddCollectionContext(ctx)

	// Create doc
	log.Printf("Create doc 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")
	assert.Equal(t, "1-cb0c9a22be0e5a1b01084ec019defa81", rev1id)

	var entries LogEntries
	// Query view (retry loop to wait for indexing)
	for i := 0; i < 10; i++ {
		var err error
		entries, err = collection.getChangesInChannelFromQuery(ctx, "*", 0, 100, 0, false)

		assert.NoError(t, err, "Couldn't create document")
		if len(entries) >= 1 {
			break
		}
		log.Printf("No entries found - retrying (%d/10)", i+1)
		time.Sleep(500 * time.Millisecond)
	}

	for i, entry := range entries {
		log.Printf("View Query returned entry (%d): %v", i, entry)
	}
	assert.Len(t, entries, 1)
	require.Equal(t, "doc1", entries[0].DocID)
	collection.RequireCurrentVersion(t, "doc1", entries[0].SourceID, entries[0].Version)
}

func TestChannelQuery(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	_, err := collection.UpdateSyncFun(ctx, `function(doc, oldDoc) {
		channel(doc.channels);
	}`)
	require.NoError(t, err)

	// Create doc
	body := Body{"key1": "value1", "key2": 1234, "channels": "ABC"}
	rev1ID, _, err := collection.Put(ctx, "doc1", body)
	require.NoError(t, err, "Couldn't create doc1")

	// Create a doc to test removal handling.  Needs three revisions so that the removal rev (2) isn't
	// the current revision
	removedDocID := "removed_doc"
	removedDocRev1, rev1, err := collection.Put(ctx, removedDocID, body)
	require.NoError(t, err, "Couldn't create removed_doc")

	updatedChannelBody := Body{"_rev": removedDocRev1, "key1": "value1", "key2": 1234, "channels": "DEF"}
	removalRev, rev2, err := collection.Put(ctx, removedDocID, updatedChannelBody)
	require.NoError(t, err, "Couldn't update removed_doc")

	updatedChannelBody = Body{"_rev": removalRev, "key1": "value1", "key2": 2345, "channels": "DEF"}
	_, rev3, err := collection.Put(ctx, removedDocID, updatedChannelBody)
	require.NoError(t, err, "Couldn't update removed_doc")

	log.Printf("versions: [%v %v %v]", rev1.HLV.Version, rev2.HLV.Version, rev3.HLV.Version)

	// TODO: check the case where the channel is removed with a putExistingRev mutation

	var entries LogEntries

	// Test query retrieval via star channel and named channel (queries use different indexes)
	testCases := []struct {
		testName    string
		channelName string
		expectedRev channels.RevAndVersion
	}{
		{
			testName:    "star channel",
			channelName: "*",
			expectedRev: rev3.RevAndVersion,
		},
		{
			testName:    "named channel",
			channelName: "ABC",
			expectedRev: rev2.RevAndVersion,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			entries, err = collection.getChangesInChannelFromQuery(ctx, testCase.channelName, 0, 100, 0, false)
			require.NoError(t, err)

			for i, entry := range entries {
				log.Printf("Channel Query returned entry (%d): %v", i, entry)
			}
			require.Len(t, entries, 2)
			require.Equal(t, "doc1", entries[0].DocID)
			require.Equal(t, rev1ID, entries[0].RevID)
			collection.RequireCurrentVersion(t, "doc1", entries[0].SourceID, entries[0].Version)

			removedDocEntry := entries[1]
			require.Equal(t, removedDocID, removedDocEntry.DocID)

			log.Printf("expectedRev: %#v", testCase.expectedRev)
			log.Printf("removedDocEntry Version: %v", removedDocEntry.Version)
			require.Equal(t, testCase.expectedRev.RevTreeID, removedDocEntry.RevID)
			require.Equal(t, testCase.expectedRev.CurrentSource, removedDocEntry.SourceID)
			require.Equal(t, base.HexCasToUint64(testCase.expectedRev.CurrentVersion), removedDocEntry.Version)
		})
	}

}

// TestChannelQueryRevocation ensures that the correct rev (revTreeID and cv) is returned by the channel query.
func TestChannelQueryRevocation(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	_, err := collection.UpdateSyncFun(ctx, `function(doc, oldDoc) {
		channel(doc.channels);
	}`)
	require.NoError(t, err)

	// Create doc with three channels (ABC, DEF, GHI)
	docID := "removalTestDoc"
	body := Body{"key1": "value1", "key2": 1234, "channels": []string{"ABC", "DEF", "GHI"}}
	rev1ID, _, err := collection.Put(ctx, docID, body)
	require.NoError(t, err, "Couldn't create document")

	// Update the doc with a simple PUT to remove channel ABC
	updatedChannelBody := Body{"_rev": rev1ID, "key1": "value1", "key2": 1234, "channels": []string{"DEF", "GHI"}}
	_, rev2, err := collection.Put(ctx, docID, updatedChannelBody)
	require.NoError(t, err, "Couldn't update document via Put")

	// Update the doc with PutExistingCurrentVersion to remove channel DEF
	/* TODO: requires fix to HLV conflict detection
	updatedChannelBody = Body{"_rev": rev2ID, "key1": "value1", "key2": 2345, "channels": "GHI"}
	existingDoc, err := collection.GetDocument(ctx, docID, DocUnmarshalAll)
	require.NoError(t, err)
	cblVersion := Version{SourceID: "CBLSource", Value: existingDoc.HLV.Version + 10}
	hlvErr := existingDoc.HLV.AddVersion(cblVersion)
	require.NoError(t, hlvErr)
	existingDoc.UpdateBody(updatedChannelBody)
	rev3, _, _, err := collection.PutExistingCurrentVersion(ctx, existingDoc, *existingDoc.HLV, nil)
	require.NoError(t, err, "Couldn't update document via PutExistingCurrentVersion")

	*/

	var entries LogEntries

	// Test query retrieval via star channel and named channel (queries use different indexes)
	testCases := []struct {
		testName    string
		channelName string
		expectedRev channels.RevAndVersion
	}{
		{
			testName:    "removal by SGW write",
			channelName: "ABC",
			expectedRev: rev2.RevAndVersion,
		},
		/*
			{
				testName:    "removal by CBL write",
				channelName: "DEF",
				expectedRev: rev3.RevAndVersion,
			},
		*/
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			entries, err = collection.getChangesInChannelFromQuery(ctx, testCase.channelName, 0, 100, 0, false)
			require.NoError(t, err)

			for i, entry := range entries {
				log.Printf("Channel Query returned entry (%d): %v", i, entry)
			}
			require.Len(t, entries, 1)
			removedDocEntry := entries[0]
			require.Equal(t, docID, removedDocEntry.DocID)

			log.Printf("removedDocEntry Version: %v", removedDocEntry.Version)
			require.Equal(t, testCase.expectedRev.RevTreeID, removedDocEntry.RevID)
			require.Equal(t, testCase.expectedRev.CurrentSource, removedDocEntry.SourceID)
			require.Equal(t, base.HexCasToUint64(testCase.expectedRev.CurrentVersion), removedDocEntry.Version)
		})
	}

}

// ////// XATTR specific tests.  These tests current require setting DefaultUseXattrs=true, and must be run against a Couchbase bucket

func TestConcurrentImport(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("Test only works with XATTRS")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyImport)

	// Add doc to the underlying bucket:
	_, err := collection.dataStore.Add("directWrite", 0, Body{"value": "hi"})
	require.NoError(t, err)

	// Attempt concurrent retrieval of the docs, and validate that they are only imported once (based on revid)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doc, err := collection.GetDocument(ctx, "directWrite", DocUnmarshalAll)
			assert.True(t, doc != nil)
			assert.NoError(t, err, "Document retrieval error")
			assert.Equal(t, "1-36fa688dc2a2c39a952ddce46ab53d12", doc.SyncData.GetRevTreeID())
		}()
	}
	wg.Wait()
}

// ////// BENCHMARKS

func BenchmarkDatabase(b *testing.B) {
	base.DisableTestLogging(b)

	for i := 0; i < b.N; i++ {
		ctx := base.TestCtx(b)
		bucket, _ := ConnectToBucket(ctx, base.BucketSpec{
			Server:     base.UnitTestUrl(),
			BucketName: fmt.Sprintf("b-%d", i)},
			true)
		dbCtx, _ := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{})
		db, _ := CreateDatabase(dbCtx)
		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

		body := Body{"key1": "value1", "key2": 1234}
		_, _, _ = collection.Put(ctx, fmt.Sprintf("doc%d", i), body)

		db.Close(ctx)
	}
}

func BenchmarkPut(b *testing.B) {
	base.DisableTestLogging(b)

	ctx := base.TestCtx(b)
	bucket, _ := ConnectToBucket(ctx, base.BucketSpec{
		Server:     base.UnitTestUrl(),
		BucketName: "Bucket"},
		true)
	context, _ := NewDatabaseContext(ctx, "db", bucket, false, DatabaseContextOptions{})
	db, _ := CreateDatabase(context)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

	body := Body{"key1": "value1", "key2": 1234}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = collection.Put(ctx, fmt.Sprintf("doc%d", i), body)
	}

	db.Close(ctx)
}

var (
	defaultProvider      = "Google"
	clientID             = "CouchbaseSynGatewayDev"
	callbackURL          = "https://127.0.0.1:4985/_callback"
	callbackURLWithQuery = "https://127.0.0.1:4985/_callback?"
	validationKey        = "some.validation.key"
)

func mockOIDCProvider() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.google.com",
			ClientID: base.Ptr(clientID),
		},
		Name:          "Google",
		CallbackURL:   &callbackURL,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithCallbackURLQuery() auth.OIDCProvider {
	return auth.OIDCProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.google.com",
			ClientID: base.Ptr(clientID),
		},
		Name:          "Google",
		CallbackURL:   &callbackURLWithQuery,
		ValidationKey: &validationKey,
	}
}

func mockOIDCProviderWithNoValidationKey() auth.OIDCProvider {
	return auth.OIDCProvider{
		Name:        "Yahoo",
		CallbackURL: &callbackURL,
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:   "https://accounts.yahoo.com",
			ClientID: base.Ptr(clientID),
		},
	}
}

func mockOIDCOptions() *auth.OIDCOptions {
	provider := mockOIDCProvider()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func mockOIDCOptionsWithMultipleProviders() *auth.OIDCOptions {
	differentProvider := "Couchbase"
	provider1 := mockOIDCProvider()
	provider2 := mockOIDCProvider()
	providers := auth.OIDCProviderMap{provider1.Name: &provider1}
	provider2.Name = "Youtube"
	providers[provider2.Name] = &provider2
	return &auth.OIDCOptions{DefaultProvider: &differentProvider, Providers: providers}
}

func mockOIDCOptionsWithMultipleProvidersCBQ() *auth.OIDCOptions {
	differentProvider := "SalesForce"
	provider1 := mockOIDCProvider()
	provider2 := mockOIDCProviderWithCallbackURLQuery()
	providers := auth.OIDCProviderMap{provider1.Name: &provider1}
	provider2.Name = "Youtube"
	providers[provider2.Name] = &provider2
	return &auth.OIDCOptions{DefaultProvider: &differentProvider, Providers: providers}
}

func mockOIDCOptionsWithNoValidationKey() *auth.OIDCOptions {
	provider := mockOIDCProviderWithNoValidationKey()
	providers := auth.OIDCProviderMap{provider.Name: &provider}
	return &auth.OIDCOptions{DefaultProvider: &defaultProvider, Providers: providers}
}

func TestNewDatabaseContextWithOIDCProviderOptions(t *testing.T) {
	tests := []struct {
		name          string
		inputOptions  *auth.OIDCOptions
		expectedError string
	}{
		// Mock valid OIDCOptions
		{
			name:          "TestWithValidOptions",
			inputOptions:  mockOIDCOptions(),
			expectedError: "",
		},
		// If Validation Key not defined in config for provider, it should warn auth code flow will not be
		// supported for this provider.
		{
			name:          "TestWithNoValidationKey",
			inputOptions:  mockOIDCOptionsWithNoValidationKey(),
			expectedError: "",
		},

		// Input to simulate the scenario where the current provider is the default provider, or there's are
		// providers defined, don't set IsDefault.
		{
			name:          "TestWithMultipleProviders",
			inputOptions:  mockOIDCOptionsWithMultipleProviders(),
			expectedError: "",
		},
		// Input to simulate the scenario where the current provider isn't the default provider, add the provider
		// to the callback URL (needed to identify provider to _oidc_callback)
		{
			name:          "TestWithMultipleProvidersAndCustomCallbackURL",
			inputOptions:  mockOIDCOptionsWithMultipleProvidersCBQ(),
			expectedError: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tBucket := base.GetTestBucket(t)
			options := DatabaseContextOptions{
				OIDCOptions: tc.inputOptions,
				Scopes:      GetScopesOptions(t, tBucket, 1),
			}
			AddOptionsFromEnvironmentVariables(&options)

			ctx := base.TestCtx(t)
			dbCtx, err := NewDatabaseContext(ctx, "db", tBucket, false, options)
			assert.NoError(t, err, "Couldn't create context for database 'db'")
			defer dbCtx.Close(ctx)
			assert.NotNil(t, dbCtx, "Database context should be created")

			database, err := CreateDatabase(dbCtx)
			assert.NotNil(t, database, "Database should be created with context options")
			assert.NoError(t, err, "Couldn't create database 'db'")
		})
	}
}

func TestGetOIDCProvider(t *testing.T) {
	options := DatabaseContextOptions{OIDCOptions: mockOIDCOptions()}
	context, ctx := SetupTestDBWithOptions(t, options)
	defer context.Close(ctx)

	// Lookup default provider by empty name, which exists in database context
	provider, err := context.GetOIDCProvider("")
	require.NoError(t, err)
	assert.Equal(t, defaultProvider, provider.Name)
	assert.Equal(t, defaultProvider, *options.OIDCOptions.DefaultProvider)
	log.Printf("%v", provider)

	// Lookup a provider by name which exists in database context.
	provider, err = context.GetOIDCProvider(defaultProvider)
	require.NoError(t, err)
	assert.Equal(t, defaultProvider, provider.Name)
	assert.Equal(t, defaultProvider, *options.OIDCOptions.DefaultProvider)
	log.Printf("%v", provider)

	// Lookup a provider which doesn't exists in database context
	provider, err = context.GetOIDCProvider("Unknown")
	assert.Nil(t, provider, "Provider doesn't exists in database context")
	assert.Contains(t, err.Error(), `No provider found for provider name "Unknown"`)
}

// TestSyncFnMutateBody ensures that any mutations made to the body by the sync function aren't persisted
func TestSyncFnMutateBody(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	_, err := collection.UpdateSyncFun(ctx, `function(doc, oldDoc) {
		doc.key1 = "mutatedValue"
		doc.key2.subkey1 = "mutatedSubValue"
		channel(doc.channels);
	}`)
	require.NoError(t, err)

	// Create first revision:
	body := Body{"key1": "value1", "key2": Body{"subkey1": "subvalue1"}, "channels": []string{"public"}}
	rev1id, _, err := collection.Put(ctx, "doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	rev, err := collection.GetRev(ctx, "doc1", rev1id, false, nil)
	require.NoError(t, err)
	revBody, err := rev.Body()
	require.NoError(t, err, "Couldn't get mutable body")
	assert.Equal(t, "value1", revBody["key1"])
	assert.Equal(t, map[string]interface{}{"subkey1": "subvalue1"}, revBody["key2"])
	log.Printf("rev: %s", rev.BodyBytes)

}

// Multiple clients are attempting to push the same new revision concurrently; first writer should be successful,
// subsequent writers should fail on CAS, and then identify that revision already exists on retry.
func TestConcurrentPushSameNewRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	var revId string
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			body := Body{"name": "Bob", "age": 52}
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			revId, _, err := collection.Put(ctx, "doc1", body)
			assert.NoError(t, err, "Couldn't create document")
			assert.NotEmpty(t, revId)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	enableCallback = true

	body := Body{"name": "Bob", "age": 52}
	_, _, err := collection.Put(ctx, "doc1", body)
	require.Error(t, err)
	assert.Equal(t, "409 Document exists", err.Error())

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.Equal(t, revId, doc.RevID)
	assert.NoError(t, err, "Couldn't retrieve document")
	assert.Equal(t, "Bob", doc.Body(ctx)["name"])
	assert.Equal(t, json.Number("52"), doc.Body(ctx)["age"])
}

// Multiple clients are attempting to push the same new, non-winning revision concurrently; non-winning is an
// update to a non-winning branch that leaves the branch still non-winning (i.e. shorter) than the active branch
func TestConcurrentPushSameNewNonWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	ctx := base.TestCtx(t)

	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)

	leakyBucket := base.NewLeakyBucket(bucket, base.LeakyBucketConfig{
		UpdateCallback: func(key string) {
			if enableCallback {
				enableCallback = false
				body := Body{"name": "Emily", "age": 20}
				collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
				assert.NoError(t, err, "Adding revision 3-b")
			}
		},
	})

	db, ctx = SetupTestDBForBucketWithOptions(t, leakyBucket, DatabaseContextOptions{AllowConflicts: base.Ptr(true)})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Emily", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 3-b")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.GetRevTreeID())
}

// Multiple clients are attempting to push the same tombstone of the winning revision for a branched document
// First writer should be successful, subsequent writers should fail on CAS, then identify rev already exists
func TestConcurrentPushSameTombstoneWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	leakyBucket := base.NewLeakyBucket(bucket, base.LeakyBucketConfig{
		UpdateCallback: func(key string) {
			if enableCallback {
				enableCallback = false
				body := Body{"name": "Charlie", "age": 10, BodyDeleted: true}
				collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
				assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")
			}
		},
	})
	db, ctx = SetupTestDBForBucketWithOptions(t, leakyBucket, DatabaseContextOptions{AllowConflicts: base.Ptr(true)})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-b")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc before tombstone")
	assert.Equal(t, "3-a", doc.GetRevTreeID())

	enableCallback = true

	body = Body{"name": "Charlie", "age": 10, BodyDeleted: true}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't add revision 4-a (tombstone)")

	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc post-tombstone")
	assert.Equal(t, "2-b", doc.GetRevTreeID())
}

// Multiple clients are attempting to push conflicting non-winning revisions; multiple clients pushing different
// updates to non-winning branches that leave the branch(es) non-winning.
func TestConcurrentPushDifferentUpdateNonWinningRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD)
	var db *Database
	var enableCallback bool

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	leakyBucket := base.NewLeakyBucket(bucket, base.LeakyBucketConfig{
		UpdateCallback: func(key string) {
			if enableCallback {
				enableCallback = false
				body := Body{"name": "Joshua", "age": 11}
				collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b1", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
				assert.NoError(t, err, "Couldn't add revision 3-b1")
			}
		},
	})

	db, ctx = SetupTestDBForBucketWithOptions(t, leakyBucket, DatabaseContextOptions{AllowConflicts: base.Ptr(true)})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	body := Body{"name": "Olivia", "age": 80}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 1-a")

	body = Body{"name": "Harry", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-a")

	body = Body{"name": "Amelia", "age": 20}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 3-a")

	body = Body{"name": "Charlie", "age": 10}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"4-a", "3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 4-a")

	body = Body{"name": "Noah", "age": 40}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Adding revision 2-b")

	enableCallback = true

	body = Body{"name": "Liam", "age": 12}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-b2", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't add revision 3-b2")

	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	assert.NoError(t, err, "Retrieve doc after adding 3-b")
	assert.Equal(t, "4-a", doc.GetRevTreeID())

	rev, err := collection.GetRev(ctx, "doc1", "3-b1", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b1")
	revBody, err := rev.Body()
	assert.NoError(t, err, "Retrieve body of revision 3-b1")
	assert.Equal(t, "Joshua", revBody["name"])
	assert.Equal(t, json.Number("11"), revBody["age"])

	rev, err = collection.GetRev(ctx, "doc1", "3-b2", false, nil)
	assert.NoError(t, err, "Retrieve revision 3-b2")
	revBody, err = rev.Body()
	assert.NoError(t, err, "Retrieve body of revision 3-b2")
	assert.Equal(t, "Liam", revBody["name"])
	assert.Equal(t, json.Number("12"), revBody["age"])
}

func TestIncreasingRecentSequences(t *testing.T) {
	var db *Database
	var enableCallback bool
	var body Body
	var revid string
	var ctx context.Context

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			// Write a doc
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"2-abc", revid}, true, ExistingVersionWithUpdateToHLV)
			assert.NoError(t, err)
		}
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), base.LeakyBucketConfig{UpdateCallback: writeUpdateCallback})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	err := json.Unmarshal([]byte(`{"prop": "value"}`), &body)
	assert.NoError(t, err)

	// Create a doc
	revid, _, err = collection.Put(ctx, "doc1", body)
	assert.NoError(t, err)

	enableCallback = true
	doc, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"3-abc", "2-abc", revid}, true, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err)

	assert.True(t, sort.IsSorted(base.SortedUint64Slice(doc.SyncData.RecentSequences)))
}

func TestRepairUnorderedRecentSequences(t *testing.T) {
	if base.TestUseXattrs() {
		t.Skip("xattr=false only - test modifies doc _sync property")
	}

	var db *Database
	var body Body
	var revid string
	var ctx context.Context

	db, ctx = setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	err := json.Unmarshal([]byte(`{"prop": "value"}`), &body)
	require.NoError(t, err)

	// Create a doc
	revid, _, err = collection.Put(ctx, "doc1", body)
	require.NoError(t, err)

	// Update the doc a few times to populate recent sequences
	for i := 0; i < 10; i++ {
		updateBody := make(map[string]interface{})
		updateBody["prop"] = i
		updateBody["_rev"] = revid
		revid, _, err = collection.Put(ctx, "doc1", updateBody)
		require.NoError(t, err)
	}

	syncData, err := collection.GetDocSyncData(ctx, "doc1")
	require.NoError(t, err)
	assert.True(t, sort.IsSorted(base.SortedUint64Slice(syncData.RecentSequences)))

	// Update document directly in the bucket to scramble recent sequences
	var rawBody Body
	_, err = collection.dataStore.Get("doc1", &rawBody)
	require.NoError(t, err)
	rawSyncData, ok := rawBody["_sync"].(map[string]interface{})
	require.True(t, ok)
	rawSyncData["recent_sequences"] = []uint64{3, 5, 9, 11, 1, 2, 4, 8, 7, 10, 5}
	assert.NoError(t, collection.dataStore.Set("doc1", 0, nil, rawBody))

	// Validate non-ordered
	var rawBodyCheck Body
	_, err = collection.dataStore.Get("doc1", &rawBodyCheck)
	require.NoError(t, err)
	log.Printf("raw body check %v", rawBodyCheck)
	rawSyncDataCheck, ok := rawBody["_sync"].(map[string]interface{})
	require.True(t, ok)
	recentSequences, ok := rawSyncDataCheck["recent_sequences"].([]uint64)
	require.True(t, ok)
	assert.False(t, sort.IsSorted(base.SortedUint64Slice(recentSequences)))

	// Update the doc again. expect sequences to now be ordered
	updateBody := make(map[string]interface{})
	updateBody["prop"] = 12
	updateBody["_rev"] = revid
	_, _, err = collection.Put(ctx, "doc1", updateBody)
	require.NoError(t, err)

	syncData, err = collection.GetDocSyncData(ctx, "doc1")
	require.NoError(t, err)
	assert.True(t, sort.IsSorted(base.SortedUint64Slice(syncData.RecentSequences)))
}

func TestDeleteWithNoTombstoneCreationSupport(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Xattrs required")
	}

	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Ensure empty doc is imported correctly
	added, err := collection.dataStore.Add("doc1", 0, map[string]interface{}{})
	assert.NoError(t, err)
	assert.True(t, added)

	waitAndAssertCondition(t, func() bool {
		return db.DbStats.SharedBucketImport().ImportCount.Value() == 1
	})

	// Ensure deleted doc with double operation isn't treated as import
	_, _, err = collection.Put(ctx, "doc", map[string]interface{}{"_deleted": true})
	assert.NoError(t, err)

	var doc Body
	var xattr SyncData

	var xattrs map[string][]byte
	// Ensure document has been added
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		_, xattrs, _, err = collection.dataStore.GetWithXattrs(ctx, "doc", []string{base.SyncXattrName})
		assert.NoError(c, err)
	}, time.Second*5, time.Millisecond*100)

	require.Contains(t, xattrs, base.SyncXattrName)
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	assert.Equal(t, int64(1), db.DbStats.SharedBucketImport().ImportCount.Value())

	assert.Nil(t, doc)
	assert.Equal(t, "1-2cac91faf7b3f5e5fd56ff377bdb5466", xattr.GetRevTreeID())
	assert.Equal(t, uint64(2), xattr.Sequence)
}

func TestTombstoneCompactionStopWithManager(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("Compaction requires xattrs")
	}

	bucket := base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{})
	db, ctx := SetupTestDBForBucketWithOptions(t, bucket, DatabaseContextOptions{
		TestPurgeIntervalOverride: base.Ptr(time.Duration(0)),
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	for i := 0; i < 300; i++ {
		docID := fmt.Sprintf("doc%d", i)
		rev, _, err := collection.Put(ctx, docID, Body{})
		assert.NoError(t, err)
		_, _, err = collection.DeleteDoc(ctx, docID, DocVersion{RevTreeID: rev})
		assert.NoError(t, err)
	}

	require.NoError(t, collection.WaitForPendingChanges(ctx))

	leakyDataStore, ok := base.AsLeakyDataStore(collection.dataStore)
	require.True(t, ok)

	queryCount := 0
	callbackFunc := func() {
		queryCount++
		if queryCount == 2 {
			assert.NoError(t, db.TombstoneCompactionManager.Stop())
		}
	}

	// FIXME (bbrks): These callbacks are not firing due to leakyDataStore being set in test and not inside SG
	if base.TestsDisableGSI() {
		leakyDataStore.SetPostQueryCallback(func(ddoc, viewName string, params map[string]interface{}) {
			callbackFunc()
		})
	} else {
		leakyDataStore.SetPostN1QLQueryCallback(func() {
			callbackFunc()
		})
	}

	assert.NoError(t, db.TombstoneCompactionManager.Start(ctx, map[string]interface{}{"database": db}))

	waitAndAssertConditionWithOptions(t, func() bool {
		return db.TombstoneCompactionManager.GetRunState() == BackgroundProcessStateStopped
	}, 60, 1000)

	var tombstoneCompactionStatus TombstoneManagerResponse
	status, err := db.TombstoneCompactionManager.GetStatus(ctx)
	assert.NoError(t, err)
	err = base.JSONUnmarshal(status, &tombstoneCompactionStatus)
	assert.NoError(t, err)

	// Ensure only 250 docs have been purged which is one iteration of querying - Means stop did terminate the compaction
	assert.Equal(t, QueryTombstoneBatch, int(tombstoneCompactionStatus.DocsPurged))
}

func TestGetAllUsers(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	log.Printf("Creating users...")
	// Create users
	authenticator := db.Authenticator(ctx)
	user, _ := authenticator.NewUser("userA", "letmein", channels.BaseSetOf(t, "ABC"))
	_ = user.SetEmail("userA@test.org")
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userB", "letmein", channels.BaseSetOf(t, "ABC"))
	_ = user.SetEmail("userB@test.org")
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userC", "letmein", channels.BaseSetOf(t, "ABC"))
	user.SetDisabled(true)
	assert.NoError(t, authenticator.Save(user))
	user, _ = authenticator.NewUser("userD", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, authenticator.Save(user))

	log.Printf("Getting users...")
	users, err := db.GetUsers(ctx, 0)
	assert.NoError(t, err)
	assert.Len(t, users, 4)
	log.Printf("THE USERS: %+v", users)
	marshalled, err := json.Marshal(users)
	require.NoError(t, err)
	log.Printf("THE USERS MARSHALLED: %s", marshalled)

	limitedUsers, err := db.GetUsers(ctx, 2)
	assert.NoError(t, err)
	assert.Len(t, limitedUsers, 2)
}

func TestGetRoleIDs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	authenticator := db.Authenticator(ctx)

	rolename1 := uuid.NewString()
	rolename2 := uuid.NewString()
	username := uuid.NewString()

	user1, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err)
	user1.SetExplicitRoles(channels.TimedSet{rolename1: channels.NewVbSimpleSequence(1), rolename2: channels.NewVbSimpleSequence(1)}, 1)
	require.NoError(t, authenticator.Save(user1))

	role1, err := authenticator.NewRole(rolename1, nil)
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role1))

	role2, err := authenticator.NewRole(rolename2, nil)
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role2))

	err = db.DeleteRole(ctx, role2.Name(), false)
	require.NoError(t, err)
	roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
	require.NoError(t, err)
	assert.True(t, roleGet.IsDeleted())

	t.Log("user1:", user1.Name())
	t.Log("role1:", role1.Name())
	t.Log("role2:", role2.Name())

	// assert allprincipals still returns users and deleted roles
	users, roles, err := db.AllPrincipalIDs(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{user1.Name()}, users)
	assert.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

	roles, err = db.GetRoleIDs(ctx, db.UseViews(), false)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{role1.Name()}, roles)
}

func Test_updateAllPrincipalsSequences(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	auth := db.Authenticator(ctx)
	roleSequences := [5]uint64{}
	userSequences := [5]uint64{}

	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	for i := 0; i < 5; i++ {
		role, err := auth.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
		require.NoError(t, err)
		assert.NotEmpty(t, role)
		err = auth.Save(role)
		require.NoError(t, err)
		roleSequences[i] = role.Sequence()

		user, err := auth.NewUser(fmt.Sprintf("user%d", i), "letmein", base.SetOf("ABC"))
		require.NoError(t, err)
		assert.NotEmpty(t, user)
		err = auth.Save(user)
		require.NoError(t, err)
		userSequences[i] = user.Sequence()
	}
	err := collection.updateAllPrincipalsSequences(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		role, err := auth.GetRole(fmt.Sprintf("role%d", i))
		assert.NoError(t, err)
		assert.Greater(t, role.Sequence(), roleSequences[i])

		user, err := auth.GetUser(fmt.Sprintf("user%d", i))
		assert.NoError(t, err)
		assert.Greater(t, user.Sequence(), userSequences[i])
	}
}

func Test_invalidateAllPrincipalsCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{AllowConflicts: base.Ptr(true)})
	defer db.Close(ctx)

	sequenceAllocator, err := newSequenceAllocator(base.DatabaseLogCtx(base.TestCtx(t), db.Name, nil), db.MetadataStore, db.DbStats.DatabaseStats, db.MetadataKeys)
	assert.NoError(t, err)

	db.sequences = sequenceAllocator

	auth := db.Authenticator(ctx)
	collection := GetSingleDatabaseCollection(t, db.DatabaseContext)

	for i := 0; i < 5; i++ {
		role, err := auth.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
		assert.NoError(t, err)
		assert.NotEmpty(t, role)
		seq, err := db.sequences.nextSequence(ctx)
		assert.NoError(t, err)
		role.SetSequence(seq)
		err = auth.Save(role)
		assert.NoError(t, err)

		user, err := auth.NewUser(fmt.Sprintf("user%d", i), "letmein", base.SetOf("ABC"))
		assert.NoError(t, err)
		assert.NotEmpty(t, user)
		seq, err = db.sequences.nextSequence(ctx)
		assert.NoError(t, err)
		user.SetSequence(seq)
		err = auth.Save(user)
		assert.NoError(t, err)
	}
	endSeq, err := db.sequences.getSequence()
	assert.NoError(t, err)
	assert.Greater(t, endSeq, uint64(0))

	collection.invalidateAllPrincipals(ctx, endSeq)
	err = collection.WaitForPendingChanges(ctx)
	assert.NoError(t, err)

	if base.TestsUseNamedCollections() {
		dataStoreName := collection.dataStore.GetName()
		var scopeName, collectionName string
		// Format: sg_int_0_1671048846916047000.sg_test_0.sg_test_0
		dataStoreNameSlice := strings.Split(dataStoreName, ".")
		require.Len(t, dataStoreNameSlice, 3)
		scopeName, collectionName = dataStoreNameSlice[1], dataStoreNameSlice[2]

		// Example of Raw response when named collection is used
		// Role {"name":"role0","all_channels":null,"sequence":1,
		// "collection_access": {"sg_test_0": {"sg_test_2": {"admin_channels":{"ABC":1},"all_channels":{"ABC":1,"!":1},"channel_inval_seq":15}}}}
		type Collection struct {
			ChannelInvalSeq uint64 `json:"channel_inval_seq,omitempty"`
		}
		type invalPric struct {
			Name             string                           `json:"name,omitempty"`
			CollectionAccess map[string]map[string]Collection `json:"collection_access,omitempty"`
		}

		var invalPrinc invalPric
		for i := 0; i < 1; i++ {
			raw, _, err := db.MetadataStore.GetRaw(db.MetadataKeys.RoleKey(fmt.Sprintf("role%d", i)))
			assert.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &invalPrinc))
			fmt.Printf("raw=%s invalPrinc: %#+v\n", raw, invalPrinc)
			assert.Equal(t, int(endSeq), int(invalPrinc.CollectionAccess[scopeName][collectionName].ChannelInvalSeq))
			assert.Equal(t, fmt.Sprintf("role%d", i), invalPrinc.Name)

			raw, _, err = db.MetadataStore.GetRaw(db.MetadataKeys.UserKey(fmt.Sprintf("user%d", i)))
			require.NoError(t, err)
			err = json.Unmarshal(raw, &invalPrinc)
			assert.NoError(t, err)
			assert.Equal(t, endSeq, invalPrinc.CollectionAccess[scopeName][collectionName].ChannelInvalSeq)
			assert.Equal(t, fmt.Sprintf("user%d", i), invalPrinc.Name)
		}
	} else {
		// Example of Raw response when default collection is used
		// Role {"name":"role0","admin_channels":{"ABC":1},"all_channels":{"!":1,"ABC":1},"sequence":1,"channel_inval_seq":15}
		type invalPric struct {
			Name            string `json:"name,omitempty"`
			ChannelInvalSeq uint64 `json:"channel_inval_seq,omitempty"`
		}

		var invalPrinc invalPric
		for i := 0; i < 1; i++ {
			raw, _, err := db.MetadataStore.GetRaw(db.MetadataKeys.RoleKey(fmt.Sprintf("role%d", i)))
			assert.NoError(t, err)
			err = json.Unmarshal(raw, &invalPrinc)
			assert.NoError(t, err)
			assert.Equal(t, endSeq, invalPrinc.ChannelInvalSeq)
			assert.Equal(t, fmt.Sprintf("role%d", i), invalPrinc.Name)

			raw, _, err = db.MetadataStore.GetRaw(db.MetadataKeys.UserKey(fmt.Sprintf("user%d", i)))
			assert.NoError(t, err)
			err = json.Unmarshal(raw, &invalPrinc)
			assert.NoError(t, err)
			assert.Equal(t, endSeq, invalPrinc.ChannelInvalSeq)
			assert.Equal(t, fmt.Sprintf("user%d", i), invalPrinc.Name)
		}
	}
}

func Test_resyncDocument(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Walrus doesn't support xattr")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	db.Options.EnableXattr = true
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	syncFn := `
	function sync(doc, oldDoc){
		channel("channel." + "ABC");
	}
`
	_, err := collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	docID := uuid.NewString()

	updateBody := make(map[string]interface{})
	updateBody["val"] = "value"
	_, doc, err := collection.Put(ctx, docID, updateBody)
	require.NoError(t, err)
	assert.NotNil(t, doc)

	syncFn = `
		function sync(doc, oldDoc){
			channel("channel." + "ABC12332423234");
		}
	`
	_, err = collection.UpdateSyncFun(ctx, syncFn)
	require.NoError(t, err)

	_, _, err = collection.resyncDocument(ctx, docID, realDocID(docID), false, []uint64{10})
	require.NoError(t, err)
	err = collection.WaitForPendingChanges(ctx)
	require.NoError(t, err)

	syncData, err := collection.GetDocSyncData(ctx, docID)
	assert.NoError(t, err)

	assert.Len(t, syncData.ChannelSet, 2)
	assert.Len(t, syncData.Channels, 2)
	found := false

	for _, chSet := range syncData.ChannelSet {
		if chSet.Name == "channel.ABC12332423234" {
			found = true
			break
		}
	}

	assert.True(t, found)
	assert.Equal(t, 2, int(db.DbStats.Database().SyncFunctionCount.Value()))

}

func Test_getUpdatedDocument(t *testing.T) {
	t.Run("Non Sync document is not processed", func(t *testing.T) {
		db, ctx := setupTestDB(t)
		defer db.Close(ctx)

		docID := "testDoc"

		body := `{"val": "nonsyncdoc"}`
		added, err := db.Bucket.DefaultDataStore().AddRaw(docID, 0, []byte(body))
		require.NoError(t, err)
		assert.True(t, added)

		raw, _, err := db.Bucket.DefaultDataStore().GetRaw(docID)
		require.NoError(t, err)
		doc, err := unmarshalDocument(docID, raw)
		require.NoError(t, err)

		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
		_, _, _, _, _, err = collection.getResyncedDocument(ctx, doc, false, []uint64{})
		assert.Equal(t, base.ErrUpdateCancel, err)
	})

	t.Run("Sync Document", func(t *testing.T) {
		db, ctx := setupTestDB(t)
		defer db.Close(ctx)
		collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
		syncFn := `
	function sync(doc, oldDoc){
		channel("channel." + "ABC");
	}
`
		_, err := collection.UpdateSyncFun(ctx, syncFn)
		require.NoError(t, err)

		docID := uuid.NewString()
		updateBody := make(map[string]interface{})
		updateBody["val"] = "value"
		_, doc, err := collection.Put(ctx, docID, updateBody)
		require.NoError(t, err)
		assert.NotNil(t, doc)

		syncFn = `
		function sync(doc, oldDoc){
			channel("channel." + "ABC12332423234");
		}
	`
		_, err = collection.UpdateSyncFun(ctx, syncFn)
		require.NoError(t, err)

		updatedDoc, shouldUpdate, _, highSeq, _, err := collection.getResyncedDocument(ctx, doc, false, []uint64{})
		require.NoError(t, err)
		assert.True(t, shouldUpdate)
		assert.Equal(t, doc.Sequence, highSeq)
		assert.Equal(t, 2, int(db.DbStats.Database().SyncFunctionCount.Value()))

		// Rerunning same resync function should mark doc not to be updated
		_, shouldUpdate, _, _, _, err = collection.getResyncedDocument(ctx, updatedDoc, false, []uint64{})
		require.NoError(t, err)
		assert.False(t, shouldUpdate)
		assert.Equal(t, 3, int(db.DbStats.Database().SyncFunctionCount.Value()))
	})

}

// Regression test for CBG-2058.
func TestImportCompactPanic(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("requires xattrs")
	}

	// Set the compaction and purge interval unrealistically low to reproduce faster
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{
		CompactInterval:           1,
		TestPurgeIntervalOverride: base.Ptr(time.Duration(0)),
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create a document, then delete it, to create a tombstone
	rev, doc, err := collection.Put(ctx, "test", Body{})
	require.NoError(t, err)
	_, _, err = collection.DeleteDoc(ctx, doc.ID, DocVersion{RevTreeID: rev})
	require.NoError(t, err)
	require.NoError(t, collection.WaitForPendingChanges(ctx))

	// Wait for Compact to run - in the failing case it'll panic before incrementing the stat
	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().NumTombstonesCompacted.Value()
	}, 1)
}

func TestGetDatabaseCollectionWithUserScopesNil(t *testing.T) {
	testCases := []struct {
		scope      string
		collection string
	}{
		{
			scope:      "",
			collection: "",
		},
		{
			scope:      "foo",
			collection: "bar",
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.scope, testCase.collection), func(t *testing.T) {

			db := Database{DatabaseContext: &DatabaseContext{}}
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			require.Error(t, err)
			require.Nil(t, col)
		})
	}
}

func TestGetDatabaseCollectionWithUserNoScopesConfigured(t *testing.T) {
	testCases := []struct {
		scope      string
		collection string
	}{
		{
			scope:      "",
			collection: "",
		},
		{
			scope:      "foo",
			collection: "bar",
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s.%s", testCase.scope, testCase.collection), func(t *testing.T) {

			db := Database{DatabaseContext: &DatabaseContext{}}
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			require.Error(t, err)
			require.Nil(t, col)
		})
	}
}

func TestGetDatabaseCollectionWithUserDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)
	base.RequireNumTestDataStores(t, 1)

	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))

	ds, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	require.NotNil(t, ds)

	testCases := []struct {
		name       string
		scope      string
		collection string
		err        bool
		options    DatabaseContextOptions
	}{
		{
			name:       "_default._default-inconfig",
			scope:      base.DefaultScope,
			collection: base.DefaultCollection,
			err:        false,
			options: DatabaseContextOptions{
				Scopes: map[string]ScopeOptions{
					ds.ScopeName(): ScopeOptions{
						Collections: map[string]CollectionOptions{
							ds.CollectionName(): {},
						},
					},
					base.DefaultScope: ScopeOptions{
						Collections: map[string]CollectionOptions{
							base.DefaultCollection: {},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			ctx := base.TestCtx(t)
			dbCtx, err := NewDatabaseContext(ctx, "db", bucket.NoCloseClone(), false, testCase.options)
			require.NoError(t, err)

			db, err := GetDatabase(dbCtx, nil)
			require.NoError(t, err)
			defer db.Close(ctx)
			col, err := db.GetDatabaseCollectionWithUser(testCase.scope, testCase.collection)
			if testCase.err {
				require.Error(t, err)
				require.Nil(t, col)
			} else {
				require.NoError(t, err)
				require.NotNil(t, col)
				require.Equal(t, col.ScopeName, testCase.scope)
				require.Equal(t, col.Name, testCase.collection)
			}

		})
	}

}

func TestServerUUID(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	if base.TestUseCouchbaseServer() {
		require.Len(t, db.ServerUUID, 32) // no dashes in UUID
	} else {
		require.Len(t, db.ServerUUID, 0) // no dashes in UUID
	}
}

func waitAndAssertConditionWithOptions(t *testing.T, fn func() bool, retryCount, msSleepTime int, failureMsgAndArgs ...interface{}) {
	for i := 0; i <= retryCount; i++ {
		if i == retryCount {
			assert.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(msSleepTime))
	}
}

func waitAndAssertCondition(t *testing.T, fn func() bool, failureMsgAndArgs ...interface{}) {
	waitAndAssertConditionWithOptions(t, fn, 20, 100, failureMsgAndArgs...)
}

func Test_stopBackgroundManagers(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	testCases := []struct {
		resyncManager               *BackgroundManager
		tombstoneCompactionManager  *BackgroundManager
		attachmentCompactionManager *BackgroundManager
		expected                    int
	}{
		{
			expected: 0,
		},
		{
			resyncManager: &BackgroundManager{
				name:    "test_resync",
				Process: &testBackgroundProcess{isStoppable: true},
			},
			expected: 1,
		},
		{
			resyncManager: &BackgroundManager{
				name:    "test_resync",
				Process: &testBackgroundProcess{isStoppable: true},
			},
			tombstoneCompactionManager: &BackgroundManager{
				name:    "test_tombstone",
				Process: &testBackgroundProcess{isStoppable: true},
			},
			attachmentCompactionManager: &BackgroundManager{
				name:    "test_attachment",
				Process: &testBackgroundProcess{isStoppable: true},
			},
			expected: 3,
		},
	}

	emptyOptions := map[string]interface{}{}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			db.ResyncManager = testCase.resyncManager
			db.AttachmentCompactionManager = testCase.attachmentCompactionManager
			db.TombstoneCompactionManager = testCase.tombstoneCompactionManager
			if db.ResyncManager != nil {
				err := db.ResyncManager.Start(ctx, emptyOptions)
				assert.NoError(t, err)
			}
			if db.AttachmentCompactionManager != nil {
				err := db.AttachmentCompactionManager.Start(ctx, emptyOptions)
				assert.NoError(t, err)
			}
			if db.TombstoneCompactionManager != nil {
				err := db.TombstoneCompactionManager.Start(ctx, emptyOptions)
				assert.NoError(t, err)
			}

			bgManagers := db.stopBackgroundManagers()
			assert.Len(t, bgManagers, testCase.expected, "Unexpected Num of BackgroundManagers returned")
		})
	}
}

func TestUpdateCalculatedStatsPanic(t *testing.T) {

	var dbc *DatabaseContext

	defer func() {
		r := recover()
		if r != nil {
			t.Errorf("UpdateCalculatedStats panic recovered, stack trace: %s", debug.Stack())
		}
	}()

	// nil DatabaseContext check
	ctx := base.TestCtx(t)
	dbc.UpdateCalculatedStats(ctx)

	// non-nil DatabaseContext, nil stats
	dbc = &DatabaseContext{}
	dbc.UpdateCalculatedStats(ctx)

	// non-nil DatabaseContext and stats, nil channel cache
	dbStats, statsError := initDatabaseStats(ctx, "db", false, DatabaseContextOptions{})
	require.NoError(t, statsError)
	dbc.DbStats = dbStats
	dbc.UpdateCalculatedStats(ctx)
}

func Test_waitForBackgroundManagersToStop(t *testing.T) {
	// use testing/synctest to have fake time once go 1.25 is available. In the meantime, windows and -race are
	// slow enough thatthe time is increased, but this keeps interactive testing fast.
	waitTime := 50 * time.Millisecond
	if runtime.GOOS == "windows" || base.IsRaceDetectorEnabled(t) {
		waitTime = 1 * time.Second
	}
	t.Run("single unstoppable process", func(t *testing.T) {
		bgMngr := &BackgroundManager{
			name:    "test_unstoppable_runner",
			Process: &testBackgroundProcess{isStoppable: false},
		}
		ctx := base.TestCtx(t)
		err := bgMngr.Start(ctx, map[string]interface{}{})
		require.NoError(t, err)
		err = bgMngr.Stop()
		require.NoError(t, err)

		startTime := time.Now()
		deadline := waitTime
		waitForBackgroundManagersToStop(ctx, deadline, []*BackgroundManager{bgMngr})
		assert.Greater(t, time.Since(startTime), deadline)
		assert.Equal(t, BackgroundProcessStateStopping, bgMngr.GetRunState())
	})

	t.Run("single stoppable process", func(t *testing.T) {
		bgMngr := &BackgroundManager{
			name:    "test_stoppable_runner",
			Process: &testBackgroundProcess{isStoppable: true},
		}
		ctx := base.TestCtx(t)
		err := bgMngr.Start(ctx, map[string]interface{}{})
		require.NoError(t, err)
		err = bgMngr.Stop()
		require.NoError(t, err)

		startTime := time.Now()
		deadline := waitTime
		waitForBackgroundManagersToStop(ctx, deadline, []*BackgroundManager{bgMngr})
		assert.Less(t, time.Since(startTime), deadline)
		assert.Equal(t, BackgroundProcessStateStopped, bgMngr.GetRunState())
	})

	t.Run("one stoppable process and one unstoppable process", func(t *testing.T) {
		stoppableBgMngr := &BackgroundManager{
			name:    "test_stoppable_runner",
			Process: &testBackgroundProcess{isStoppable: true},
		}
		ctx := base.TestCtx(t)
		err := stoppableBgMngr.Start(ctx, map[string]interface{}{})
		require.NoError(t, err)
		err = stoppableBgMngr.Stop()
		require.NoError(t, err)

		unstoppableBgMngr := &BackgroundManager{
			name:    "test_unstoppable_runner",
			Process: &testBackgroundProcess{isStoppable: false},
		}

		err = unstoppableBgMngr.Start(ctx, map[string]interface{}{})
		require.NoError(t, err)
		err = unstoppableBgMngr.Stop()
		require.NoError(t, err)

		startTime := time.Now()
		deadline := waitTime
		waitForBackgroundManagersToStop(ctx, deadline, []*BackgroundManager{stoppableBgMngr, unstoppableBgMngr})
		assert.Greater(t, time.Since(startTime), deadline)
		assert.Equal(t, BackgroundProcessStateStopped, stoppableBgMngr.GetRunState())
		assert.Equal(t, BackgroundProcessStateStopping, unstoppableBgMngr.GetRunState())
	})
}

// Test BackgroundManagerProcessI which can be configured to stop or run forever
var _ BackgroundManagerProcessI = &testBackgroundProcess{}

type testBackgroundProcess struct {
	isStoppable bool
}

func (i *testBackgroundProcess) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	return nil
}

func (i *testBackgroundProcess) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	<-terminator.Done()
	if i.isStoppable {
		return nil
	}
	// stimulate a process taking 30 seconds to stop
	time.Sleep(30 * time.Second)
	return nil
}

func (i *testBackgroundProcess) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	statusJSON, err := base.JSONMarshal(status)
	if err != nil {
		return nil, nil, err
	}

	return statusJSON, nil, nil
}

func (i *testBackgroundProcess) ResetStatus() {
}

// Make sure that closing a database context after a mutation feed fails to start results does not panic
func TestBadDCPStart(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	if !bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		t.Skip("This test requires collections support on server (7.0 or greater)")
	}
	dbcOptions := DatabaseContextOptions{
		Scopes: GetScopesOptions(t, bucket, 1),
	}
	dbCtx, err := NewDatabaseContext(ctx, "db", bucket, true, dbcOptions)
	require.NoError(t, err)

	// fake a bad DCP start by setting invalid scope. This test is fragile to the fact that DatabaseContext.mutationListener.Start is the first function to use the Scopes parameter
	dbCtx.Scopes = map[string]Scope{
		"1InvalidScope": Scope{
			Collections: map[string]*DatabaseCollection{
				"InvalidCollection": nil,
			},
		},
	}

	ctx = dbCtx.AddDatabaseLogContext(ctx)
	err = dbCtx.StartOnlineProcesses(ctx)
	require.Error(t, err)

	dbCtx.Close(ctx)
}

func TestInject1xBodyProperties(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	rev1ID, _, err := collection.Put(ctx, "doc", Body{"test": "doc"})
	require.NoError(t, err)
	var rev2Body Body
	rev2Data := `{"key":"value", "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	require.NoError(t, base.JSONUnmarshal([]byte(rev2Data), &rev2Body))
	_, rev2ID, err := collection.PutExistingRevWithBody(ctx, "doc", rev2Body, []string{"2-abc", rev1ID}, true, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)

	docRev, err := collection.GetRev(ctx, "doc", rev2ID, true, nil)
	require.NoError(t, err)

	// mock expiry on doc
	exp := time.Now()
	docRev.Expiry = &exp

	newDoc, err := docRev.Inject1xBodyProperties(ctx, collection, docRev.History, nil, true)
	require.NoError(t, err)
	var resBody Body
	require.NoError(t, resBody.Unmarshal(newDoc))

	// cast to map of interface given we have injected the properties runtime has no concept of the AttachmentMeta and Revisions types
	revs := resBody[BodyRevisions].(map[string]interface{})
	atts := resBody[BodyAttachments].(map[string]interface{})

	assert.NotNil(t, atts)
	assert.NotNil(t, revs)
	assert.Equal(t, "doc", resBody[BodyId])
	assert.Equal(t, "2-abc", resBody[BodyRev])
	assert.Equal(t, exp.Format(time.RFC3339), resBody[BodyExpiry])
	assert.Equal(t, "value", resBody["key"])

	// mock doc deleted
	docRev.Deleted = true

	newDoc, err = docRev.Inject1xBodyProperties(ctx, collection, docRev.History, []string{"2-abc"}, true)
	require.NoError(t, err)
	require.NoError(t, resBody.Unmarshal(newDoc))

	// cast to map of interface given we have injected the properties runtime has no concept of the AttachmentMeta and Revisions types
	revs = resBody[BodyRevisions].(map[string]interface{})
	atts = resBody[BodyAttachments].(map[string]interface{})

	assert.NotNil(t, atts)
	assert.NotNil(t, revs)
	assert.Equal(t, "doc", resBody[BodyId])
	assert.Equal(t, "2-abc", resBody[BodyRev])
	assert.Equal(t, exp.Format(time.RFC3339), resBody[BodyExpiry])
	assert.Equal(t, "value", resBody["key"])
	assert.True(t, resBody[BodyDeleted].(bool))
}

func TestDatabaseCloseIdempotent(t *testing.T) {
	db, ctx := setupTestDBWithOptionsAndImport(t, nil, DatabaseContextOptions{})
	defer db.Close(ctx)

	// simulate a stop from StartOnlineProcesses, if the import feed does not work
	db.BucketLock.Lock()
	defer db.BucketLock.Unlock()
	db._stopOnlineProcesses(ctx)
}

// TestSettingSyncInfo:
//   - Purpose of the test is to call both SetSyncInfoMetaVersion + SetSyncInfoMetadataID in different orders
//     asserting that the operations preserve the metadataID/metaVersion
//   - Permutations include doc being created if it doesn't exist, one element being updated and preserving the other
//     elements if it exists and
func TestSettingSyncInfo(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	ds := collection.GetCollectionDatastore()

	require.NoError(t, base.SetSyncInfoMetaVersion(ds, "1"))
	require.NoError(t, base.SetSyncInfoMetadataID(ds, "someID"))

	// assert that after above operations meta version is preserved after setting of metadataID
	var syncInfo base.SyncInfo
	_, err := ds.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, "1", syncInfo.MetaDataVersion)
	assert.Equal(t, "someID", syncInfo.MetadataID)

	// remove sync info to test another permutation
	require.NoError(t, ds.Delete(base.SGSyncInfo))

	require.NoError(t, base.SetSyncInfoMetadataID(ds, "someID"))
	require.NoError(t, base.SetSyncInfoMetaVersion(ds, "1"))

	// assert that after above operations metadataID is preserved after setting of metaVersion
	syncInfo = base.SyncInfo{}
	_, err = ds.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, "1", syncInfo.MetaDataVersion)
	assert.Equal(t, "someID", syncInfo.MetadataID)

	// test updating each element in sync info now both elements are defined
	require.NoError(t, base.SetSyncInfoMetaVersion(ds, "4"))
	_, err = ds.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, "4", syncInfo.MetaDataVersion)
	assert.Equal(t, "someID", syncInfo.MetadataID)

	require.NoError(t, base.SetSyncInfoMetadataID(ds, "test"))
	_, err = ds.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, "4", syncInfo.MetaDataVersion)
	assert.Equal(t, "test", syncInfo.MetadataID)
}

// TestRequireMigration:
//   - Purpose is to test code pathways inside the InitSyncInfo function will return requires attachment migration
//     as expected.
func TestRequireMigration(t *testing.T) {
	type testCase struct {
		name             string
		initialMetaID    string
		newMetadataID    string
		metaVersion      string
		requireMigration bool
	}
	testCases := []testCase{
		{
			name:             "sync info in bucket with metadataID set",
			initialMetaID:    "someID",
			requireMigration: true,
		},
		{
			name:             "sync info in bucket with metadataID set, set newMetadataID",
			initialMetaID:    "someID",
			newMetadataID:    "testID",
			requireMigration: true,
		},
		{
			name:             "correct metaversion already defined, no metadata ID to set",
			metaVersion:      "4.0.0",
			requireMigration: false,
		},
		{
			name:             "correct metaversion already defined, metadata ID to set",
			metaVersion:      "4.0.0",
			newMetadataID:    "someID",
			requireMigration: false,
		},
		{
			name:             "old metaversion defined, metadata ID to set",
			metaVersion:      "3.0.0",
			newMetadataID:    "someID",
			requireMigration: true,
		},
		{
			name:             "old metaversion defined, no metadata ID to set",
			metaVersion:      "3.0.0",
			requireMigration: true,
		},
	}
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	ds := tb.GetSingleDataStore()
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			if testcase.initialMetaID != "" {
				require.NoError(t, base.SetSyncInfoMetadataID(ds, testcase.initialMetaID))
			}
			if testcase.metaVersion != "" {
				require.NoError(t, base.SetSyncInfoMetaVersion(ds, testcase.metaVersion))
			}

			_, requireMigration, err := base.InitSyncInfo(ctx, ds, testcase.newMetadataID)
			require.NoError(t, err)
			if testcase.requireMigration {
				assert.True(t, requireMigration)
			} else {
				assert.False(t, requireMigration)
			}

			// cleanup bucket
			require.NoError(t, ds.Delete(base.SGSyncInfo))
		})
	}
}

// TestInitSyncInfoRequireMigrationEmptyBucket:
//   - Empty bucket call InitSyncInfo with metadata ID defined, assert require migration is returned
//   - Cleanup bucket
//   - Call InitSyncInfo with metadata ID not defined, assert require migration is not returned
func TestInitSyncInfoRequireMigrationEmptyBucket(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	ds := tb.GetSingleDataStore()

	// test no sync info in bucket and set metadataID, returns requireMigration
	_, requireMigration, err := base.InitSyncInfo(ctx, ds, "someID")
	require.NoError(t, err)
	assert.True(t, requireMigration)

	// delete the doc, test no sync info in bucket returns requireMigration
	require.NoError(t, ds.Delete(base.SGSyncInfo))
	_, requireMigration, err = base.InitSyncInfo(ctx, ds, "")
	require.NoError(t, err)
	assert.True(t, requireMigration)
}

// TestInitSyncInfoMetaVersionComparison:
//   - Test requireMigration is true for metaVersion == 4.0.0 and > 4.0.0
//   - Test requireMigration is true for non-existent metaVersion
func TestInitSyncInfoMetaVersionComparison(t *testing.T) {
	type testCase struct {
		name        string
		metadataID  string
		metaVersion string
	}
	testCases := []testCase{
		{
			name:       "requireMigration for sync info with no meta version defined",
			metadataID: "someID",
		},
		{
			name:        "test requireMigration for metaVersion == 4.0.0",
			metadataID:  "someID",
			metaVersion: "4.0.0",
		},
		{
			name:        "test we return true for metaVersion minor version > 4.0.0",
			metadataID:  "someID",
			metaVersion: "4.1.0",
		},
		{
			name:        "test we return true for metaVersion patch version > 4.0.0",
			metadataID:  "someID",
			metaVersion: "4.0.1",
		},
		{
			name:        "test we return true for metaVersion major version > 4.0.0",
			metadataID:  "someID",
			metaVersion: "5.0.0",
		},
	}
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	ds := tb.GetSingleDataStore()
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			// set sync info with no metaversion
			require.NoError(t, base.SetSyncInfoMetadataID(ds, testcase.metadataID))

			if testcase.metaVersion == "" {
				_, requireMigration, err := base.InitSyncInfo(ctx, ds, "someID")
				require.NoError(t, err)
				assert.True(t, requireMigration)
			} else {
				require.NoError(t, base.SetSyncInfoMetaVersion(ds, testcase.metaVersion))
				_, requireMigration, err := base.InitSyncInfo(ctx, ds, "someID")
				require.NoError(t, err)
				assert.False(t, requireMigration)
			}
			// cleanup bucket
			require.NoError(t, ds.Delete(base.SGSyncInfo))
		})
	}
}

func TestRevTreeConflictCheck(t *testing.T) {
	testCases := []struct {
		name             string
		localHistory     []string
		incomingHistory  []string
		localDelete      bool
		remoteDelete     bool
		expectedConflict bool
	}{
		{
			name:             "no conflict, simple update",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"4-abc", "3-abc", "2-abc", "1-abc"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: false,
		},
		{
			name:             "diff in history",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"3-def", "2-def", "1-abc"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: true,
		},
		{
			name:             "complete diff in history",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"3-def", "2-def", "1-def"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: true,
		},
		{
			name:             "gap in history",
			localHistory:     []string{"5-abc", "4-abc", "3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"10-abc", "9-abc"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: true,
		},
		{
			name:             "incoming only has one entry in history, conflict 1",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"2-abc"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: true,
		},
		{
			name:             "incoming only has one entry in history, conflict 2",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"2-def"},
			localDelete:      false,
			remoteDelete:     false,
			expectedConflict: true,
		},
		{
			name:             "local is deleted and branch is disconnected",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"10-abc", "9-abc"},
			localDelete:      true,
			remoteDelete:     false,
			expectedConflict: false,
		},
		{
			name:             "remote is deleted and local branch is not tombstones",
			localHistory:     []string{"3-abc", "2-abc", "1-abc"},
			incomingHistory:  []string{"4-abc", "3-abc", "2-abc", "1-abc"},
			localDelete:      false,
			remoteDelete:     true,
			expectedConflict: false,
		},
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			doc := NewDocument("doc1")
			if len(testCase.localHistory) != 0 {
				doc.SetRevTreeID(testCase.localHistory[0])
			}
			if testCase.localDelete {
				doc.Deleted = true
				doc.setFlag(channels.Deleted, true)
			}
			lenLocalHistory := len(testCase.localHistory)

			parent := ""
			for i, revID := range testCase.localHistory {
				err := doc.History.addRevision(revID,
					RevInfo{
						ID:      revID,
						Parent:  parent, // set the parent of this revision to the element of docHistory from the last iteration
						Deleted: i == lenLocalHistory-1 && testCase.localDelete})
				require.NoError(t, err)
				parent = revID
			}

			_, _, conflictErr := collection.revTreeConflictCheck(t.Context(), testCase.incomingHistory, doc, testCase.remoteDelete)
			if testCase.expectedConflict {
				require.Error(t, conflictErr)
			} else {
				require.NoError(t, conflictErr)
			}
		})
	}

}
