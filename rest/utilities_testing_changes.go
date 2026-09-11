/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

type ChangesResults struct {
	Results  []db.ChangeEntry
	Last_Seq db.SequenceID
}

func (cr ChangesResults) RequireDocIDs(t testing.TB, docIDs []string) {
	t.Helper()
	require.Len(t, cr.Results, len(docIDs))
	for _, docID := range docIDs {
		var found bool
		for _, changeEntry := range cr.Results {
			if changeEntry.ID == docID {
				found = true
				break
			}
		}
		require.True(t, found, "DocID %q missing from results %v", docID, cr.Results)
	}
}

func (cr ChangesResults) RequireRevID(t testing.TB, revIDs []string) {
	t.Helper()
	require.Equal(t, len(revIDs), len(cr.Results))
	for _, rev := range revIDs {
		var found bool
		for _, changeEntry := range cr.Results {
			if changeEntry.Changes[0]["rev"] == rev {
				found = true
				break
			}
		}
		require.True(t, found, "RevID %q missing from results %v", rev, cr.Results)
	}
}

// GetChangeEntry returns the single change entry for docID, failing the test if there isn't exactly one.
func (cr ChangesResults) GetChangeEntry(t testing.TB, docID string) db.ChangeEntry {
	t.Helper()
	var matches []db.ChangeEntry
	for _, changeEntry := range cr.Results {
		if changeEntry.ID == docID {
			matches = append(matches, changeEntry)
		}
	}
	require.Lenf(t, matches, 1, "Expected exactly one entry for DocID %q in results %s", docID, cr.Summary())
	return matches[0]
}

func (cr ChangesResults) Summary() string {
	var revs []string
	for _, changeEntry := range cr.Results {
		revs = append(revs, fmt.Sprintf("{ID:%s}", changeEntry.ID))
	}
	return strings.Join(revs, ", ")
}

// RequireChangeRev asserts that the given db.ChangeByVersionType returned a /_changes feed has the expected DocVersion entry, for a given versionType (rev or cv)
func RequireChangeRev(t *testing.T, expected DocVersion, changeRev db.ChangeByVersionType, versionType db.ChangesVersionType) {
	t.Helper()
	// Only one version type will be populated on a changes feed, based on what the original request demanded and what version types are available on that particular revision.
	var expectedStr string
	switch versionType {
	case db.ChangesVersionTypeRevTreeID:
		expectedStr = expected.RevTreeID
	case db.ChangesVersionTypeCV:
		expectedStr = expected.CV.String()
	default:
		t.Fatalf("Unexpected version type: %q", versionType)
	}
	require.Equalf(t, expectedStr, changeRev[versionType], "Expected changeRev[%q]==%s, got %s", versionType, expected.RevTreeID, changeRev[versionType])
}

// WaitForChanges waits for the specific number of changes to appear. Fails the test harness if more or fewer changes appear.
func (rt *RestTester) WaitForChanges(numChangesExpected int, changesURL, username string, useAdminPort bool) ChangesResults {
	rt.TB().Helper()
	return rt.WaitForChangesWithOptions(numChangesExpected, WaitForChangesOptions{
		Method:   http.MethodGet,
		URL:      changesURL,
		Username: username,
		Admin:    useAdminPort,
	})
}

// WaitForChangesOptions describes the _changes request issued by WaitForChangesWithOptions.
type WaitForChangesOptions struct {
	Method   string // http.MethodGet or http.MethodPost
	URL      string // request URL, templated via mustTemplateResource
	Username string // user to authenticate as, required unless Admin is set
	Admin    bool   // issue the request against the admin port
	Body     string // request body, only valid with http.MethodPost
}

// validate returns an error if the options describe a request that can't be issued.
func (o WaitForChangesOptions) validate() error {
	switch o.Method {
	case http.MethodPost:
	case http.MethodGet:
		if o.Body != "" {
			return fmt.Errorf("body must be empty for %s requests, had %q", o.Method, o.Body)
		}
	default:
		return fmt.Errorf("unsupported method %q, expecting %s or %s", o.Method, http.MethodGet, http.MethodPost)
	}
	if !o.Admin && o.Username == "" {
		return fmt.Errorf("username is required for requests against the public port")
	}
	return nil
}

// WaitForChangesWithOptions waits for the specific number of changes to appear. Fails the test harness if more or fewer changes appear.
func (rt *RestTester) WaitForChangesWithOptions(numChangesExpected int, options WaitForChangesOptions) ChangesResults {
	rt.TB().Helper()
	require.NoError(rt.TB(), options.validate())
	waitTime := 20 * time.Second // some tests rely on cbgt import which can be quite slow if it needs to rollback
	if db.HasCachingFeedDelay(rt.TB()) {
		waitTime *= db.GetCachingFeedDelayFactor(rt.TB())
	} else if sgtest.UnitTestUrlIsWalrus() && !sgtest.IsRaceDetectorEnabled(rt.TB()) && os.Getenv("CI") == "" {
		// local rosmar will never take a long time, but it is sometimes slower in jenkins/github actions
		waitTime = 1 * time.Second
	}
	var changes *ChangesResults
	url := rt.mustTemplateResource(options.URL)
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		var response *TestResponse
		if options.Admin {
			response = rt.SendAdminRequest(options.Method, url, options.Body)
		} else {
			response = rt.Send(RequestByUser(options.Method, url, options.Body, options.Username))
		}
		if !AssertStatus(c, response, http.StatusOK) {
			return
		}
		if !assert.NoError(c, base.JSONUnmarshal(response.Body.Bytes(), &changes), "Could not unmarshal %s", response.BodyString()) {
			return
		}
		assert.Len(c, changes.Results, numChangesExpected, "Expected %d changes, got %s changes", numChangesExpected, changes.Summary())
	}, waitTime, 10*time.Millisecond)
	return *changes
}
