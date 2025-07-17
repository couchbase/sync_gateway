/*
Copyright 2020-Present Couchbase, Inc.

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
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

// TestX509RoundtripUsingIP is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using an IP address which is also present in the node cert.
func TestX509RoundtripUsingIP(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb, useTLSServer: true})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	RequireStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	rt.WaitForDoc(t.Name())
}

// TestX509RoundtripUsingDomain is a happy-path roundtrip write test for SG connecting to CBS using valid X.509 certs for authentication.
// The test enforces SG connects using a domain name which is also present in the node cert.
func TestX509RoundtripUsingDomain(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb, useTLSServer: true})
	defer rt.Close()

	// write a doc to ensure bucket ops work
	tr := rt.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"sgwrite":true}`)
	RequireStatus(t, tr, http.StatusCreated)

	// wait for doc to come back over DCP
	rt.WaitForDoc(t.Name())
}

func TestX509UnknownAuthorityWrap(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	tb.BucketSpec.CACertPath = ""

	sc := DefaultStartupConfig("")

	sc.Bootstrap.Server = tb.BucketSpec.Server
	if tb.BucketSpec.Auth != nil {
		username, password, _ := tb.BucketSpec.Auth.GetCredentials()
		sc.Bootstrap.Username = username
		sc.Bootstrap.Password = password
	}

	_, err := initClusterAgent(base.TestCtx(t), sc.Bootstrap.Server, sc.Bootstrap.Username, sc.Bootstrap.Password,
		sc.Bootstrap.X509CertPath, sc.Bootstrap.X509KeyPath, sc.Bootstrap.CACertPath, sc.Bootstrap.ServerTLSSkipVerify)
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "Provide a CA cert, or set tls_skip_verify to true in config")
}

func TestAttachmentCompactionRun(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb, useTLSServer: true})
	defer rt.Close()

	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	for i := 0; i < 20; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attID := fmt.Sprintf("testAtt-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		CreateLegacyAttachmentDoc(t, ctx, collection, docID, []byte("{}"), attID, attJSONBody)
	}

	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	RequireStatus(t, resp, http.StatusOK)

	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
	assert.Equal(t, int64(20), status.MarkedAttachments)
}
