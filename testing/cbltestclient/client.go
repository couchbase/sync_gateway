// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// apiVersionHeader carries the API version on both the request and the response.
	apiVersionHeader = "CBLTest-API-Version"
	// clientIDHeader identifies the session every request but GET / belongs to.
	clientIDHeader = "CBLTest-Client-ID"

	// DefaultDatasetVersion is the dataset version NewSession sends when none is given.  It only
	// matters for datasets, which Sync Gateway's tests don't use, but the C test server requires
	// the field to be present.
	DefaultDatasetVersion = "4.0"

	// defaultRequestTimeout bounds a single control API call, so a wedged test server surfaces as
	// an error rather than a hung test.
	defaultRequestTimeout = 30 * time.Second
)

// notFoundMessage matches the message the test server uses when a document is absent or deleted.
// The spec says a missing document is a 404, and the C server returns one since
// couchbase-lite-tests#598.  A server built before that returns a 400 with this message, and the
// message is the only thing telling it apart from a malformed request.
var notFoundMessage = regexp.MustCompile(`^Document '.*' not found$`)

// Client is an HTTP client for the Couchbase Lite test server control API.  It is safe for
// concurrent use, but the test server itself holds a single session, so callers still have to
// coordinate Reset with each other.
type Client struct {
	baseURL    string
	httpClient *http.Client
	// apiVersion is negotiated from GET / rather than assumed: the spec is at 2 while the C server
	// still reports 1, and the server rejects a version it does not recognise.
	apiVersion int
	// clientID is both the CBLTest-Client-ID header and the session ID passed to NewSession.
	clientID string
}

// NewClient probes the test server at baseURL ("http://host:port") and returns a client bound to
// its API version.  Call NewSession before any other method: every endpoint but GET / needs a
// session to exist.
func NewClient(ctx context.Context, baseURL string) (*Client, error) {
	client := &Client{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: defaultRequestTimeout},
		clientID:   uuid.NewString(),
	}
	info, err := client.ServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	client.apiVersion = info.APIVersion
	return client, nil
}

// BaseURL returns the URL the client was created with.
func (c *Client) BaseURL() string { return c.baseURL }

// APIVersion returns the API version the server reported.
func (c *Client) APIVersion() int { return c.apiVersion }

// ClientID returns the session ID this client uses.
func (c *Client) ClientID() string { return c.clientID }

// ServerInfo returns the test server's version, API version and platform.  It is the only endpoint
// that works before a session exists.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	var info ServerInfo
	err := c.do(ctx, http.MethodGet, "/", nil, &info)
	return info, err
}

// NewSession creates the session every other endpoint needs, identified by the client's ID.  The
// test server keeps exactly one session and deletes its whole sessions directory when a new one is
// created, so this must be called once per server process, never once per test.
//
// Pass an empty datasetVersion for DefaultDatasetVersion.
func (c *Client) NewSession(ctx context.Context, datasetVersion string) error {
	if datasetVersion == "" {
		datasetVersion = DefaultDatasetVersion
	}
	request := struct {
		ID             string `json:"id"`
		DatasetVersion string `json:"dataset_version"`
	}{ID: c.clientID, DatasetVersion: datasetVersion}
	return c.post(ctx, "/newSession", request, nil)
}

// Reset deletes every database in the session and creates the ones given.  Because it deletes
// everything, callers sharing a test server have to reset together rather than one at a time.
//
// testName is recorded in the test server's log to mark where a test begins.
func (c *Client) Reset(ctx context.Context, testName string, databases map[string]DatabaseSpec) error {
	request := struct {
		Test      string                  `json:"test,omitempty"`
		Databases map[string]DatabaseSpec `json:"databases"`
	}{Test: testName, Databases: databases}
	return c.post(ctx, "/reset", request, nil)
}

// UpdateDatabase applies a batch of document changes in a single transaction.
func (c *Client) UpdateDatabase(ctx context.Context, database string, updates []DatabaseUpdateItem) error {
	request := struct {
		Database string               `json:"database"`
		Updates  []DatabaseUpdateItem `json:"updates"`
	}{Database: database, Updates: updates}
	return c.post(ctx, "/updateDatabase", request, nil)
}

// GetDocument returns a document's properties along with its _id and _revs metadata.  An absent or
// deleted document returns an error satisfying IsDocumentNotFound.
func (c *Client) GetDocument(ctx context.Context, database string, ref DocumentRef) (Document, error) {
	request := struct {
		Database string      `json:"database"`
		Document DocumentRef `json:"document"`
	}{Database: database, Document: ref}
	var document Document
	if err := c.post(ctx, "/getDocument", request, &document); err != nil {
		return nil, err
	}
	return document, nil
}

// GetAllDocuments returns the ID and current revision of every live document in each collection.
// Deleted documents are not included, and bodies are not returned - use GetDocument for those.
func (c *Client) GetAllDocuments(ctx context.Context, database string, collections []string) (map[string][]DocumentEntry, error) {
	request := struct {
		Database    string   `json:"database"`
		Collections []string `json:"collections"`
	}{Database: database, Collections: collections}
	var response map[string][]DocumentEntry
	if err := c.post(ctx, "/getAllDocuments", request, &response); err != nil {
		return nil, err
	}
	return response, nil
}

// StartReplicator creates and starts a replicator, returning its ID.  Pass reset to discard the
// existing checkpoint and replicate from the beginning.
func (c *Client) StartReplicator(ctx context.Context, config ReplicatorConfig, reset bool) (string, error) {
	request := struct {
		Config ReplicatorConfig `json:"config"`
		Reset  bool             `json:"reset,omitempty"`
	}{Config: config, Reset: reset}
	var response struct {
		ID string `json:"id"`
	}
	if err := c.post(ctx, "/startReplicator", request, &response); err != nil {
		return "", err
	}
	return response.ID, nil
}

// StopReplicator stops a replicator.  It returns once the request is accepted, not once the
// replicator has reached ReplicatorActivityStopped - poll ReplicatorStatus for that.
//
// A C test server built before the route was registered compiles the handler but never reaches
// it, and answers "Request API Not Found" instead.
func (c *Client) StopReplicator(ctx context.Context, replicatorID string) error {
	request := struct {
		ID string `json:"id"`
	}{ID: replicatorID}
	return c.post(ctx, "/stopReplicator", request, nil)
}

// ReplicatorStatus returns a replicator's current activity.  When the replicator was started with
// EnableDocumentListener, the returned Documents are those replicated since the previous call -
// the test server clears them once they are returned, so a caller that wants a running total has
// to accumulate them itself.
func (c *Client) ReplicatorStatus(ctx context.Context, replicatorID string) (ReplicatorStatus, error) {
	request := struct {
		ID string `json:"id"`
	}{ID: replicatorID}
	var status ReplicatorStatus
	err := c.post(ctx, "/getReplicatorStatus", request, &status)
	return status, err
}

// SnapshotDocuments records the current state of the given documents and returns a snapshot ID for
// VerifyDocuments.  Documents that are absent or deleted are recorded as null, which is what makes
// this the only way to assert that a document was deleted.
func (c *Client) SnapshotDocuments(ctx context.Context, database string, documents []DocumentRef) (string, error) {
	request := struct {
		Database  string        `json:"database"`
		Documents []DocumentRef `json:"documents"`
	}{Database: database, Documents: documents}
	var response struct {
		ID string `json:"id"`
	}
	if err := c.post(ctx, "/snapshotDocuments", request, &response); err != nil {
		return "", err
	}
	return response.ID, nil
}

// VerifyDocuments checks that applying changes to the snapshot produces the database's current
// state.  Documents in the snapshot that changes does not mention are expected to be unchanged.
func (c *Client) VerifyDocuments(ctx context.Context, database, snapshotID string, changes []DatabaseUpdateItem) (VerifyResult, error) {
	request := struct {
		Database string               `json:"database"`
		Snapshot string               `json:"snapshot"`
		Changes  []DatabaseUpdateItem `json:"changes"`
	}{Database: database, Snapshot: snapshotID, Changes: changes}
	var result VerifyResult
	err := c.post(ctx, "/verifyDocuments", request, &result)
	return result, err
}

// RunQuery runs a SQL++ query against a database and returns its results.
func (c *Client) RunQuery(ctx context.Context, database, query string) (any, error) {
	request := struct {
		Database string `json:"database"`
		Query    string `json:"query"`
	}{Database: database, Query: query}
	var response struct {
		Results any `json:"results"`
	}
	if err := c.post(ctx, "/runQuery", request, &response); err != nil {
		return nil, err
	}
	return response.Results, nil
}

// PerformMaintenance runs a maintenance operation on a database.
func (c *Client) PerformMaintenance(ctx context.Context, database string, maintenanceType MaintenanceType) error {
	request := struct {
		Database        string          `json:"database"`
		MaintenanceType MaintenanceType `json:"maintenanceType"`
	}{Database: database, MaintenanceType: maintenanceType}
	return c.post(ctx, "/performMaintenance", request, nil)
}

// post sends a POST with a JSON body, decoding the response into result when result is non-nil.
func (c *Client) post(ctx context.Context, path string, body, result any) error {
	return c.do(ctx, http.MethodPost, path, body, result)
}

// do sends one control API request.  Every path but "/" carries the API version and client ID
// headers; "/" is reachable before a session exists and takes neither.
func (c *Client) do(ctx context.Context, method, path string, body, result any) error {
	var bodyReader io.Reader
	if body != nil {
		encoded, err := base.JSONMarshal(body)
		if err != nil {
			return fmt.Errorf("cbltestclient: marshalling %s request: %w", path, err)
		}
		bodyReader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("cbltestclient: building %s request: %w", path, err)
	}
	if path != "/" {
		request.Header.Set(apiVersionHeader, strconv.Itoa(c.apiVersion))
		request.Header.Set(clientIDHeader, c.clientID)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	base.TracefCtx(ctx, base.KeySGTest, "cbltestclient %s %s%s", method, base.MD(c.baseURL), base.MD(path))
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("cbltestclient: %s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("cbltestclient: reading %s response: %w", path, err)
	}

	if response.StatusCode != http.StatusOK {
		return responseError(path, response.StatusCode, responseBody)
	}

	// A successful response can have an empty body ("/reset") or a literal null ("/newSession"),
	// so only decode when there is something to decode.
	trimmed := bytes.TrimSpace(responseBody)
	if result == nil || len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if err := base.JSONUnmarshal(responseBody, result); err != nil {
		return fmt.Errorf("cbltestclient: decoding %s response %q: %w", path, responseBody, err)
	}
	return nil
}

// responseError turns a non-200 into an APIError, wrapping ErrDocumentNotFound when the server is
// reporting an absent or deleted document.
func responseError(path string, statusCode int, body []byte) error {
	apiErr := &APIError{StatusCode: statusCode}
	if err := base.JSONUnmarshal(body, apiErr); err != nil {
		return fmt.Errorf("cbltestclient: %s failed with HTTP %d: %s", path, statusCode, body)
	}
	if isNotFoundResponse(statusCode, apiErr) {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, apiErr.Message)
	}
	return apiErr
}

// isNotFoundResponse reports whether an error response means the document was absent or deleted.
// The spec says 404, and the C test server reports one since couchbase-lite-tests#598, but a server
// built before that reports a 400 whose message is the only thing distinguishing it from a
// malformed request.
func isNotFoundResponse(statusCode int, apiErr *APIError) bool {
	if statusCode == http.StatusNotFound {
		return true
	}
	return statusCode == http.StatusBadRequest &&
		apiErr.Domain == ErrorDomainTestServer &&
		notFoundMessage.MatchString(apiErr.Message)
}
