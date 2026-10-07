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

	// datasetVersion is the dataset version NewSession sends.  It only matters for datasets, which
	// Sync Gateway's tests don't use, but the C test server requires the field to be present.
	datasetVersion = "4.0"

	// defaultRequestTimeout bounds a single control API call, so a wedged test server surfaces as
	// an error rather than a hung test.
	defaultRequestTimeout = 30 * time.Second
)

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
// its API version, along with what the server reported about itself.  Call NewSession before any
// other method: every endpoint but GET / needs a session to exist.
func NewClient(ctx context.Context, baseURL string) (*Client, ServerInfo, error) {
	client := &Client{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: defaultRequestTimeout},
		clientID:   uuid.NewString(),
	}
	info, err := client.ServerInfo(ctx)
	if err != nil {
		return nil, ServerInfo{}, err
	}
	client.apiVersion = info.APIVersion
	return client, info, nil
}

// ServerInfo returns the test server's version and API version.  It is the only endpoint that works
// before a session exists.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	var info ServerInfo
	err := c.do(ctx, http.MethodGet, "/", nil, &info)
	return info, err
}

// NewSession creates the session every other endpoint needs, identified by the client's ID.  The
// test server keeps exactly one session and deletes its whole sessions directory when a new one is
// created, so this must be called once per server process, never once per test.
func (c *Client) NewSession(ctx context.Context) error {
	request := struct {
		ID             string `json:"id"`
		DatasetVersion string `json:"dataset_version"`
	}{ID: c.clientID, DatasetVersion: datasetVersion}
	return c.do(ctx, http.MethodPost, "/newSession", request, nil)
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
	return c.do(ctx, http.MethodPost, "/reset", request, nil)
}

// UpdateDatabase applies a batch of document changes in a single transaction.
func (c *Client) UpdateDatabase(ctx context.Context, database string, updates []DatabaseUpdateItem) error {
	request := struct {
		Database string               `json:"database"`
		Updates  []DatabaseUpdateItem `json:"updates"`
	}{Database: database, Updates: updates}
	return c.do(ctx, http.MethodPost, "/updateDatabase", request, nil)
}

// GetDocument returns a document's properties along with its _id and _revs metadata.  An absent or
// deleted document returns an error satisfying IsDocumentNotFound.
func (c *Client) GetDocument(ctx context.Context, database string, ref DocumentRef) (Document, error) {
	request := struct {
		Database string      `json:"database"`
		Document DocumentRef `json:"document"`
	}{Database: database, Document: ref}
	var document Document
	if err := c.do(ctx, http.MethodPost, "/getDocument", request, &document); err != nil {
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
	if err := c.do(ctx, http.MethodPost, "/getAllDocuments", request, &response); err != nil {
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
	if err := c.do(ctx, http.MethodPost, "/startReplicator", request, &response); err != nil {
		return "", err
	}
	return response.ID, nil
}

// StopReplicator stops a replicator.  It returns once the request is accepted, not once the
// replicator has reached ReplicatorActivityStopped - poll ReplicatorStatus for that.
func (c *Client) StopReplicator(ctx context.Context, replicatorID string) error {
	request := struct {
		ID string `json:"id"`
	}{ID: replicatorID}
	return c.do(ctx, http.MethodPost, "/stopReplicator", request, nil)
}

// ReplicatorStatus returns a replicator's current activity.
func (c *Client) ReplicatorStatus(ctx context.Context, replicatorID string) (ReplicatorStatus, error) {
	request := struct {
		ID string `json:"id"`
	}{ID: replicatorID}
	var status ReplicatorStatus
	err := c.do(ctx, http.MethodPost, "/getReplicatorStatus", request, &status)
	return status, err
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

// responseError turns a non-200 into an APIError, wrapping ErrDocumentNotFound for a 404, which is
// how the test server reports an absent or deleted document.
func responseError(path string, statusCode int, body []byte) error {
	apiErr := &APIError{StatusCode: statusCode}
	if err := base.JSONUnmarshal(body, apiErr); err != nil {
		return fmt.Errorf("cbltestclient: %s failed with HTTP %d: %s", path, statusCode, body)
	}
	if statusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, apiErr.Message)
	}
	return apiErr
}
