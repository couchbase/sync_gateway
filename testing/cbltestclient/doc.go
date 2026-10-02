// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package cbltestclient drives a real Couchbase Lite client from Go tests.
//
// It talks to the CBL-C "test server" from couchbaselabs/couchbase-lite-tests, a real Couchbase
// Lite application that exposes its database and replicator over an HTTP/JSON control API.  That
// gives Sync Gateway tests a genuine client to replicate against without linking libcblite into
// the Sync Gateway build.
//
// Client is a plain HTTP client for the control API and knows nothing about testing.  GetServer
// finds a test server build, runs it once per test binary, and hands out a ready Client.
package cbltestclient
