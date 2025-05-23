//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisablePublicAllDocs(t *testing.T) {
	tests := []struct {
		name                 string
		disablePublicAllDocs *bool
		expectedPublicStatus int
		expectedPublicError  string
	}{
		{
			name:                 "default",
			disablePublicAllDocs: nil,
			expectedPublicStatus: http.StatusOK,
		},
		{
			name:                 "disabled",
			disablePublicAllDocs: base.Ptr(true),
			expectedPublicStatus: http.StatusForbidden,
			expectedPublicError:  "public access to _all_docs is disabled for this database",
		},
		{
			name:                 "enabled",
			disablePublicAllDocs: base.Ptr(false),
			expectedPublicStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rtConfig := RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{
					DbConfig: DbConfig{
						DisablePublicAllDocs: test.disablePublicAllDocs,
					},
				},
			}
			rt := NewRestTester(t, &rtConfig)
			defer rt.Close()

			rt.CreateUser("user1", nil)
			rt.CreateTestDoc("doc1")
			rt.CreateTestDoc("doc2")

			t.Run("public", func(t *testing.T) {
				response := rt.SendUserRequest("GET", "/{{.keyspace}}/_all_docs", "", "user1")
				RequireStatus(t, response, test.expectedPublicStatus)
				if test.expectedPublicError != "" {
					require.Contains(t, response.Body.String(), test.expectedPublicError)
				}
			})

			t.Run("admin", func(t *testing.T) {
				response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_all_docs", "")
				RequireStatus(t, response, http.StatusOK)
				var result allDocsResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				assert.Equal(t, 2, len(result.Rows))
				assert.Equal(t, "doc1", result.Rows[0].ID)
				assert.Equal(t, "doc2", result.Rows[1].ID)
			})
		})
	}
}
