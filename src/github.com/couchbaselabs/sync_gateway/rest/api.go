//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

const VersionString = "Couchbase Sync Gateway/0.51"

type context struct {
	dbcontext *db.DatabaseContext
	auth      *auth.Authenticator
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	response := map[string]string{
		"couchdb": "welcome",
		"version": VersionString,
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleOptions() error {
	//FIX: This is inaccurate; should figure out what methods the requested URL handles.
	h.setHeader("Accept", "GET, HEAD, PUT, DELETE, POST")
	return nil
}

func (h *handler) handleBadRoute() error {
	return &base.HTTPError{http.StatusMethodNotAllowed, "unknown route"}
}

func (h *handler) handleAllDbs() error {
	dbs := []string{}
	for _, db := range h.server.databases {
		dbs = append(dbs, db.dbcontext.Name)
	}
	h.writeJSON(dbs)
	return nil
}

func (h *handler) handleCompact() error {
	revsDeleted, err := h.db.Compact()
	if err != nil {
		return err
	}
	h.writeJSON(db.Body{"revs": revsDeleted})
	return nil
}

func (h *handler) handleVacuum() error {
	attsDeleted, err := db.VacuumAttachments(h.context.dbcontext.Bucket)
	if err != nil {
		return err
	}
	h.writeJSON(db.Body{"atts": attsDeleted})
	return nil
}

func (h *handler) handleCreateDB() error {
	if h.server.databases[h.PathVars()["newdb"]] != nil {
		return &base.HTTPError{http.StatusConflict, "already exists"}
	} else {
		return &base.HTTPError{http.StatusForbidden, "can't create any databases"}
	}
	return nil // unreachable
}

func (h *handler) handleGetDB() error {
	if h.rq.Method == "HEAD" {
		return nil
	}
	lastSeq := h.db.LastSequence()
	response := db.Body{
		"db_name":              h.db.Name,
		"doc_count":            h.db.DocCount(),
		"update_seq":           lastSeq,
		"committed_update_seq": lastSeq,
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleDeleteDB() error {
	if !h.admin {
		return &base.HTTPError{http.StatusForbidden, "forbidden (admins only)"}
	}
	return h.db.Delete()
}

func (h *handler) handleEFC() error { // Handles _ensure_full_commit.
	// no-op. CouchDB's replicator sends this, so don't barf. Status must be 201.
	h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true})
	return nil
}

func (h *handler) handleDesign() error {
	// we serve this content here so that CouchDB 1.2 has something to
	// hash into the replication-id, to correspond to our filter.
	filter := "ok"
	if h.db.DatabaseContext.ChannelMapper != nil {
		hash := sha1.New()
		io.WriteString(hash, h.db.DatabaseContext.ChannelMapper.Function())
		filter = fmt.Sprint(hash.Sum(nil))
	}
	h.writeJSON(db.Body{"filters": db.Body{"bychannel": filter}})
	return nil
}
