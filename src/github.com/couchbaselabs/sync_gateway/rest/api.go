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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

const VersionString = "Couchbase Sync Gateway/0.81"

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	response := map[string]interface{}{
		"couchdb": "welcome",
		"version": VersionString,
	}
	if h.privs == adminPrivs {
		response["ADMIN"] = true
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleAllDbs() error {
	h.writeJSON(h.server.AllDatabaseNames())
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
	attsDeleted, err := db.VacuumAttachments(h.db.Bucket)
	if err != nil {
		return err
	}
	h.writeJSON(db.Body{"atts": attsDeleted})
	return nil
}

func (h *handler) instanceStartTime() json.Number {
	return json.Number(strconv.FormatInt(h.db.StartTime.UnixNano()/1000, 10))
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
		"instance_start_time":  h.instanceStartTime(),
		"compact_running":      false, // TODO: Implement this
		"purge_seq":            0,     // TODO: Should track this value
		"disk_format_version":  0,     // Probably meaningless, but add for compatibility
	}
	h.writeJSON(response)
	return nil
}

func (h *handler) handleEFC() error { // Handles _ensure_full_commit.
	// no-op. CouchDB's replicator sends this, so don't barf. Status must be 201.
	h.writeJSONStatus(http.StatusCreated, db.Body{
		"ok": true,
		"instance_start_time": h.instanceStartTime(),
	})
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

// ADMIN API to turn Go CPU profiling on/off
func (h *handler) handleProfiling() error {
	var params struct {
		File string `json:"file"`
	}
	body, err := ioutil.ReadAll(h.rq.Body)
	if err != nil {
		return err
	}
	if len(body) > 0 {
		if err = json.Unmarshal(body, &params); err != nil {
			return err
		}
	}

	if params.File != "" {
		base.Log("Profiling to %s ...", params.File)
		f, err := os.Create(params.File)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
	} else {
		base.Log("...ending profile.")
		pprof.StopCPUProfile()
	}
	return nil
}
