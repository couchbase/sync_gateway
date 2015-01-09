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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
	"github.com/couchbaselabs/walrus"
)

const ServerName = "Couchbase Sync Gateway"
const VersionNumber float64 = 1.0                    // API/feature level
const VersionBuildNumberString = "@PRODUCT_VERSION@" // Real string substituted by Gerrit
const VersionCommitSHA = "@COMMIT_SHA@"              // Real string substituted by Gerrit

// This appears in the "Server:" header of HTTP responses
var VersionString string

// This includes build number; appears in the response of "GET /" and the initial log message
var LongVersionString string

func init() {
	if VersionBuildNumberString[0] != '@' {
		//Split version number and build number (optional)
		versionTokens := strings.Split(VersionBuildNumberString, "-")
		BuildVersionString := versionTokens[0]
		var BuildNumberString string
		if len(versionTokens) > 1 {
			BuildNumberString = fmt.Sprintf("%s;", versionTokens[1])
		}
		LongVersionString = fmt.Sprintf("%s/%s(%s%.7s)",
			ServerName, BuildVersionString, BuildNumberString, VersionCommitSHA)

		VersionString = fmt.Sprintf("%s/%.2f", ServerName, VersionNumber)
	} else {
		LongVersionString = fmt.Sprintf("%s/%s(%.7s%s)", ServerName, GitBranch, GitCommit, GitDirty)
		VersionString = fmt.Sprintf("%s/unofficial", ServerName)
	}
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	response := map[string]interface{}{
		"couchdb": "Welcome",
		"version": LongVersionString,
		"vendor":  db.Body{"name": ServerName, "version": VersionNumber},
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

func (h *handler) handleFlush() error {
	if bucket, ok := h.db.Bucket.(walrus.DeleteableBucket); ok {
		name := h.db.Name
		config := h.server.GetDatabaseConfig(name)
		h.server.RemoveDatabase(name)
		err := bucket.CloseAndDelete()
		_, err2 := h.server.AddDatabaseFromConfig(config)
		if err == nil {
			err = err2
		}
		return err
	} else {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Bucket does not support flush")
	}
}

func (h *handler) handleResync() error {
	docsChanged, err := h.db.UpdateAllDocChannels(true, false)
	if err != nil {
		return err
	}
	h.writeJSON(db.Body{"changes": docsChanged})
	return nil
}

func (h *handler) instanceStartTime() json.Number {
	return json.Number(strconv.FormatInt(h.db.StartTime.UnixNano()/1000, 10))
}

func (h *handler) handleGetDB() error {
	if h.rq.Method == "HEAD" {
		return nil
	}
	lastSeq, err := h.db.LastSequence()
	if err != nil {
		return err
	}
	response := db.Body{
		"db_name":              h.db.Name,
		"update_seq":           lastSeq,
		"committed_update_seq": lastSeq,
		"instance_start_time":  h.instanceStartTime(),
		"compact_running":      false, // TODO: Implement this
		"purge_seq":            0,     // TODO: Should track this value
		"disk_format_version":  0,     // Probably meaningless, but add for compatibility
		//"doc_count":          h.db.DocCount(), // Removed: too expensive to compute (#278)
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

// ADMIN API to turn Go CPU profiling on/off
func (h *handler) handleProfiling() error {
	profileName := h.PathVar("name")
	var params struct {
		File string `json:"file"`
	}
	body, err := h.readBody()
	if err != nil {
		return err
	}
	if len(body) > 0 {
		if err = json.Unmarshal(body, &params); err != nil {
			return err
		}
	}

	if params.File != "" {
		f, err := os.Create(params.File)
		if err != nil {
			return err
		}
		if profileName != "" {
			defer f.Close()
			if profile := pprof.Lookup(profileName); profile != nil {
				profile.WriteTo(f, 0)
				base.Log("Wrote %s profile to %s", profileName, params.File)
			} else {
				return base.HTTPErrorf(http.StatusNotFound, "No such profile %q", profileName)
			}
		} else {
			base.Log("Starting CPU profile to %s ...", params.File)
			pprof.StartCPUProfile(f)
		}
	} else {
		if profileName != "" {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing JSON 'file' parameter")
		} else {
			base.Log("...ending CPU profile.")
			pprof.StopCPUProfile()
		}
	}
	return nil
}

// ADMIN API to dump Go heap profile
func (h *handler) handleHeapProfiling() error {
	var params struct {
		File string `json:"file"`
	}
	body, err := h.readBody()
	if err != nil {
		return err
	}
	if err = json.Unmarshal(body, &params); err != nil {
		return err
	}

	base.Log("Dumping heap profile to %s ...", params.File)
	f, err := os.Create(params.File)
	if err != nil {
		return err
	}
	pprof.WriteHeapProfile(f)
	f.Close()
	return nil
}

type stats struct {
	MemStats runtime.MemStats
}

// ADMIN API to expose runtime and other stats
func (h *handler) handleStats() error {
	st := stats{}
	runtime.ReadMemStats(&st.MemStats)

	h.writeJSON(st)
	return nil
}
