//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	httpprof "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/felixge/fgprof"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var mutexProfileRunning uint32
var blockProfileRunning uint32

const (
	profileStopped uint32 = iota
	profileRunning
)

type rootResponse struct {
	Admin   bool   `json:"ADMIN,omitempty"`
	CouchDB string `json:"couchdb,omitempty"` // TODO: Lithium - remove couchdb welcome
	Vendor  vendor `json:"vendor,omitempty"`
	Version string `json:"version,omitempty"`
}

type vendor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	resp := rootResponse{
		Admin:   h.privs == adminPrivs,
		CouchDB: "Welcome",
		Vendor: vendor{
			Name: base.ProductNameString,
		},
	}

	if h.shouldShowProductVersion() {
		resp.Version = base.LongVersionString
		resp.Vendor.Version = base.ProductVersionNumber
	}

	h.writeJSON(resp)
	return nil
}

func (h *handler) handleAllDbs() error {
	h.writeJSON(h.server.AllDatabaseNames())
	return nil
}

func (h *handler) handleGetCompact() error {
	compactionType := h.getQuery("type")
	if compactionType == "" {
		compactionType = "tombstone"
	}

	if compactionType != "tombstone" && compactionType != "attachment" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'type'. Must be 'tombstone' or 'attachment'")
	}

	var status []byte
	var err error
	if compactionType == "tombstone" {
		status, err = h.db.TombstoneCompactionManager.GetStatus()
	}

	if compactionType == "attachment" {
		status, err = h.db.AttachmentCompactionManager.GetStatus()
	}

	if err != nil {
		return err
	}
	h.writeRawJSON(status)

	return nil
}

func (h *handler) handleCompact() error {
	action := h.getQuery("action")
	if action == "" {
		action = string(db.BackgroundProcessActionStart)
	}

	if action != string(db.BackgroundProcessActionStart) && action != string(db.BackgroundProcessActionStop) {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'action'. Must be start or stop")
	}

	compactionType := h.getQuery("type")
	if compactionType == "" {
		compactionType = "tombstone"
	}

	if compactionType != "tombstone" && compactionType != "attachment" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'type'. Must be 'tombstone' or 'attachment'")
	}

	if compactionType == "tombstone" {
		if action == string(db.BackgroundProcessActionStart) {
			if atomic.CompareAndSwapUint32(&h.db.CompactState, db.DBCompactNotRunning, db.DBCompactRunning) {
				err := h.db.TombstoneCompactionManager.Start(map[string]interface{}{
					"database": h.db,
				})
				if err != nil {
					return err
				}

				status, err := h.db.TombstoneCompactionManager.GetStatus()
				if err != nil {
					return err
				}
				h.writeRawJSON(status)
			} else {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Database compact already in progress")

			}
		} else if action == string(db.BackgroundProcessActionStop) {
			dbState := atomic.LoadUint32(&h.db.CompactState)
			if dbState != db.DBCompactRunning {
				return base.HTTPErrorf(http.StatusBadRequest, "Database compact is not running")
			}

			err := h.db.TombstoneCompactionManager.Stop()
			if err != nil {
				return err
			}
			status, err := h.db.TombstoneCompactionManager.GetStatus()
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
		}
	}

	if compactionType == "attachment" {
		if action == string(db.BackgroundProcessActionStart) {
			err := h.db.AttachmentCompactionManager.Start(map[string]interface{}{
				"database": h.db,
				"reset":    h.getBoolQuery("reset"),
				"dryRun":   h.getBoolQuery("dry_run"),
			})
			if err != nil {
				return err
			}

			status, err := h.db.AttachmentCompactionManager.GetStatus()
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
		} else if action == string(db.BackgroundProcessActionStop) {
			err := h.db.AttachmentCompactionManager.Stop()
			if err != nil {
				return err
			}

			status, err := h.db.AttachmentCompactionManager.GetStatus()
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
		}
	}

	return nil
}

func (h *handler) handleFlush() error {

	baseBucket := base.GetBaseBucket(h.db.Bucket)

	// If it can be flushed, then flush it
	if _, ok := baseBucket.(sgbucket.FlushableStore); ok {

		// If it's not a walrus bucket, don't allow flush unless the unsupported config is set
		if !h.db.BucketSpec.IsWalrusBucket() {
			if !h.db.DatabaseContext.AllowFlushNonCouchbaseBuckets() {
				msg := "Flush not allowed on Couchbase buckets by default."
				return fmt.Errorf(msg)
			}
		}

		name := h.db.Name
		config := h.server.GetDatabaseConfig(name)

		// This needs to first call RemoveDatabase since flushing the bucket under Sync Gateway might cause issues.
		h.server.RemoveDatabase(name)

		// Create a bucket connection spec from the database config
		spec, err := GetBucketSpec(config, h.server.config)
		if err != nil {
			return err
		}

		// Manually re-open a temporary bucket connection just for flushing purposes
		tempBucketForFlush, err := db.ConnectToBucket(spec)
		if err != nil {
			return err
		}
		defer tempBucketForFlush.Close() // Close the temporary connection to the bucket that was just for purposes of flushing it

		// Flush the bucket (assuming it conforms to sgbucket.DeleteableStore interface
		if tempBucketForFlush, ok := tempBucketForFlush.(sgbucket.FlushableStore); ok {

			// Flush
			err := tempBucketForFlush.Flush()
			if err != nil {
				return err
			}

		}

		// Re-open database and add to Sync Gateway
		_, err2 := h.server.AddDatabaseFromConfig(*config)
		if err2 != nil {
			return err2
		}

	} else if bucket, ok := baseBucket.(sgbucket.DeleteableStore); ok {

		// If it's not flushable, but it's deletable, then delete it

		name := h.db.Name
		config := h.server.GetDatabaseConfig(name)
		h.server.RemoveDatabase(name)
		err := bucket.CloseAndDelete()
		_, err2 := h.server.AddDatabaseFromConfig(*config)
		if err == nil {
			err = err2
		}
		return err

	} else {

		return base.HTTPErrorf(http.StatusServiceUnavailable, "Bucket does not support flush or delete")

	}

	return nil

}

func (h *handler) handleGetResync() error {
	status, err := h.db.ResyncManager.GetStatus()
	if err != nil {
		return err
	}
	h.writeRawJSON(status)
	return nil
}

func (h *handler) handlePostResync() error {
	action := h.getQuery("action")
	regenerateSequences, _ := h.getOptBoolQuery("regenerate_sequences", false)

	if action != "" && action != string(db.BackgroundProcessActionStart) && action != string(db.BackgroundProcessActionStop) {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'action'. Must be start or stop")
	}

	if action == "" {
		action = string(db.BackgroundProcessActionStart)
	}

	if action == string(db.BackgroundProcessActionStart) {
		if atomic.CompareAndSwapUint32(&h.db.State, db.DBOffline, db.DBResyncing) {
			err := h.db.ResyncManager.Start(map[string]interface{}{
				"database":            h.db,
				"regenerateSequences": regenerateSequences,
			})
			if err != nil {
				return err
			}

			status, err := h.db.ResyncManager.GetStatus()
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
		} else {
			dbState := atomic.LoadUint32(&h.db.State)
			if dbState == db.DBResyncing {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Database _resync already in progress")
			}

			if dbState != db.DBOffline {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Database must be _offline before calling _resync")
			}
		}

	} else if action == string(db.BackgroundProcessActionStop) {
		dbState := atomic.LoadUint32(&h.db.State)
		if dbState != db.DBResyncing {
			return base.HTTPErrorf(http.StatusBadRequest, "Database _resync is not running")
		}

		err := h.db.ResyncManager.Stop()
		if err != nil {
			return err
		}

		status, err := h.db.ResyncManager.GetStatus()
		if err != nil {
			return err
		}
		h.writeRawJSON(status)
	}

	return nil
}

type PostUpgradeResponse struct {
	Result  PostUpgradeResult `json:"post_upgrade_results"`
	Preview bool              `json:"preview,omitempty"`
}

func (h *handler) handlePostUpgrade() error {

	preview := h.getBoolQuery("preview")

	postUpgradeResults, err := h.server.PostUpgrade(preview)
	if err != nil {
		return err
	}

	result := &PostUpgradeResponse{
		Result:  postUpgradeResults,
		Preview: preview,
	}

	h.writeJSON(result)
	return nil
}

func (h *handler) instanceStartTimeMicro() int64 {
	return h.db.StartTime.UnixMicro()
}

type DatabaseRoot struct {
	DBName                        string `json:"db_name"`
	SequenceNumber                uint64 `json:"update_seq"`
	CommittedUpdateSequenceNumber uint64 `json:"committed_update_seq"` // Used by perf tests, shouldn't be removed
	InstanceStartTimeMicro        int64  `json:"instance_start_time"`  // microseconds since epoch
	CompactRunning                bool   `json:"compact_running"`
	PurgeSequenceNumber           uint64 `json:"purge_seq"`
	DiskFormatVersion             uint64 `json:"disk_format_version"`
	State                         string `json:"state"`
	ServerUUID                    string `json:"server_uuid,omitempty"`
}

func (h *handler) handleGetDB() error {
	if h.rq.Method == "HEAD" {
		return nil
	}

	lastSeq := uint64(0)
	runState := db.RunStateString[atomic.LoadUint32(&h.db.State)]

	// Don't bother trying to lookup LastSequence() if offline
	if runState != db.RunStateString[db.DBOffline] {
		lastSeq, _ = h.db.LastSequence()
	}

	var response = DatabaseRoot{
		DBName:                        h.db.Name,
		SequenceNumber:                lastSeq,
		CommittedUpdateSequenceNumber: lastSeq,
		InstanceStartTimeMicro:        h.instanceStartTimeMicro(),
		CompactRunning:                h.db.IsCompactRunning(),
		PurgeSequenceNumber:           0, // TODO: Should track this value
		DiskFormatVersion:             0, // Probably meaningless, but add for compatibility
		State:                         runState,
		ServerUUID:                    h.db.DatabaseContext.GetServerUUID(),
	}

	h.writeJSON(response)
	return nil
}

// Stub handler for hadling create DB on the public API returns HTTP status 412
// if the db exists, and 403 if it doesn't.
// fixes issue #562
func (h *handler) handleCreateTarget() error {
	dbname := h.PathVar("targetdb")
	if _, err := h.server.GetDatabase(dbname); err != nil {
		return base.HTTPErrorf(http.StatusForbidden, "Creating a DB over the public API is unsupported")
	} else {
		return base.HTTPErrorf(http.StatusPreconditionFailed, "Database already exists")
	}
}

func (h *handler) handleEFC() error { // Handles _ensure_full_commit.
	// no-op. CouchDB's replicator sends this, so don't barf. Status must be 201.
	h.writeRawJSONStatus(http.StatusCreated, []byte(`{"instance_start_time":`+strconv.FormatInt(h.instanceStartTimeMicro(), 10)+`,"ok":true}`))
	return nil
}

// ADMIN API to turn Go CPU profiling on/off
func (h *handler) handleProfiling() error {
	profileName := h.PathVar("profilename")
	isCPUProfile := profileName == ""

	var params struct {
		File string `json:"file"`
	}
	body, err := h.readBody()
	if err != nil {
		return err
	}
	if len(body) > 0 {
		if err = base.JSONUnmarshal(body, &params); err != nil {
			return err
		}
	}

	// Handle no file
	if params.File == "" {
		if isCPUProfile {
			base.InfofCtx(h.ctx(), base.KeyAll, "... ending CPU profile")
			pprof.StopCPUProfile()
			h.server.CloseCpuPprofFile()
			return nil
		}
		return base.HTTPErrorf(http.StatusBadRequest, "Missing JSON 'file' parameter")
	}

	f, err := os.Create(params.File)
	if err != nil {
		return err
	}

	if isCPUProfile {
		base.InfofCtx(h.ctx(), base.KeyAll, "Starting CPU profile to %s ...", base.UD(params.File))
		if err = pprof.StartCPUProfile(f); err != nil {
			if fileError := os.Remove(params.File); fileError != nil {
				base.InfofCtx(h.ctx(), base.KeyAll, "Error removing file: %s", base.UD(params.File))
			}
			return err
		}
		h.server.SetCpuPprofFile(f)
		return err
	} else if profile := pprof.Lookup(profileName); profile != nil {
		base.InfofCtx(h.ctx(), base.KeyAll, "Writing %q profile to %s ...", profileName, base.UD(params.File))
		err = profile.WriteTo(f, 0)
	} else {
		err = base.HTTPErrorf(http.StatusNotFound, "No such profile %q", profileName)
	}

	if fileCloseError := f.Close(); fileCloseError != nil {
		base.WarnfCtx(h.ctx(), "Error closing profile file: %v", fileCloseError)
	}

	return err
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
	if err = base.JSONUnmarshal(body, &params); err != nil {
		return err
	}

	base.InfofCtx(h.ctx(), base.KeyAll, "Dumping heap profile to %s ...", base.UD(params.File))
	f, err := os.Create(params.File)
	if err != nil {
		return err
	}
	err = pprof.WriteHeapProfile(f)

	if fileCloseError := f.Close(); fileCloseError != nil {
		base.WarnfCtx(h.ctx(), "Error closing profile file: %v", fileCloseError)
	}

	return err
}

func (h *handler) handlePprofGoroutine() error {
	httpprof.Handler("goroutine").ServeHTTP(h.response, h.rq)
	return nil
}

// Go execution tracer
func (h *handler) handlePprofTrace() error {
	httpprof.Trace(h.response, h.rq)
	return nil
}

func (h *handler) handlePprofCmdline() error {
	httpprof.Cmdline(h.response, h.rq)
	return nil
}

func (h *handler) handlePprofSymbol() error {
	httpprof.Symbol(h.response, h.rq)
	return nil
}

func (h *handler) handlePprofHeap() error {
	httpprof.Handler("heap").ServeHTTP(h.response, h.rq)
	return nil
}

func (h *handler) handlePprofProfile() error {
	httpprof.Profile(h.response, h.rq)
	return nil
}

func (h *handler) handleFgprof() error {
	sec, err := strconv.ParseInt(h.rq.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}
	stopFn := fgprof.Start(h.response, fgprof.FormatPprof)
	select {
	case <-time.After(time.Duration(sec) * time.Second):
	case <-h.rq.Context().Done():
	}
	return stopFn()
}

func (h *handler) handlePprofBlock() error {
	sec, err := strconv.ParseInt(h.rq.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}
	if !atomic.CompareAndSwapUint32(&blockProfileRunning, profileStopped, profileRunning) {
		return base.HTTPErrorf(http.StatusForbidden, "Can only run one block profile at a time")
	}
	runtime.SetBlockProfileRate(1)
	sleep(h.rq, time.Duration(sec)*time.Second)
	httpprof.Handler("block").ServeHTTP(h.response, h.rq)
	runtime.SetBlockProfileRate(0)
	atomic.StoreUint32(&blockProfileRunning, profileStopped)
	return nil
}

func (h *handler) handlePprofThreadcreate() error {
	httpprof.Handler("threadcreate").ServeHTTP(h.response, h.rq)
	return nil
}

func (h *handler) handlePprofMutex() error {
	sec, err := strconv.ParseInt(h.rq.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}
	if !atomic.CompareAndSwapUint32(&mutexProfileRunning, profileStopped, profileRunning) {
		return base.HTTPErrorf(http.StatusForbidden, "Can only run one mutex profile at a time")
	}
	runtime.SetMutexProfileFraction(1)
	sleep(h.rq, time.Duration(sec)*time.Second)
	httpprof.Handler("mutex").ServeHTTP(h.response, h.rq)
	runtime.SetMutexProfileFraction(0)
	atomic.StoreUint32(&mutexProfileRunning, profileStopped)
	return nil
}

type stats struct {
	MemStats runtime.MemStats
}

func sleep(rq *http.Request, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-rq.Context().Done():
	}
}

// ADMIN API to expose runtime and other stats
func (h *handler) handleStats() error {
	st := stats{}
	runtime.ReadMemStats(&st.MemStats)

	h.writeJSON(st)
	return nil
}

func (h *handler) handleMetrics() error {
	promhttp.Handler().ServeHTTP(h.response, h.rq)

	return nil
}
