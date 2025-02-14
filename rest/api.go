//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"errors"
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

const (
	compactionTypeTombstone  = "tombstone"
	compactionTypeAttachment = "attachment"
)

type rootResponse struct {
	Admin            bool   `json:"ADMIN,omitempty"`
	CouchDB          string `json:"couchdb,omitempty"` // TODO: Lithium - remove couchdb welcome
	Vendor           vendor `json:"vendor,omitempty"`
	Version          string `json:"version,omitempty"`
	PersistentConfig bool   `json:"persistent_config"`
}

type vendor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// HTTP handler for the root ("/")
func (h *handler) handleRoot() error {
	resp := rootResponse{
		Admin:   h.serverType == adminServer,
		CouchDB: "Welcome",
		Vendor: vendor{
			Name: base.ProductNameString,
		},
		PersistentConfig: h.server.persistentConfig,
	}

	if h.shouldShowProductVersion() {
		resp.Version = base.LongVersionString
		resp.Vendor.Version = base.ProductAPIVersion
	}

	h.writeJSON(resp)
	return nil
}

// HTTP handler for a simple ping healthcheck
func (h *handler) handlePing() error {
	h.writeTextStatus(http.StatusOK, []byte("OK"))
	return nil
}

func (h *handler) handleAllDbs() error {
	verbose := h.getBoolQuery("verbose")
	var dbNames []string
	if verbose {
		summaries := h.server.allDatabaseSummaries()
		for _, summary := range summaries {
			dbNames = append(dbNames, summary.DBName)
		}
		h.writeJSON(summaries)
	} else {
		dbNames = h.server.AllDatabaseNames()
		h.writeJSON(dbNames)
	}
	base.Audit(h.ctx(), base.AuditIDDatabaseAllRead, base.AuditFields{base.AuditFieldDBNames: dbNames, "verbose": verbose})
	return nil
}

func (h *handler) handleGetCompact() error {
	compactionType := h.getQuery("type")
	if compactionType == "" {
		compactionType = compactionTypeTombstone
	}

	if compactionType != compactionTypeTombstone && compactionType != compactionTypeAttachment {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'type'. Must be 'tombstone' or 'attachment'")
	}

	auditFields := base.AuditFields{base.AuditFieldCompactionType: compactionType}
	var status []byte
	var err error
	if compactionType == compactionTypeTombstone {
		status, err = h.db.TombstoneCompactionManager.GetStatus(h.ctx())
		if err != nil {
			return err
		}
		base.Audit(h.ctx(), base.AuditIDDatabaseCompactStatus, auditFields)
	}

	if compactionType == compactionTypeAttachment {
		status, err = h.db.AttachmentCompactionManager.GetStatus(h.ctx())
		if err != nil {
			return err
		}
		base.Audit(h.ctx(), base.AuditIDDatabaseCompactStatus, auditFields)
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
		compactionType = compactionTypeTombstone
	}

	if compactionType != compactionTypeTombstone && compactionType != compactionTypeAttachment {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'type'. Must be 'tombstone' or 'attachment'")
	}

	auditFields := base.AuditFields{base.AuditFieldCompactionType: compactionType}
	if compactionType == compactionTypeTombstone {
		if action == string(db.BackgroundProcessActionStart) {
			if atomic.CompareAndSwapUint32(&h.db.CompactState, db.DBCompactNotRunning, db.DBCompactRunning) {
				err := h.db.TombstoneCompactionManager.Start(h.ctx(), map[string]interface{}{
					"database": h.db,
				})
				if err != nil {
					return err
				}

				status, err := h.db.TombstoneCompactionManager.GetStatus(h.ctx())
				if err != nil {
					return err
				}
				h.writeRawJSON(status)
				base.Audit(h.ctx(), base.AuditIDDatabaseCompactStart, auditFields)
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
			status, err := h.db.TombstoneCompactionManager.GetStatus(h.ctx())
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
			base.Audit(h.ctx(), base.AuditIDDatabaseCompactStop, auditFields)
		}
	}

	if compactionType == compactionTypeAttachment {
		if action == string(db.BackgroundProcessActionStart) {
			err := h.db.AttachmentCompactionManager.Start(h.ctx(), map[string]interface{}{
				"database": h.db,
				"reset":    h.getBoolQuery("reset"),
				"dryRun":   h.getBoolQuery("dry_run"),
			})
			if err != nil {
				return err
			}

			status, err := h.db.AttachmentCompactionManager.GetStatus(h.ctx())
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
			auditFields[base.AuditFieldCompactionReset] = h.getBoolQuery("reset")
			auditFields[base.AuditFieldCompactionReset] = h.getBoolQuery("dry_run")
			base.Audit(h.ctx(), base.AuditIDDatabaseCompactStart, auditFields)
		} else if action == string(db.BackgroundProcessActionStop) {
			err := h.db.AttachmentCompactionManager.Stop()
			if err != nil {
				return err
			}

			status, err := h.db.AttachmentCompactionManager.GetStatus(h.ctx())
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
			base.Audit(h.ctx(), base.AuditIDDatabaseCompactStop, auditFields)
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
				return errors.New("Flush not allowed on Couchbase buckets by default.")
			}
		}

		name := h.db.Name
		config := h.server.GetDatabaseConfig(name)

		// This needs to first call RemoveDatabase since flushing the bucket under Sync Gateway might cause issues.
		h.server.RemoveDatabase(h.ctx(), name)

		// Create a bucket connection spec from the database config
		spec, err := GetBucketSpec(h.ctx(), &config.DatabaseConfig, h.server.Config)
		if err != nil {
			return err
		}

		// Manually re-open a temporary bucket connection just for flushing purposes
		tempBucketForFlush, err := db.ConnectToBucket(h.ctx(), spec, false)
		if err != nil {
			return err
		}
		defer tempBucketForFlush.Close(h.ctx()) // Close the temporary connection to the bucket that was just for purposes of flushing it

		// Flush the bucket (assuming it conforms to sgbucket.DeleteableStore interface
		if tempBucketForFlush, ok := tempBucketForFlush.(sgbucket.FlushableStore); ok {

			// Flush
			err := tempBucketForFlush.Flush()
			if err != nil {
				return err
			}

		}

		// Re-open database and add to Sync Gateway
		_, err2 := h.server.AddDatabaseFromConfig(h.ctx(), config.DatabaseConfig)
		if err2 != nil {
			return err2
		}
		base.Audit(h.ctx(), base.AuditIDDatabaseFlush, nil)

	} else if bucket, ok := baseBucket.(sgbucket.DeleteableStore); ok {

		// If it's not flushable, but it's deletable, then delete it

		name := h.db.Name
		config := h.server.GetDatabaseConfig(name)
		h.server.RemoveDatabase(h.ctx(), name)
		err := bucket.CloseAndDelete(h.ctx())
		_, err2 := h.server.AddDatabaseFromConfig(h.ctx(), config.DatabaseConfig)
		if err != nil {
			return err
		} else if err2 != nil {
			return err2
		}
		base.Audit(h.ctx(), base.AuditIDDatabaseFlush, nil)
		return nil
	} else {

		return base.HTTPErrorf(http.StatusServiceUnavailable, "Bucket does not support flush or delete")

	}

	return nil

}

func (h *handler) handleGetResync() error {
	status, err := h.db.ResyncManager.GetStatus(h.ctx())
	if err != nil {
		return err
	}
	h.writeRawJSON(status)
	base.Audit(h.ctx(), base.AuditIDDatabaseResyncStatus, nil)
	return nil
}

// ResyncPostReqBody represents Resync POST body to run resync for custom collections
type ResyncPostReqBody struct {
	Scope               db.ResyncCollections `json:"scopes,omitempty"`
	RegenerateSequences bool                 `json:"regenerate_sequences,omitempty"`
}

func (h *handler) handlePostResync() error {

	action := h.getQuery("action")
	regenerateSequences, _ := h.getOptBoolQuery("regenerate_sequences", false)
	reset := h.getBoolQuery("reset")

	body, err := h.readBody()
	if err != nil {
		return err
	}

	resyncPostReqBody := ResyncPostReqBody{}
	if len(body) != 0 {
		if err := json.Unmarshal(body, &resyncPostReqBody); err != nil {
			return err
		}
	}

	if action != "" && action != string(db.BackgroundProcessActionStart) && action != string(db.BackgroundProcessActionStop) {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter for 'action'. Must be start or stop")
	}

	if action == "" {
		action = string(db.BackgroundProcessActionStart)
	}

	// Regenerate sequences if it is set true via query param or via request body
	regenerateSequences = regenerateSequences || resyncPostReqBody.RegenerateSequences

	if action == string(db.BackgroundProcessActionStart) {
		if atomic.CompareAndSwapUint32(&h.db.State, db.DBOffline, db.DBResyncing) {
			err := h.db.ResyncManager.Start(h.ctx(), map[string]interface{}{
				"database":            h.db,
				"regenerateSequences": regenerateSequences,
				"collections":         resyncPostReqBody.Scope,
				"reset":               reset,
			})
			if err != nil {
				return err
			}

			status, err := h.db.ResyncManager.GetStatus(h.ctx())
			if err != nil {
				return err
			}
			h.writeRawJSON(status)
			base.Audit(h.ctx(), base.AuditIDDatabaseResyncStart, base.AuditFields{
				"collections":          resyncPostReqBody.Scope,
				"regenerate_sequences": regenerateSequences,
				"reset":                reset,
			})
		} else {
			dbState := atomic.LoadUint32(&h.db.State)
			if dbState == db.DBResyncing {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Database _resync already in progress")
			}

			if dbState != db.DBOffline {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Database must be _offline before calling _resync, current state: %s", db.RunStateString[dbState])
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

		status, err := h.db.ResyncManager.GetStatus(h.ctx())
		if err != nil {
			return err
		}
		h.writeRawJSON(status)

		base.Audit(h.ctx(), base.AuditIDDatabaseResyncStop, nil)

	}

	return nil
}

type PostUpgradeResponse struct {
	Result  PostUpgradeResult `json:"post_upgrade_results"`
	Preview bool              `json:"preview,omitempty"`
}

func (h *handler) handlePostUpgrade() error {

	preview := h.getBoolQuery("preview")

	postUpgradeResults, err := h.server.PostUpgrade(h.ctx(), preview)
	if err != nil {
		return err
	}

	result := &PostUpgradeResponse{
		Result:  postUpgradeResults,
		Preview: preview,
	}

	h.writeJSON(result)
	base.Audit(h.ctx(), base.AuditIDPostUpgrade, base.AuditFields{base.AuditFieldPostUpgradePreview: preview})
	return nil
}

func (h *handler) instanceStartTimeMicro() int64 {
	return h.db.StartTime.UnixMicro()
}

type DatabaseRoot struct {
	DBName                        string   `json:"db_name"`
	SequenceNumber                *uint64  `json:"update_seq,omitempty"`           // The last sequence written to the _default collection, if not running with multiple collections.
	CommittedUpdateSequenceNumber *uint64  `json:"committed_update_seq,omitempty"` // Same as above - Used by perf tests, shouldn't be removed
	InstanceStartTimeMicro        int64    `json:"instance_start_time"`            // microseconds since epoch
	CompactRunning                bool     `json:"compact_running"`
	PurgeSequenceNumber           uint64   `json:"purge_seq"`
	DiskFormatVersion             uint64   `json:"disk_format_version"`
	State                         string   `json:"state"`
	ServerUUID                    string   `json:"server_uuid,omitempty"`
	RequireResync                 []string `json:"require_resync,omitempty"`
	InitializationActive          bool     `json:"init_in_progress,omitempty"`
}

type DbSummary struct {
	DBName               string            `json:"db_name"`
	Bucket               string            `json:"bucket"`
	State                string            `json:"state"`
	InitializationActive bool              `json:"init_in_progress,omitempty"`
	RequireResync        bool              `json:"require_resync,omitempty"`
	DatabaseError        *db.DatabaseError `json:"database_error,omitempty"`
}

func (h *handler) handleGetDB() error {
	if h.rq.Method == "HEAD" {
		return nil
	}

	// Don't bother trying to lookup LastSequence() if offline
	var lastSeq uint64
	runState := db.RunStateString[atomic.LoadUint32(&h.db.State)]
	if runState != db.RunStateString[db.DBOffline] {
		lastSeq, _ = h.db.LastSequence(h.ctx())
	}

	var response = DatabaseRoot{
		DBName:                        h.db.Name,
		SequenceNumber:                &lastSeq,
		CommittedUpdateSequenceNumber: &lastSeq,
		InstanceStartTimeMicro:        h.instanceStartTimeMicro(),
		CompactRunning:                h.db.IsCompactRunning(),
		PurgeSequenceNumber:           0, // TODO: Should track this value
		DiskFormatVersion:             0, // Probably meaningless, but add for compatibility
		State:                         runState,
		ServerUUID:                    h.db.DatabaseContext.ServerUUID,
		RequireResync:                 h.db.RequireResync.ScopeAndCollectionNames(),
		InitializationActive:          h.server.DatabaseInitManager.HasActiveInitialization(h.db.Name),
	}

	base.Audit(h.ctx(), base.AuditIDReadDatabase, nil)
	h.writeJSON(response)
	return nil
}

// Stub handler for hadling create DB on the public API returns HTTP status 412
// if the db exists, and 403 if it doesn't.
// fixes issue #562
func (h *handler) handleCreateTarget() error {
	dbname := h.PathVar("targetdb")
	if _, err := h.server.GetDatabase(h.ctx(), dbname); err != nil {
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
func (h *handler) handleProfiling() (err error) {
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
			filename := h.server.CloseCpuPprofFile(h.ctx())
			base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "cpu", base.AuditFieldFileName: filename})
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
		base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "cpu (start)", base.AuditFieldFileName: params.File})
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
		base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: profileName, base.AuditFieldFileName: params.File})
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
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "heap", base.AuditFieldFileName: params.File})
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
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "goroutine"})
	return nil
}

// Go execution tracer
func (h *handler) handlePprofTrace() error {
	httpprof.Trace(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "trace"})
	return nil
}

func (h *handler) handlePprofCmdline() error {
	httpprof.Cmdline(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "cmdline"})
	return nil
}

func (h *handler) handlePprofSymbol() error {
	httpprof.Symbol(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "symbol"})
	return nil
}

func (h *handler) handlePprofHeap() error {
	httpprof.Handler("heap").ServeHTTP(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "heap"})
	return nil
}

func (h *handler) handlePprofProfile() error {
	httpprof.Profile(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "profile"})
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

	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "fgprof"})
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
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "block"})
	return nil
}

func (h *handler) handlePprofThreadcreate() error {
	httpprof.Handler("threadcreate").ServeHTTP(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "threadcreate"})
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
	base.Audit(h.ctx(), base.AuditIDSyncGatewayProfiling, base.AuditFields{base.AuditFieldPprofProfileType: "mutex"})
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

	base.Audit(h.ctx(), base.AuditIDSyncGatewayStats, base.AuditFields{base.AuditFieldStatsFormat: "memstats"})
	return nil
}

func (h *handler) handleMetrics() error {
	promhttp.Handler().ServeHTTP(h.response, h.rq)
	base.Audit(h.ctx(), base.AuditIDSyncGatewayStats, base.AuditFields{base.AuditFieldStatsFormat: "prometheus"})
	return nil
}
