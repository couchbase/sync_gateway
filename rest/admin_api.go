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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

const kDefaultDBOnlineDelay = 0

//////// DATABASE MAINTENANCE:

// "Create" a database (actually just register an existing bucket)
func (h *handler) handleCreateDB() error {
	h.assertAdminOnly()
	dbName := h.PathVar("newdb")
	var config *DbConfig
	if err := h.readJSONInto(&config); err != nil {
		return err
	}
	if err := config.setup(dbName); err != nil {
		return err
	}
	if _, err := h.server.AddDatabaseFromConfig(config); err != nil {
		return err
	}
	return base.HTTPErrorf(http.StatusCreated, "created")
}

// Take a DB online, first reload the DB config
func (h *handler) handleDbOnline() error {
	h.assertAdminOnly()
	dbState := atomic.LoadUint32(&h.db.State)
	//If the DB is already trasitioning to: online or is online silently return
	if dbState == db.DBOnline || dbState == db.DBStarting {
		return nil
	}

	//If the DB is currently re-syncing return an error asking the user to retry later
	if dbState == db.DBResyncing {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Database _resync is in progress, this may take some time, try again later")
	}

	body, err := h.readBody()
	if err != nil {
		return err
	}

	var input struct {
		Delay int `json:"delay"`
	}

	input.Delay = kDefaultDBOnlineDelay

	_ = base.JSONUnmarshal(body, &input)

	base.Infof(base.KeyCRUD, "Taking Database : %v, online in %v seconds", base.MD(h.db.Name), input.Delay)

	timer := time.NewTimer(time.Duration(input.Delay) * time.Second)
	go func() {
		<-timer.C

		h.server.TakeDbOnline(h.db.DatabaseContext)
	}()

	return nil
}

//Take a DB offline
func (h *handler) handleDbOffline() error {
	h.assertAdminOnly()
	var err error
	if err = h.db.TakeDbOffline("ADMIN Request"); err != nil {
		base.Infof(base.KeyCRUD, "Unable to take Database : %v, offline", base.MD(h.db.Name))
	}

	return err
}

// Get admin database info
func (h *handler) handleGetDbConfig() error {
	redact, _ := h.getOptBoolQuery("redact", true)
	if redact {
		cfg, err := h.server.GetDatabaseConfig(h.db.Name).Redacted()
		if err != nil {
			return err
		}
		h.writeJSON(cfg)
	} else {
		h.writeJSON(h.server.GetDatabaseConfig(h.db.Name))
	}
	return nil
}

// Get admin config info
func (h *handler) handleGetConfig() error {
	redact, _ := h.getOptBoolQuery("redact", true)
	if redact {
		cfg, err := h.server.GetConfig().Redacted()
		if err != nil {
			return err
		}
		h.writeJSON(cfg)
	} else {
		h.writeJSON(h.server.GetConfig())
	}
	return nil
}

// PUT a new database config
func (h *handler) handlePutDbConfig() error {
	h.assertAdminOnly()
	dbName := h.db.Name
	var config *DbConfig
	if err := h.readJSONInto(&config); err != nil {
		return err
	}
	if err := config.setup(dbName); err != nil {
		return err
	}
	h.server.lock.Lock()
	defer h.server.lock.Unlock()
	h.server.config.Databases[dbName] = config

	return base.HTTPErrorf(http.StatusCreated, "created")
}

// "Delete" a database (it doesn't actually do anything to the underlying bucket)
func (h *handler) handleDeleteDB() error {
	h.assertAdminOnly()
	if !h.server.RemoveDatabase(h.db.Name) {
		return base.HTTPErrorf(http.StatusNotFound, "missing")
	}
	_, _ = h.response.Write([]byte("{}"))
	return nil
}

/////// Replication and Task monitoring

func (h *handler) handleReplicate() error {

	h.assertAdminOnly()
	body, err := h.readBody()
	if err != nil {
		return err
	}

	base.Warnf("The /_replicate API is deprecated - use /{db}/_replication instead.")

	params, cancel, _, err := h.readReplicateV1ParametersFromJSON(body)
	if err != nil {
		return err
	}

	if !cancel {
		response, err := h.server.HTTPClient.Get(params.GetTargetDbUrl())
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode >= 400 {
			b, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			var body db.Body
			err = base.JSONUnmarshal(b, &body)
			if err != nil {
				return err
			}
			return base.HTTPErrorf(response.StatusCode, "Unable to start replication to target db: %s", body["reason"])
		}

		response, err = h.server.HTTPClient.Get(params.GetSourceDbUrl())
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode >= 400 {
			b, err := ioutil.ReadAll(response.Body)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			var body db.Body
			err = base.JSONUnmarshal(b, &body)
			if err != nil {
				return err
			}
			return base.HTTPErrorf(response.StatusCode, "Unable to start replication from source db: %s", body["reason"])
		}
	}

	replication, err := h.server.replicator.Replicate(params, cancel)

	if err == nil {
		h.writeJSON(replication)
	}

	return err

}

type ReplicateV1Config struct {
	Source           string      `json:"source"`
	Target           string      `json:"target"`
	Continuous       bool        `json:"continuous"`
	CreateTarget     bool        `json:"create_target"`
	DocIds           []string    `json:"doc_ids"`
	Filter           string      `json:"filter"`
	Proxy            string      `json:"proxy"`
	QueryParams      interface{} `json:"query_params"`
	Cancel           bool        `json:"cancel"`
	Async            bool        `json:"async"`
	ChangesFeedLimit *int        `json:"changes_feed_limit"`
	ReplicationId    string      `json:"replication_id"`
	upgradedToSGR2   bool        // upgradedToSGR2 is set to true when an equivalent SGR2 replication is found, which prevents this v1 replication from starting.
}

func (h *handler) readReplicateV1ParametersFromJSON(jsonData []byte) (params sgreplicate.ReplicationParameters, cancel bool, localdb bool, err error) {

	var in ReplicateV1Config
	if err = base.JSONUnmarshal(jsonData, &in); err != nil {
		return params, false, localdb, err
	}

	return validateReplicateV1Parameters(in, false, *h.server.config.AdminInterface)
}

func validateReplicateV1Parameters(requestParams ReplicateV1Config, paramsFromConfig bool, adminInterface string) (params sgreplicate.ReplicationParameters, cancel bool, localdb bool, err error) {
	if requestParams.CreateTarget {
		err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate create_target option is not currently supported.")
		return
	}

	if len(requestParams.DocIds) > 0 {
		err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate doc_ids option is not currently supported.")
		return
	}

	if requestParams.Proxy != "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate proxy option is not currently supported.")
		return
	}

	params.ReplicationId = requestParams.ReplicationId

	//cancel parameter is only supported via the REST API
	if requestParams.Cancel {
		if paramsFromConfig {
			err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate cancel is invalid in Sync Gateway configuration")
			return
		} else {
			cancel = true
			if params.ReplicationId != "" {
				return params, cancel, localdb, nil
			}
		}
	}

	sourceUrl, err := url.Parse(requestParams.Source)
	if err != nil || requestParams.Source == "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate source URL [%s] is invalid.", requestParams.Source)
		return
	}
	syncSource := base.SyncSourceFromURL(sourceUrl)
	if syncSource != "" {
		params.Source, _ = url.Parse(syncSource)
	}
	// Strip leading and trailing / from path to get db name
	params.SourceDb = strings.Trim(sourceUrl.Path, "/")

	targetUrl, err := url.Parse(requestParams.Target)
	if err != nil || requestParams.Target == "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate target URL [%s] is invalid.", requestParams.Target)
		return
	}
	syncTarget := base.SyncSourceFromURL(targetUrl)
	if syncTarget != "" {
		params.Target, _ = url.Parse(syncTarget)
	}
	params.TargetDb = strings.Trim(targetUrl.Path, "/")

	if requestParams.Continuous {
		params.Lifecycle = sgreplicate.CONTINUOUS
	}

	params.Async = requestParams.Async
	if requestParams.ChangesFeedLimit != nil {
		params.ChangesFeedLimit = *requestParams.ChangesFeedLimit
	} else {
		params.ChangesFeedLimit = sgreplicate.DefaultChangesFeedLimit
	}

	if requestParams.Filter != "" {
		if requestParams.Filter == base.ByChannelFilter {
			if requestParams.QueryParams == "" {
				err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate sync_gateway/bychannel filter; Missing query_params")
				return
			}

			params.Channels, err = db.ChannelsFromQueryParams(requestParams.QueryParams)
			if err != nil {
				return params, cancel, localdb, base.HTTPErrorf(http.StatusBadRequest, "/_replicate "+err.Error())
			}
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "/_replicate Unknown filter; try sync_gateway/bychannel")
			return
		}
	}

	//If source and/or target are local DB names add local AdminInterface URL
	localDbUrl := "http://" + adminInterface
	if params.Source == nil {
		localdb = true
		params.Source, _ = url.Parse(localDbUrl)
	}

	if params.Target == nil {
		localdb = true
		params.Target, _ = url.Parse(localDbUrl)
	}

	return params, requestParams.Cancel, localdb, nil
}

func (h *handler) handleActiveTasks() error {

	base.Warnf("The /_active_tasks API is deprecated - use /{db}/_replicationStatus instead.")
	h.writeJSON(h.server.replicator.ActiveTasks())
	return nil
}

// raw document access for admin api

func (h *handler) handleGetRawDoc() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")

	includeDoc, includeDocSet := h.getOptBoolQuery("include_doc", true)
	redact, _ := h.getOptBoolQuery("redact", false)
	salt := h.getQuery("salt")

	if redact && includeDoc && includeDocSet {
		return base.HTTPErrorf(http.StatusBadRequest, "redact and include_doc cannot be true at the same time. "+
			"If you want to redact you must specify include_doc=false")
	}

	if redact && !includeDocSet {
		includeDoc = false
	}

	doc, err := h.db.GetDocument(docid, db.DocUnmarshalSync)
	if err != nil {
		return err
	}

	rawBytes := []byte(base.EmptyDocument)
	if includeDoc {
		if doc.IsDeleted() {
			rawBytes = []byte(db.DeletedDocument)
		} else {
			docRawBodyBytes, err := doc.BodyBytes()
			if err != nil {
				return err
			}
			rawBytes = docRawBodyBytes
		}
	}

	syncData := doc.SyncData
	if redact {
		if salt == "" {
			salt = uuid.New().String()
		}
		syncData = doc.SyncData.HashRedact(salt)
	}

	rawBytes, err = base.InjectJSONProperties(rawBytes, base.KVPair{Key: base.SyncPropertyName, Val: syncData})
	if err != nil {

		return err
	}

	h.writeRawJSON(rawBytes)
	return nil
}

func (h *handler) handleGetRevTree() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")
	doc, err := h.db.GetDocument(docid, db.DocUnmarshalAll)

	if doc != nil {
		h.writeText([]byte(doc.History.RenderGraphvizDot()))
	}
	return err
}

func (h *handler) handleGetLogging() error {
	h.writeJSON(base.GetLogKeys())
	return nil
}

type DatabaseStatus struct {
	SequenceNumber    uint64                  `json:"seq"`
	ServerUUID        string                  `json:"server_uuid"`
	State             string                  `json:"state"`
	ReplicationStatus []*db.ReplicationStatus `json:"replication_status"`
	SGRCluster        *db.SGRCluster          `json:"cluster"`
}

type Vendor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Status struct {
	Databases   map[string]DatabaseStatus `json:"databases"`
	ActiveTasks []base.Task               `json:"active_tasks"`
	Version     string                    `json:"version"`
	Vendor      Vendor                    `json:"vendor"`
}

func (h *handler) handleGetStatus() error {

	var status = Status{
		Databases:   make(map[string]DatabaseStatus),
		ActiveTasks: h.server.replicator.ActiveTasks(),
		Version:     base.LongVersionString,
		Vendor: Vendor{
			Name:    base.ProductName,
			Version: base.VersionNumber,
		},
	}

	for _, database := range h.server.databases_ {
		lastSeq := uint64(0)
		runState := db.RunStateString[atomic.LoadUint32(&database.State)]

		// Don't bother trying to lookup LastSequence() if offline
		if runState != db.RunStateString[db.DBOffline] {
			lastSeq, _ = database.LastSequence()
		}

		replicationsStatus, err := database.SGReplicateMgr.GetReplicationStatusAll(db.DefaultReplicationStatusOptions())
		if err != nil {
			return err
		}
		cluster, err := database.SGReplicateMgr.GetSGRCluster()
		if err != nil {
			return err
		}
		for _, replication := range cluster.Replications {
			replication.ReplicationConfig = *replication.Redacted()
		}

		status.Databases[database.Name] = DatabaseStatus{
			SequenceNumber:    lastSeq,
			State:             runState,
			ServerUUID:        database.GetServerUUID(),
			ReplicationStatus: replicationsStatus,
			SGRCluster:        cluster,
		}
	}

	h.writeJSON(status)
	return nil
}

func (h *handler) handleSetLogging() error {
	body, err := h.readBody()
	if err != nil {
		return nil
	}

	var newLogLevel base.LogLevel
	var setLogLevel bool
	if level := h.getQuery("logLevel"); level != "" {
		if err := newLogLevel.UnmarshalText([]byte(level)); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}
		setLogLevel = true
	} else if level := h.getIntQuery("level", 0); level != 0 {
		base.Warnf("Using deprecated query parameter: %q. Use %q instead.", "level", "logLevel")
		switch base.GetRestrictedInt(&level, 0, 1, 3, false) {
		case 1:
			newLogLevel = base.LevelInfo
		case 2:
			newLogLevel = base.LevelWarn
		case 3:
			newLogLevel = base.LevelError
		}
		setLogLevel = true
	}

	if setLogLevel {
		base.Infof(base.KeyAll, "Setting log level to: %v", newLogLevel)
		base.ConsoleLogLevel().Set(newLogLevel)

		// empty body is OK if request is just setting the log level
		if len(body) == 0 {
			return nil
		}
	}

	var keys map[string]bool
	if err := base.JSONUnmarshal(body, &keys); err != nil {

		// return a better error if a user is setting log level inside the body
		var logLevel map[string]string
		if err := base.JSONUnmarshal(body, &logLevel); err == nil {
			if _, ok := logLevel["logLevel"]; ok {
				return base.HTTPErrorf(http.StatusBadRequest, "Can't set log level in body, please use \"logLevel\" query parameter instead.")
			}
		}

		return base.HTTPErrorf(http.StatusBadRequest, "Invalid JSON or non-boolean values for log key map")
	}

	base.UpdateLogKeys(keys, h.rq.Method == "PUT")
	return nil
}

func (h *handler) handleSGCollectStatus() error {
	status := "stopped"
	if sgcollectInstance.IsRunning() {
		status = "running"
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"`+status+`"}`))
	return nil
}

func (h *handler) handleSGCollectCancel() error {
	err := sgcollectInstance.Stop()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error stopping sgcollect_info: %v", err)
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"cancelled"}`))
	return nil
}

func (h *handler) handleSGCollect() error {
	body, err := h.readBody()
	if err != nil {
		return err
	}

	var params sgCollectOptions
	if err = base.JSONUnmarshal(body, &params); err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to parse request body: %v", err)
	}

	if errs := params.Validate(); errs.ErrorOrNil() != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid options used for sgcollect_info: %v", errs)
	}

	zipFilename := sgcollectFilename()

	if err := sgcollectInstance.Start(h.serialNumber, zipFilename, params); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Error running sgcollect_info: %v", err)
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"started"}`))

	return nil
}

//////// USERS & ROLES:

func internalUserName(name string) string {
	if name == base.GuestUsername {
		return ""
	}
	return name
}

func externalUserName(name string) string {
	if name == "" {
		return base.GuestUsername
	}
	return name
}

func marshalPrincipal(princ auth.Principal) ([]byte, error) {
	name := externalUserName(princ.Name())
	info := db.PrincipalConfig{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
	}
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedChannels().AsSet()
		info.Email = user.Email()
		info.Disabled = user.Disabled()
		info.ExplicitRoleNames = user.ExplicitRoles().AllChannels()
		info.RoleNames = user.RoleNames().AllChannels()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return base.JSONMarshal(info)
}

// Handles PUT and POST for a user or a role.
func (h *handler) updatePrincipal(name string, isUser bool) error {
	h.assertAdminOnly()
	// Unmarshal the request body into a PrincipalConfig struct:
	body, _ := h.readBody()
	var newInfo db.PrincipalConfig
	var err error
	if err = base.JSONUnmarshal(body, &newInfo); err != nil {
		return err
	}

	if h.rq.Method == "POST" {
		// On POST, take the name from the "name" property in the request body:
		if newInfo.Name == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing name property")
		}
	} else {
		// ON PUT, verify the name matches, if given:
		if newInfo.Name == nil {
			newInfo.Name = &name
		} else if *newInfo.Name != name {
			return base.HTTPErrorf(http.StatusBadRequest, "Name mismatch (can't change name)")
		}
	}

	internalName := internalUserName(*newInfo.Name)
	newInfo.Name = &internalName
	replaced, err := h.db.UpdatePrincipal(newInfo, isUser, h.rq.Method != "POST")
	if err != nil {
		return err
	} else if replaced {
		// on update with a new password, remove previous user sessions
		if newInfo.Password != nil {
			err = h.db.DeleteUserSessions(*newInfo.Name)
			if err != nil {
				return err
			}
		}
		h.writeStatus(http.StatusOK, "OK")
	} else {
		h.writeStatus(http.StatusCreated, "Created")
	}
	return nil
}

// Handles PUT or POST to /_user/*
func (h *handler) putUser() error {
	username := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(username, true)
}

// Handles PUT or POST to /_role/*
func (h *handler) putRole() error {
	rolename := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(rolename, false)
}

func (h *handler) deleteUser() error {
	h.assertAdminOnly()
	username := mux.Vars(h.rq)["name"]

	// Can't delete the guest user, only disable.
	if username == base.GuestUsername {
		return base.HTTPErrorf(http.StatusMethodNotAllowed,
			"The %s user cannot be deleted. Only disabled via an update.", base.GuestUsername)
	}

	user, err := h.db.Authenticator().GetUser(username)
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator().Delete(user)
}

func (h *handler) deleteRole() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator().GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator().Delete(role)
}

func (h *handler) getUserInfo() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator().GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}

	bytes, err := marshalPrincipal(user)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) getRoleInfo() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator().GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	bytes, err := marshalPrincipal(role)
	_, _ = h.response.Write(bytes)
	return err
}

func (h *handler) getUsers() error {
	users, _, err := h.db.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := base.JSONMarshal(users)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) getRoles() error {
	_, roles, err := h.db.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := base.JSONMarshal(roles)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) handlePurge() error {
	h.assertAdminOnly()

	message := "OK"

	//Get the list of docs to purge

	input, err := h.readJSON()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "_purge document ID's must be passed as a JSON")
	}

	startTime := time.Now()
	docIDs := make([]string, 0)

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	_, _ = h.response.Write([]byte("{\"purged\":{\r\n"))
	var first bool = true

	for key, value := range input {
		//For each one validate that the revision list is set to ["*"], otherwise skip doc and log warning
		base.Infof(base.KeyCRUD, "purging document = %v", base.UD(key))

		if revisionList, ok := value.([]interface{}); ok {

			//There should only be a single revision entry of "*"
			if len(revisionList) != 1 {
				base.Infof(base.KeyCRUD, "Revision list for doc ID %v, should contain exactly one entry", base.UD(key))
				continue //skip this entry its not valid
			}

			if revisionList[0] != "*" {
				base.Infof(base.KeyCRUD, "Revision entry for doc ID %v, should be the '*' revison", base.UD(key))
				continue //skip this entry its not valid
			}

			//Attempt to delete document, if successful add to response, otherwise log warning
			err = h.db.Purge(key)
			if err == nil {

				docIDs = append(docIDs, key)

				if first {
					first = false
				} else {
					_, _ = h.response.Write([]byte(","))
				}

				s := fmt.Sprintf("\"%v\" : [\"*\"]\n", key)
				_, _ = h.response.Write([]byte(s))

			} else {
				base.Infof(base.KeyCRUD, "Failed to purge document %v, err = %v", base.UD(key), err)
				continue //skip this entry its not valid
			}

		} else {
			base.Infof(base.KeyCRUD, "Revision list for doc ID %v, is not an array, ", base.UD(key))
			continue //skip this entry its not valid
		}
	}

	if len(docIDs) > 0 {
		count := h.db.GetChangeCache().Remove(docIDs, startTime)
		base.Debugf(base.KeyCache, "Purged %d items from caches", count)
	}

	_, _ = h.response.Write([]byte("}\n}\n"))
	h.logStatusWithDuration(http.StatusOK, message)

	return nil
}

// sg-replicate endpoints
func (h *handler) getReplications() error {
	replications, err := h.db.SGReplicateMgr.GetReplications()
	if err != nil {
		return err
	}

	for _, replication := range replications {
		if replication.AssignedNode == h.db.UUID {
			replication.AssignedNode = replication.AssignedNode + " (local)"
		} else {
			replication.AssignedNode = replication.AssignedNode + " (non-local)"
		}
		replication.ReplicationConfig = *replication.Redacted()
	}

	h.writeJSON(replications)
	return nil
}

func (h *handler) getReplication() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	replication, err := h.db.SGReplicateMgr.GetReplication(replicationID)
	if replication == nil {
		if err == nil {
			return kNotFoundError
		}
		return err
	}

	h.writeJSON(replication.Redacted())
	return nil
}

func (h *handler) putReplication() error {

	body, readErr := h.readBody()
	if readErr != nil {
		return readErr
	}
	body = base.ConvertBackQuotedStrings(body)

	replicationConfig := &db.ReplicationUpsertConfig{}
	if err := base.JSONUnmarshal(body, replicationConfig); err != nil {
		return err
	}

	if h.rq.Method == "PUT" {
		replicationID := mux.Vars(h.rq)["replicationID"]
		if replicationConfig.ID != "" && replicationConfig.ID != replicationID {
			return base.HTTPErrorf(http.StatusBadRequest, "Replication ID in body %q does not match request URI", replicationConfig.ID)
		}
		replicationConfig.ID = replicationID
	}

	created, err := h.db.SGReplicateMgr.UpsertReplication(replicationConfig)
	if err != nil {
		return err
	}
	if created {
		h.writeStatus(http.StatusCreated, "Created")
	}

	return nil
}

func (h *handler) deleteReplication() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	return h.db.SGReplicateMgr.DeleteReplication(replicationID)
}

func (h *handler) getReplicationsStatus() error {
	replicationsStatus, err := h.db.SGReplicateMgr.GetReplicationStatusAll(h.getReplicationStatusOptions())
	if err != nil {
		return err
	}
	h.writeJSON(replicationsStatus)
	return nil
}

func (h *handler) getReplicationStatus() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	status, err := h.db.SGReplicateMgr.GetReplicationStatus(replicationID, h.getReplicationStatusOptions())
	if err != nil {
		return err
	}
	h.writeJSON(status)
	return nil
}

func (h *handler) getReplicationStatusOptions() db.ReplicationStatusOptions {
	activeOnly, _ := h.getOptBoolQuery("activeOnly", false)
	localOnly, _ := h.getOptBoolQuery("localOnly", false)
	includeError, _ := h.getOptBoolQuery("includeError", true)
	includeConfig, _ := h.getOptBoolQuery("includeConfig", false)
	return db.ReplicationStatusOptions{
		ActiveOnly:    activeOnly,
		LocalOnly:     localOnly,
		IncludeError:  includeError,
		IncludeConfig: includeConfig,
	}
}

func (h *handler) putReplicationStatus() error {
	replicationID := mux.Vars(h.rq)["replicationID"]

	action := h.getQuery("action")
	if action == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Query parameter 'action' must be specified")
	}

	updatedStatus, err := h.db.SGReplicateMgr.PutReplicationStatus(replicationID, action)
	if err != nil {
		return err
	}
	h.writeJSON(updatedStatus)
	return nil
}
