package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// kHandlersByProfile defines the routes for each message profile (verb) of an incoming request to the function that handles it.
var kHandlersByProfile = map[string]blipHandlerFunc{
	MessageGetCheckpoint:   (*blipHandler).handleGetCheckpoint,
	MessageSetCheckpoint:   (*blipHandler).handleSetCheckpoint,
	MessageSubChanges:      userBlipHandler((*blipHandler).handleSubChanges),
	MessageChanges:         userBlipHandler((*blipHandler).handleChanges),
	MessageRev:             userBlipHandler((*blipHandler).handleRev),
	MessageNoRev:           (*blipHandler).handleNoRev,
	MessageGetAttachment:   userBlipHandler((*blipHandler).handleGetAttachment),
	MessageProveAttachment: userBlipHandler((*blipHandler).handleProveAttachment),
	MessageProposeChanges:  (*blipHandler).handleProposeChanges,
}

// maxInFlightChangesBatches is the maximum number of in-flight changes batches a client is allowed to send without being throttled.
const maxInFlightChangesBatches = 2

type blipHandler struct {
	*BlipSyncContext
	db           *Database // Handler-specific copy of the BlipSyncContext's blipContextDb
	serialNumber uint64    // This blip handler's serial number to differentiate logs w/ other handlers
}

type BLIPSyncContextClientType string

const (
	BLIPSyncClientTypeQueryParam = "client"

	BLIPClientTypeCBL2 BLIPSyncContextClientType = "cbl2"
	BLIPClientTypeSGR2 BLIPSyncContextClientType = "sgr2"
)

type blipHandlerFunc func(*blipHandler, *blip.Message) error

// userBlipHandler wraps another blip handler with code that reloads the user object when the user
// or the user's roles have changed, to make sure that the replication has the latest channel access grants.
// Uses a userChangeWaiter to detect changes to the user or roles.  Note that in the case of a pushed document
// triggering a user access change, this happens at write time (via MarkPrincipalsChanged), and doesn't
// depend on the userChangeWaiter.
func userBlipHandler(next blipHandlerFunc) blipHandlerFunc {
	return func(bh *blipHandler, bm *blip.Message) error {

		// Reload user if it has changed
		if err := bh.refreshUser(); err != nil {
			return err
		}
		// Call down to the underlying handler and return it's value
		return next(bh, bm)
	}
}

func (bh *blipHandler) refreshUser() error {

	bc := bh.BlipSyncContext
	if bc.userName != "" {
		// Check whether user needs to be refreshed
		bc.dbUserLock.Lock()
		userChanged := bc.userChangeWaiter.RefreshUserCount()

		// If changed, refresh the user and db while holding the lock
		if userChanged {
			// Refresh the BlipSyncContext database
			newUser, err := bc.blipContextDb.Authenticator().GetUser(bc.userName)
			if err != nil {
				bc.dbUserLock.Unlock()
				return err
			}
			bc.userChangeWaiter.RefreshUserKeys(newUser)
			bc.blipContextDb.SetUser(newUser)

			// refresh the handler's database with the new BlipSyncContext database
			bh.db = bh._copyContextDatabase()
		}
		bc.dbUserLock.Unlock()
	}
	return nil
}

//////// CHECKPOINTS

// Received a "getCheckpoint" request
func (bh *blipHandler) handleGetCheckpoint(rq *blip.Message) error {

	client := rq.Properties[BlipClient]
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("Client:%s", client))

	response := rq.Response()
	if response == nil {
		return nil
	}

	value, err := bh.db.GetSpecial(DocTypeLocal, checkpointDocIDPrefix+client)
	if err != nil {
		return err
	}
	if value == nil {
		return base.HTTPErrorf(http.StatusNotFound, http.StatusText(http.StatusNotFound))
	}
	response.Properties[GetCheckpointResponseRev] = value[BodyRev].(string)
	delete(value, BodyRev)
	delete(value, BodyId)
	// TODO: Marshaling here when we could use raw bytes all the way from the bucket
	_ = response.SetJSONBody(value)
	return nil
}

// Received a "setCheckpoint" request
func (bh *blipHandler) handleSetCheckpoint(rq *blip.Message) error {

	checkpointMessage := SetCheckpointMessage{rq}
	bh.logEndpointEntry(rq.Profile(), checkpointMessage.String())

	var checkpoint Body
	if err := checkpointMessage.ReadJSONBody(&checkpoint); err != nil {
		return err
	}
	if revID := checkpointMessage.rev(); revID != "" {
		checkpoint[BodyRev] = revID
	}
	revID, err := bh.db.PutSpecial(DocTypeLocal, checkpointDocIDPrefix+checkpointMessage.client(), checkpoint)
	if err != nil {
		return err
	}

	checkpointResponse := SetCheckpointResponse{checkpointMessage.Response()}
	checkpointResponse.setRev(revID)

	return nil
}

//////// CHANGES

// Received a "subChanges" subscription request
func (bh *blipHandler) handleSubChanges(rq *blip.Message) error {

	bh.lock.Lock()
	defer bh.lock.Unlock()

	bh.gotSubChanges = true

	logCtx := bh.loggingCtx
	defaultSince := CreateZeroSinceValue()
	subChangesParams, err := NewSubChangesParams(logCtx, rq, defaultSince, ParseJSONSequenceID)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid subChanges parameters")
	}

	// Ensure that only _one_ subChanges subscription can be open on this blip connection at any given time.  SG #3222.
	if !bh.activeSubChanges.CASRetry(false, true) {
		return fmt.Errorf("blipHandler already has an outstanding continous subChanges.  Cannot open another one.")
	}

	if len(subChangesParams.docIDs()) > 0 && subChangesParams.continuous() {
		return base.HTTPErrorf(http.StatusBadRequest, "DocIDs filter not supported for continuous subChanges")
	}

	bh.logEndpointEntry(rq.Profile(), subChangesParams.String())

	var channels base.Set
	if filter := subChangesParams.filter(); filter == base.ByChannelFilter {
		var err error

		channels, err = subChangesParams.channelsExpandedSet()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "%s", err)
		} else if len(channels) == 0 {
			return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")

		}
	} else if filter != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel")
	}

	clientType := clientTypeCBL2
	if rq.Properties["client_sgr2"] == "true" {
		clientType = clientTypeSGR2
	}

	continuous := subChangesParams.continuous()
	// used for stats tracking
	bh.continuous = continuous
	// Start asynchronous changes goroutine
	go func() {
		// Pull replication stats by type - Active stats decremented in Close()
		if bh.continuous {
			bh.replicationStats.SubChangesContinuousActive.Add(1)
			bh.replicationStats.SubChangesContinuousTotal.Add(1)
		} else {
			bh.replicationStats.SubChangesOneShotActive.Add(1)
			bh.replicationStats.SubChangesOneShotTotal.Add(1)
		}

		defer func() {
			bh.activeSubChanges.Set(false)
		}()
		// sendChanges runs until blip context closes, or fails due to error
		startTime := time.Now()
		_ = bh.sendChanges(rq.Sender, &sendChangesOptions{
			docIDs:            subChangesParams.docIDs(),
			since:             subChangesParams.Since(),
			continuous:        continuous,
			activeOnly:        subChangesParams.activeOnly(),
			batchSize:         subChangesParams.batchSize(),
			channels:          channels,
			clientType:        clientType,
			ignoreNoConflicts: clientType == clientTypeSGR2, // force this side to accept a "changes" message, even in no conflicts mode for SGR2.
		})
		base.DebugfCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> Time:%v", bh.serialNumber, rq.Profile(), time.Since(startTime))
	}()

	return nil
}

type clientType uint8

const (
	clientTypeCBL2 clientType = iota
	clientTypeSGR2
)

type sendChangesOptions struct {
	docIDs                 []string
	since                  SequenceID
	continuous             bool
	activeOnly             bool
	batchSize              int
	channels               base.Set
	clientType             clientType
	ignoreNoConflicts      bool
	disableRemovalMessages bool // stops returning a document removal message when the channels are not any of the filtered channels
}

// Sends all changes since the given sequence
func (bh *blipHandler) sendChanges(sender *blip.Sender, opts *sendChangesOptions) (isComplete bool) {
	defer func() {
		if panicked := recover(); panicked != nil {
			base.Warnf("[%s] PANIC sending changes: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	base.InfofCtx(bh.loggingCtx, base.KeySync, "Sending changes since %v", opts.since)

	options := ChangesOptions{
		Since:      opts.since,
		Conflicts:  false, // CBL 2.0/BLIP don't support branched rev trees (LiteCore #437)
		Continuous: opts.continuous,
		ActiveOnly: opts.activeOnly,
		Terminator: bh.BlipSyncContext.terminator,
		Ctx:        bh.loggingCtx,
		clientType: opts.clientType,
	}

	channelSet := opts.channels
	if channelSet == nil {
		channelSet = base.SetOf(channels.AllChannelWildcard)
	}

	caughtUp := false
	pendingChanges := make([][]interface{}, 0, opts.batchSize)
	sendPendingChangesAt := func(minChanges int) error {
		if len(pendingChanges) >= minChanges {
			if err := bh.sendBatchOfChanges(sender, pendingChanges, opts.ignoreNoConflicts); err != nil {
				return err
			}
			pendingChanges = make([][]interface{}, 0, opts.batchSize)
		}
		return nil
	}

	// Create a distinct database instance for changes, to avoid races between reloadUser invocation in changes.go
	// and BlipSyncContext user access.
	changesDb := bh.copyContextDatabase()
	_, forceClose := generateBlipSyncChanges(changesDb, channelSet, options, opts.docIDs, func(changes []*ChangeEntry) error {
		base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Sending %d changes", len(changes))
		for _, change := range changes {

			if !strings.HasPrefix(change.ID, "_") {
				for _, item := range change.Changes {
					changeRow := []interface{}{change.Seq, change.ID, item["rev"], change.Deleted}
					if opts.disableRemovalMessages && change.allRemoved {
						continue
					}
					if !change.Deleted {
						changeRow = changeRow[0:3]
					}
					pendingChanges = append(pendingChanges, changeRow)
					if err := sendPendingChangesAt(opts.batchSize); err != nil {
						return err
					}
				}
			}
		}
		if caughtUp || len(changes) == 0 {
			if err := sendPendingChangesAt(1); err != nil {
				return err
			}
			if !caughtUp {
				caughtUp = true
				// Signal to client that it's caught up
				if err := bh.sendBatchOfChanges(sender, nil, opts.ignoreNoConflicts); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// On forceClose, send notify to trigger immediate exit from change waiter
	if forceClose {
		user := ""
		if bh.db.User() != nil {
			user = bh.db.User().Name()
		}
		bh.db.DatabaseContext.NotifyTerminatedChanges(user)
	}

	return !forceClose
}

func (bh *blipHandler) sendBatchOfChanges(sender *blip.Sender, changeArray [][]interface{}, ignoreNoConflicts bool) error {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	if ignoreNoConflicts {
		outrq.Properties[ChangesMessageIgnoreNoConflicts] = "true"
	}
	err := outrq.SetJSONBody(changeArray)
	if err != nil {
		base.InfofCtx(bh.loggingCtx, base.KeyAll, "Error setting changes: %v", err)
	}

	if len(changeArray) > 0 {
		// Check for user updates before creating the db copy for handleChangesResponse
		if err := bh.refreshUser(); err != nil {
			return err
		}
		handleChangesResponseDb := bh.copyContextDatabase()

		sendTime := time.Now()
		if !bh.sendBLIPMessage(sender, outrq) {
			return ErrClosedBLIPSender
		}

		bh.inFlightChangesThrottle <- struct{}{}
		atomic.AddInt64(&bh.changesPendingResponseCount, 1)

		bh.replicationStats.SendChangesCount.Add(int64(len(changeArray)))
		// Spawn a goroutine to await the client's response:
		go func(bh *blipHandler, sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, sendTime time.Time, database *Database) {
			if err := bh.handleChangesResponse(sender, response, changeArray, sendTime, database); err != nil {
				base.ErrorfCtx(bh.loggingCtx, "Error from bh.handleChangesResponse: %v", err)
			}

			// Sent all of the revs for this changes batch, allow another changes batch to be sent.
			select {
			case <-bh.inFlightChangesThrottle:
			case <-bh.terminator:
			}

			atomic.AddInt64(&bh.changesPendingResponseCount, -1)
		}(bh, sender, outrq.Response(), changeArray, sendTime, handleChangesResponseDb)
	} else {
		outrq.SetNoReply(true)
		if !bh.sendBLIPMessage(sender, outrq) {
			return ErrClosedBLIPSender
		}
	}

	if len(changeArray) > 0 {
		sequence := changeArray[0][0].(SequenceID)
		base.InfofCtx(bh.loggingCtx, base.KeySync, "Sent %d changes to client, from seq %s", len(changeArray), sequence.String())
	} else {
		base.InfofCtx(bh.loggingCtx, base.KeySync, "Sent all changes to client")
	}

	return nil
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handleChanges(rq *blip.Message) error {
	var ignoreNoConflicts bool
	if val := rq.Properties[ChangesMessageIgnoreNoConflicts]; val != "" {
		ignoreNoConflicts = val == "true"
	}

	if !ignoreNoConflicts && !bh.db.AllowConflicts() {
		return base.HTTPErrorf(http.StatusConflict, "Use 'proposeChanges' instead")
	}

	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		base.Warnf("Handle changes got error: %v", err)
		return err
	}

	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes:%d", len(changeList)))
	if len(changeList) == 0 {
		// An empty changeList is sent when a one-shot replication sends its final changes
		// message, or a continuous replication catches up *for the first time*.
		// Note that this doesn't mean that rev messages associated with previous changes
		// messages have been fully processed
		if bh.emptyChangesMessageCallback != nil {
			bh.emptyChangesMessageCallback()
		}
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 100*len(changeList)))
	output.Write([]byte("["))
	jsonOutput := base.JSONEncoder(output)
	nWritten := 0
	nRequested := 0

	// Include changes messages w/ proposeChanges stats, although CBL should only be using proposeChanges
	startTime := time.Now()
	bh.replicationStats.HandleChangesCount.Add(int64(len(changeList)))
	defer func() {
		bh.replicationStats.HandleChangesTime.Add(time.Since(startTime).Nanoseconds())
	}()

	// DocID+RevID -> SeqNo
	expectedSeqs := make(map[IDAndRev]SequenceID, 0)
	alreadyKnownSeqs := make([]SequenceID, 0)

	for _, change := range changeList {
		docID := change[1].(string)
		revID := change[2].(string)
		missing, possible := bh.db.RevDiff(docID, []string{revID})
		if nWritten > 0 {
			output.Write([]byte(","))
		}
		if missing == nil {
			// already have this rev, tell the peer to skip sending it
			output.Write([]byte("0"))
			if bh.sgr2PullAlreadyKnownSeqsCallback != nil {
				seq, err := ParseJSONSequenceID(seqStr(change[0]))
				if err != nil {
					base.WarnfCtx(bh.loggingCtx, "Unable to parse known sequence %q for %q / %q: %v", change[0], base.UD(docID), revID, err)
				} else {
					// we're not able to checkpoint a sequence we can't parse and aren't expecting so just skip the callback if we errored
					alreadyKnownSeqs = append(alreadyKnownSeqs, seq)
				}
			}
		} else {
			// we want this rev, send possible ancestors to the peer
			nRequested++
			if len(possible) == 0 {
				output.Write([]byte("[]"))
			} else {
				err := jsonOutput.Encode(possible)
				if err != nil {
					base.InfofCtx(bh.loggingCtx, base.KeyAll, "Error encoding json: %v", err)
				}
			}

			// skip parsing seqno if we're not going to use it (no callback defined)
			if bh.sgr2PullAddExpectedSeqsCallback != nil {
				seq, err := ParseJSONSequenceID(seqStr(change[0]))
				if err != nil {
					// We've already asked for the doc/rev for the sequence so assume we're going to receive it... Just log this and carry on
					base.WarnfCtx(bh.loggingCtx, "Unable to parse expected sequence %q for %q / %q: %v", change[0], base.UD(docID), revID, err)
				} else {
					expectedSeqs[IDAndRev{DocID: docID, RevID: revID}] = seq
				}
			}
		}
		nWritten++
	}
	output.Write([]byte("]"))
	response := rq.Response()
	if bh.sgCanUseDeltas {
		base.DebugfCtx(bh.loggingCtx, base.KeyAll, "Setting deltas=true property on handleChanges response")
		response.Properties[ChangesResponseDeltas] = "true"
		bh.replicationStats.HandleChangesDeltaRequestedCount.Add(int64(nRequested))
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())

	if bh.sgr2PullAddExpectedSeqsCallback != nil {
		bh.sgr2PullAddExpectedSeqsCallback(expectedSeqs)
	}
	if bh.sgr2PullAlreadyKnownSeqsCallback != nil {
		bh.sgr2PullAlreadyKnownSeqsCallback(alreadyKnownSeqs...)
	}

	return nil
}

// Handles a "proposeChanges" request, similar to "changes" but in no-conflicts mode
func (bh *blipHandler) handleProposeChanges(rq *blip.Message) error {

	includeConflictRev := false
	if val := rq.Properties[ProposeChangesConflictsIncludeRev]; val != "" {
		includeConflictRev = val == "true"
	}

	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes: %d", len(changeList)))
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 5*len(changeList)))
	output.Write([]byte("["))
	nWritten := 0

	// proposeChanges stats
	startTime := time.Now()
	bh.replicationStats.HandleChangesCount.Add(int64(len(changeList)))
	defer func() {
		bh.replicationStats.HandleChangesTime.Add(time.Since(startTime).Nanoseconds())
	}()

	for i, change := range changeList {
		docID := change[0].(string)
		revID := change[1].(string)
		parentRevID := ""
		if len(change) > 2 {
			parentRevID = change[2].(string)
		}
		status, currentRev := bh.db.CheckProposedRev(docID, revID, parentRevID)
		if status != 0 {
			// Skip writing trailing zeroes; but if we write a number afterwards we have to catch up
			if nWritten > 0 {
				output.Write([]byte(","))
			}
			for ; nWritten < i; nWritten++ {
				output.Write([]byte("0,"))
			}
			if includeConflictRev && status == ProposedRev_Conflict {
				revEntry := IncludeConflictRevEntry{Status: status, Rev: currentRev}
				entryBytes, marshalErr := base.JSONMarshal(revEntry)
				if marshalErr != nil {
					base.Warnf("Unable to marshal proposeChangesEntry as includeConflictRev - falling back to status-only entry.  Error: %v", marshalErr)
					output.Write([]byte(strconv.FormatInt(int64(status), 10)))
				}
				output.Write(entryBytes)

			} else {
				output.Write([]byte(strconv.FormatInt(int64(status), 10)))
			}
			nWritten++
		}
	}
	output.Write([]byte("]"))
	response := rq.Response()
	if bh.sgCanUseDeltas {
		base.DebugfCtx(bh.loggingCtx, base.KeyAll, "Setting deltas=true property on proposeChanges response")
		response.Properties[ChangesResponseDeltas] = "true"
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

//////// DOCUMENTS:

func (bsc *BlipSyncContext) sendRevAsDelta(sender *blip.Sender, docID, revID, deltaSrcRevID string, seq SequenceID, knownRevs map[string]bool, maxHistory int, handleChangesResponseDb *Database) error {

	bsc.replicationStats.SendRevDeltaRequestedCount.Add(1)

	revDelta, redactedRev, err := handleChangesResponseDb.GetDelta(docID, deltaSrcRevID, revID)
	if err == ErrForbidden {
		return err
	} else if base.IsDeltaError(err) {
		// Something went wrong in the diffing library. We want to know about this!
		base.WarnfCtx(bsc.loggingCtx, "Falling back to full body replication. Error generating delta from %s to %s for key %s - err: %v", deltaSrcRevID, revID, base.UD(docID), err)
		return bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDb)
	} else if err != nil {
		base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Falling back to full body replication. Couldn't get delta from %s to %s for key %s - err: %v", deltaSrcRevID, revID, base.UD(docID), err)
		return bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDb)
	}

	if redactedRev != nil {
		history := toHistory(redactedRev.History, knownRevs, maxHistory)
		properties := blipRevMessageProperties(history, redactedRev.Deleted, seq)
		return bsc.sendRevisionWithProperties(sender, docID, revID, redactedRev.BodyBytes, nil, properties, seq, nil)
	}

	if revDelta == nil {
		base.DebugfCtx(bsc.loggingCtx, base.KeySync, "Falling back to full body replication. Couldn't get delta from %s to %s for key %s", deltaSrcRevID, revID, base.UD(docID))
		return bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDb)
	}

	resendFullRevisionFunc := func() error {
		base.InfofCtx(bsc.loggingCtx, base.KeySync, "Resending revision as full body. Peer couldn't process delta %s from %s to %s for key %s", base.UD(revDelta.DeltaBytes), deltaSrcRevID, revID, base.UD(docID))
		return bsc.sendRevision(sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseDb)
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "docID: %s - delta: %v", base.UD(docID), base.UD(string(revDelta.DeltaBytes)))
	if err := bsc.sendDelta(sender, docID, deltaSrcRevID, revDelta, seq, resendFullRevisionFunc); err != nil {
		return err
	}

	bsc.replicationStats.SendRevDeltaSentCount.Add(1)
	return nil
}

func (bh *blipHandler) handleNoRev(rq *blip.Message) error {
	docID, revID := rq.Properties[NorevMessageId], rq.Properties[NorevMessageRev]
	var seqStr string
	if bh.clientType == BLIPClientTypeSGR2 {
		seqStr = rq.Properties[NorevMessageSeq]
	} else {
		seqStr = rq.Properties[NorevMessageSequence]
	}
	base.InfofCtx(bh.loggingCtx, base.KeySyncMsg, "%s: norev for doc %q / %q seq:%q - error: %q - reason: %q",
		rq.String(), base.UD(docID), revID, seqStr, rq.Properties[NorevMessageError], rq.Properties[NorevMessageReason])

	if bh.sgr2PullProcessedSeqCallback != nil {
		seq, err := ParseJSONSequenceID(seqStr)
		if err != nil {
			base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from norev message: %w - not tracking for checkpointing", seqStr, err)
		} else {
			bh.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: revID})
		}
	}

	// Couchbase Lite always sends noreply=true for norev profiles
	// but for testing purposes, it's useful to know which handler processed the message
	if !rq.NoReply() && rq.Properties[SGShowHandler] == "true" {
		response := rq.Response()
		response.Properties[SGHandler] = "handleNoRev"
	}

	return nil
}

type removalDocument struct {
	Removed bool `json:"_removed"`
}

// Received a "rev" request, i.e. client is pushing a revision body
func (bh *blipHandler) handleRev(rq *blip.Message) (err error) {
	startTime := time.Now()
	defer func() {
		bh.replicationStats.HandleRevProcessingTime.Add(time.Since(startTime).Nanoseconds())
		if err == nil {
			bh.BlipSyncContext.replicationStats.HandleRevCount.Add(1)
		} else {
			bh.BlipSyncContext.replicationStats.HandleRevErrorCount.Add(1)
		}
	}()

	//addRevisionParams := newAddRevisionParams(rq)
	revMessage := RevMessage{Message: rq}

	base.DebugfCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, rq.Profile(), revMessage.String())

	bodyBytes, err := rq.Body()
	if err != nil {
		return err
	}

	base.TracefCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Properties:%v  Body:%s", bh.serialNumber, base.UD(revMessage.Properties), base.UD(string(bodyBytes)))

	bh.replicationStats.HandleRevBytes.Add(int64(len(bodyBytes)))

	// Doc metadata comes from the BLIP message metadata, not magic document properties:
	docID, found := revMessage.ID()
	revID, rfound := revMessage.Rev()
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing docID or revID")
	}

	if bh.BlipSyncContext.purgeOnRemoval && bytes.Contains(bodyBytes, []byte(`"`+BodyRemoved+`":`)) {
		var r removalDocument
		if err := json.Unmarshal(bodyBytes, &r); err != nil {
			return err
		}
		if r.Removed {
			base.InfofCtx(bh.loggingCtx, base.KeySync, "Purging doc %v - removed at rev %v", docID, revID)
			if err := bh.db.Purge(docID); err != nil {
				return err
			}
			bh.replicationStats.HandleRevDocsPurgedCount.Add(1)
			if bh.sgr2PullProcessedSeqCallback != nil {
				seqStr := rq.Properties[RevMessageSequence]
				seq, err := ParseJSONSequenceID(seqStr)
				if err != nil {
					base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from rev message: %w - not tracking for checkpointing", seqStr, err)
				} else {
					bh.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: revID})
				}
			}
			return nil
		}
	}

	newDoc := &Document{
		ID:    docID,
		RevID: revID,
	}
	newDoc.UpdateBodyBytes(bodyBytes)

	injectedAttachmentsForDelta := false
	if deltaSrcRevID, isDelta := revMessage.DeltaSrc(); isDelta {
		if !bh.sgCanUseDeltas {
			return base.HTTPErrorf(http.StatusBadRequest, "Deltas are disabled for this peer")
		}

		//  TODO: Doing a GetRevCopy here duplicates some rev cache retrieval effort, since deltaRevSrc is always
		//        going to be the current rev (no conflicts), and PutExistingRev will need to retrieve the
		//        current rev over again.  Should push this handling down PutExistingRev and use the version
		//        returned via callback in WriteUpdate, but blocked by moving attachment metadata to a rev property first
		//        (otherwise we don't have information needed to do downloadOrVerifyAttachments below prior to PutExistingRev)

		// Note: Using GetRevCopy here, and not direct rev cache retrieval, because it's still necessary to apply access check
		//       while retrieving deltaSrcRevID.  Couchbase Lite replication guarantees client has access to deltaSrcRevID,
		//       due to no-conflict write restriction, but we still need to enforce security here to prevent leaking data about previous
		//       revisions to malicious actors (in the scenario where that user has write but not read access).
		deltaSrcRev, err := bh.db.GetRev(docID, deltaSrcRevID, false, nil)
		if err != nil {
			return base.HTTPErrorf(http.StatusNotFound, "Can't fetch doc for deltaSrc=%s %v", deltaSrcRevID, err)
		}

		// Receiving a delta to be applied on top of a tombstone is not valid.
		if deltaSrcRev.Deleted {
			return base.HTTPErrorf(http.StatusNotFound, "Can't use delta. Found tombstone for deltaSrc=%s", deltaSrcRevID)
		}

		deltaSrcBody, err := deltaSrcRev.DeepMutableBody()
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Unable to unmarshal mutable body for deltaSrc=%s %v", deltaSrcRevID, err)
		}

		// Stamp attachments so we can patch them
		if len(deltaSrcRev.Attachments) > 0 {
			deltaSrcBody[BodyAttachments] = map[string]interface{}(deltaSrcRev.Attachments)
			injectedAttachmentsForDelta = true
		}

		deltaSrcMap := map[string]interface{}(deltaSrcBody)
		err = base.Patch(&deltaSrcMap, newDoc.Body())
		if err != nil {
			// Something went wrong in the diffing library. We want to know about this!
			base.WarnfCtx(bh.loggingCtx, "Error patching deltaSrc %s with %s for key %s with delta - err: %v", deltaSrcRevID, revID, base.UD(docID), err)
			return base.HTTPErrorf(http.StatusInternalServerError, "Error patching deltaSrc with delta: %s", err)
		}

		newDoc.UpdateBody(deltaSrcMap)
		base.TracefCtx(bh.loggingCtx, base.KeySync, "docID: %s - body after patching: %v", base.UD(docID), base.UD(deltaSrcMap))
		bh.replicationStats.HandleRevDeltaRecvCount.Add(1)
	}

	// Handle and pull out expiry
	if bytes.Contains(bodyBytes, []byte(BodyExpiry)) {
		body := newDoc.Body()
		expiry, err := body.ExtractExpiry()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
		}
		newDoc.DocExpiry = expiry
		newDoc.UpdateBody(body)
	}

	newDoc.Deleted = revMessage.Deleted()

	// noconflicts flag from LiteCore
	// https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol#rev
	revNoConflicts := false
	if val, ok := rq.Properties[RevMessageNoConflicts]; ok {
		var err error
		revNoConflicts, err = strconv.ParseBool(val)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid value for noconflicts: %s", err)
		}
	}

	history := []string{revID}
	if historyStr := rq.Properties[RevMessageHistory]; historyStr != "" {
		history = append(history, strings.Split(historyStr, ",")...)
	}

	// Look at attachments with revpos > the last common ancestor's
	minRevpos := 1
	if len(history) > 0 {
		minRevpos, _ = ParseRevID(history[len(history)-1])
		// TODO: we can't identify at this point whether the last entry in history represents a
		// common ancestor, or is the oldest history for a newly inserted doc.  In the former case,
		// we'd prefer to run with minRevpos++, but since we can't determine without an additional
		// rev lookup, pay the cost for redundant attachment verification when
		// the attachment revpos equals the common ancestor
	}

	// Pull out attachments
	if injectedAttachmentsForDelta || bytes.Contains(bodyBytes, []byte(BodyAttachments)) {
		body := newDoc.Body()

		// Check for any attachments I don't have yet, and request them:
		if err := bh.downloadOrVerifyAttachments(rq.Sender, body, minRevpos, docID); err != nil {
			base.ErrorfCtx(bh.loggingCtx, "Error during downloadOrVerifyAttachments for doc %s/%s: %v", base.UD(docID), revID, err)
			return err
		}

		newDoc.DocAttachments = GetBodyAttachments(body)
		delete(body, BodyAttachments)
		newDoc.UpdateBody(body)
	}

	// Finally, save the revision (with the new attachments inline)
	// If a conflict resolver is defined for the handler, write with conflict resolution.

	// If the doc is a tombstone we want to allow conflicts when running SGR2
	// bh.conflictResolver != nil represents an active SGR2 and BLIPClientTypeSGR2 represents a passive SGR2
	forceAllowConflictingTombstone := newDoc.Deleted && (bh.conflictResolver != nil || bh.clientType == BLIPClientTypeSGR2)
	if bh.conflictResolver != nil {
		_, _, err = bh.db.PutExistingRevWithConflictResolution(newDoc, history, true, bh.conflictResolver, forceAllowConflictingTombstone)
	} else {
		_, _, err = bh.db.PutExistingRev(newDoc, history, revNoConflicts, forceAllowConflictingTombstone)
	}
	if err != nil {
		return err
	}

	if bh.sgr2PullProcessedSeqCallback != nil {
		seqProperty := rq.Properties[RevMessageSequence]
		seq, err := ParseJSONSequenceID(seqProperty)
		if err != nil {
			base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from rev message: %w - not tracking for checkpointing", seqProperty, err)
		} else {
			bh.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: revID})
		}
	}

	return nil
}

//////// ATTACHMENTS:

func (bh *blipHandler) handleProveAttachment(rq *blip.Message) error {
	nonce, err := rq.Body()
	if err != nil {
		return err
	}

	if len(nonce) == 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "no nonce sent with proveAttachment")
	}

	digest, ok := rq.Properties[ProveAttachmentDigest]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "no digest sent with proveAttachment")
	}

	attData, err := bh.db.GetAttachment(AttachmentKey(digest))
	if err != nil {
		panic(fmt.Sprintf("error getting client attachment: %v", err))
	}

	proof := ProveAttachment(attData, nonce)

	resp := rq.Response()
	resp.SetBody([]byte(proof))

	bh.replicationStats.HandleProveAttachment.Add(1)

	return nil
}

// Received a "getAttachment" request
func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {

	getAttachmentParams := newGetAttachmentParams(rq)
	bh.logEndpointEntry(rq.Profile(), getAttachmentParams.String())

	digest := getAttachmentParams.digest()
	if digest == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'digest'")
	}
	if !bh.isAttachmentAllowed(digest) {
		return base.HTTPErrorf(http.StatusForbidden, "Attachment's doc not being synced")
	}
	attachment, err := bh.db.GetAttachment(AttachmentKey(digest))
	if err != nil {
		return err

	}
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "Sending attachment with digest=%q (%dkb)", digest, len(attachment)/1024)
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties[BlipCompress] == "true")
	bh.replicationStats.HandleGetAttachment.Add(1)
	bh.replicationStats.HandleGetAttachmentBytes.Add(int64(len(attachment)))

	return nil
}

var NoBLIPHandlerError = fmt.Errorf("404 - No handler for BLIP request")

// sendGetAttachment requests the full attachment from the peer.
func (bh *blipHandler) sendGetAttachment(sender *blip.Sender, docID string, name string, digest string, meta map[string]interface{}) ([]byte, error) {
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Asking for attachment %q for doc %s (digest %s)", base.UD(name), base.UD(docID), digest)
	outrq := blip.NewRequest()
	outrq.Properties = map[string]string{BlipProfile: MessageGetAttachment, GetAttachmentDigest: digest}
	if isCompressible(name, meta) {
		outrq.Properties[BlipCompress] = "true"
	}
	if !bh.sendBLIPMessage(sender, outrq) {
		return nil, ErrClosedBLIPSender
	}

	resp := outrq.Response()

	respBody, err := resp.Body()
	if err != nil {
		return nil, err
	}

	lNum, metaLengthOK := meta["length"].(json.Number)
	metaLength, err := lNum.Int64()
	if err != nil {
		return nil, err
	}

	// Verify that the attachment we received matches the metadata stored in the document
	if !metaLengthOK || len(respBody) != int(metaLength) || Sha1DigestKey(respBody) != digest {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Incorrect data sent for attachment with digest: %s", digest)
	}

	bh.replicationStats.GetAttachment.Add(1)
	bh.replicationStats.GetAttachmentBytes.Add(metaLength)

	return respBody, nil
}

// sendProveAttachment asks the peer to prove they have the attachment, without actually sending it.
// This is to prevent clients from creating a doc with a digest for an attachment they otherwise can't access, in order to download it.
func (bh *blipHandler) sendProveAttachment(sender *blip.Sender, docID, name, digest string, knownData []byte) error {
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Verifying attachment %q for doc %s (digest %s)", base.UD(name), base.UD(docID), digest)
	nonce, proof := GenerateProofOfAttachment(knownData)
	outrq := blip.NewRequest()
	outrq.Properties = map[string]string{BlipProfile: MessageProveAttachment, ProveAttachmentDigest: digest}
	outrq.SetBody(nonce)
	if !bh.sendBLIPMessage(sender, outrq) {
		return ErrClosedBLIPSender
	}

	resp := outrq.Response()

	body, err := resp.Body()
	if err != nil {
		base.WarnfCtx(bh.loggingCtx, "Error returned for proveAttachment message for doc %s (digest %s).  Error: %v", base.UD(docID), digest, err)
		return err
	}

	if resp.Type() == blip.ErrorType &&
		resp.Properties["Error-Domain"] == blip.BLIPErrorDomain &&
		resp.Properties["Error-Code"] == "404" {
		return NoBLIPHandlerError
	}

	if string(body) != proof {
		base.WarnfCtx(bh.loggingCtx, "Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q", digest, base.MD(nonce), base.MD(proof), base.MD(string(body)))
		return base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
	}

	bh.replicationStats.ProveAttachment.Add(1)

	base.InfofCtx(bh.loggingCtx, base.KeySync, "proveAttachment successful for doc %s (digest %s)", base.UD(docID), digest)
	return nil
}

// For each attachment in the revision, makes sure it's in the database, asking the client to
// upload it if necessary. This method blocks until all the attachments have been processed.
func (bh *blipHandler) downloadOrVerifyAttachments(sender *blip.Sender, body Body, minRevpos int, docID string) error {
	return bh.db.ForEachStubAttachment(body, minRevpos,
		func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
			// request attachment if we don't have it
			if knownData == nil {
				return bh.sendGetAttachment(sender, docID, name, digest, meta)
			}

			// ask client to prove they have the attachemnt without sending it
			proveAttErr := bh.sendProveAttachment(sender, docID, name, digest, knownData)
			if proveAttErr == nil {
				return nil, nil
			}

			// peer doesn't support proveAttachment... Fall back to using getAttachment as proof.
			if proveAttErr == NoBLIPHandlerError {
				base.InfofCtx(bh.loggingCtx, base.KeySync, "Peer doesn't support proveAttachment, falling back to getAttachment for proof in doc %s (digest %s)", base.UD(docID), digest)
				_, getAttErr := bh.sendGetAttachment(sender, docID, name, digest, meta)
				if getAttErr == nil {
					return nil, nil
				}
				return nil, getAttErr
			}

			return nil, proveAttErr
		})
}

func (bsc *BlipSyncContext) incrementSerialNumber() uint64 {
	return atomic.AddUint64(&bsc.handlerSerialNumber, 1)
}

func (bsc *BlipSyncContext) addAllowedAttachments(attDigests []string) {
	if len(attDigests) == 0 {
		return
	}

	bsc.lock.Lock()
	defer bsc.lock.Unlock()

	if bsc.allowedAttachments == nil {
		bsc.allowedAttachments = make(map[string]int, 100)
	}
	for _, digest := range attDigests {
		bsc.allowedAttachments[digest] = bsc.allowedAttachments[digest] + 1
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "addAllowedAttachments, added: %v current set: %v", attDigests, bsc.allowedAttachments)
}

func (bsc *BlipSyncContext) removeAllowedAttachments(attDigests []string) {
	if len(attDigests) == 0 {
		return
	}

	bsc.lock.Lock()
	defer bsc.lock.Unlock()

	for _, digest := range attDigests {
		if n := bsc.allowedAttachments[digest]; n > 1 {
			bsc.allowedAttachments[digest] = n - 1
		} else {
			delete(bsc.allowedAttachments, digest)
		}
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "removeAllowedAttachments, removed: %v current set: %v", attDigests, bsc.allowedAttachments)
}

func (bh *blipHandler) logEndpointEntry(profile, endpoint string) {
	base.InfofCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, profile, endpoint)
}
