//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"slices"
	"time"
	"unicode/utf8"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// Instead of using `sgbucket.DataStore`, define a smaller interface covering what we need.
// This makes it easier to mock out in tests; see `mockDataStore`.
type iDataStore interface {
	sgbucket.KVStore
	GetName() string
}

// Internal struct used by `ingestMessage()`
type ingester struct {
	ctx       context.Context
	topic     TopicMatch
	payload   any
	sub       *IngestConfig
	dataStore iDataStore
	docID     string
	exp       uint32
	timestamp time.Time
}

// Finds an IngestConfig that matches the given topic name.
func (bc *BrokerConfig) matchIngest(topic string) (*IngestConfig, *TopicMatch) {
	bc.mutex.Lock()
	defer func() { bc.mutex.Unlock() }()

	if bc.ingestFilters == nil {
		if tm, err := MakeTopicMap(bc.Ingest); err == nil {
			bc.ingestFilters = &tm
		} else {
			return nil, nil // should have been caught in validation
		}
	}
	return bc.ingestFilters.Match(topic)
}

// Saves an incoming MQTT message to a document in a database. Returns the docID.
func ingestMessage(
	ctx context.Context,
	topic TopicMatch,
	rawPayload []byte,
	sub *IngestConfig,
	dbc *db.DatabaseContext,
	exp uint32,
) error {
	dataStore, err := dbc.Bucket.NamedDataStore(sgbucket.DataStoreNameImpl{Scope: sub.Scope, Collection: sub.Collection})
	if err != nil {
		return err
	}
	return ingestMessageToDataStore(ctx, topic, rawPayload, sub, dataStore, exp)
}

// Same as `ingestMessage` but takes a (i)DataStore.
func ingestMessageToDataStore(
	ctx context.Context,
	topic TopicMatch,
	rawPayload []byte,
	sub *IngestConfig,
	dataStore iDataStore,
	exp uint32,
) error {
	ing := ingester{
		ctx:       ctx,
		topic:     topic,
		sub:       sub,
		dataStore: dataStore,
		docID:     sub.DocID,
		exp:       exp,
		timestamp: time.Now(),
	}

	// Parse the payload per the `encoding` config:
	err := ing.decodePayload(rawPayload)
	if err != nil {
		return fmt.Errorf("failed to parse message from topic %q: %w", topic, err)
	}

	if ing.docID == "" {
		ing.docID = topic.Name
	} else {
		tmpl := templater{
			payload:   nil,
			timestamp: time.Time{},
			topic:     topic,
		}
		if docID, ok := tmpl.expand(ing.docID).(string); ok {
			ing.docID = docID
		} else {
			return fmt.Errorf("doc_id template %q expands to a non-string", ing.docID)
		}
	}

	switch *ing.sub.Model {
	case ModelState:
		err = ing.saveState()
		if err == nil {
			base.InfofCtx(ing.ctx, base.KeyMQTT, "Saved msg as doc %q in db %s", base.UD(ing.docID), base.UD(ing.dataStore.GetName()))
		}
	case ModelTimeSeries:
		var entry []any
		entry, err = applyTimeSeriesTemplate(ing.sub.TimeSeries, ing.payload, ing.timestamp, false)
		if err == nil {
			err = ing.saveTimeSeries(entry, "ts_data", 0)
		}
	case ModelSpaceTimeSeries:
		var entry []any
		entry, err = applySpaceTimeSeriesTemplate(ing.sub.SpaceTimeSeries, ing.payload, ing.timestamp, false)
		if err == nil {
			err = ing.saveTimeSeries(entry, "spts_data", 1)
		}
	default:
		err = fmt.Errorf("invalid 'model' in subscription config") // validation will have caught this
	}
	return err
}

// Applies the config's encoding to a raw MQTT payload.
func (ing *ingester) decodePayload(rawPayload []byte) error {
	encoding := EncodingString
	if ing.sub.Encoding != nil {
		encoding = *ing.sub.Encoding
	}
	switch encoding {
	case EncodingBase64:
		ing.payload = base64.StdEncoding.EncodeToString(rawPayload)

	case EncodingString:
		if str := string(rawPayload); utf8.ValidString(str) {
			ing.payload = str
		} else {
			return fmt.Errorf("invalid UTF-8")
		}

	case EncodingJSON:
		var j any
		if err := base.JSONUnmarshal(rawPayload, &j); err == nil {
			ing.payload = j
		} else {
			return fmt.Errorf("invalid JSON: %w", err)
		}

	default:
		return fmt.Errorf("invalid 'transform' in subscription config")
	}
	return nil
}

// Saves a document using the "state" model.
func (ing *ingester) saveState() error {
	body, err := applyStateTemplate(ing.sub.StateTemplate, ing.payload, ing.timestamp, ing.topic)
	if err != nil {
		return err
	} else if err = ing.dataStore.Set(ing.docID, ing.exp, nil, body); err != nil {
		return err
	}
	base.InfofCtx(ing.ctx, base.KeyMQTT, "Saved msg to doc %q in db %s", base.UD(ing.docID), base.UD(ing.dataStore.GetName()))
	return nil
}

// Saves a document using the "time_series" or "space_time_series" model.
func (ing *ingester) saveTimeSeries(entry []any, seriesKey string, timeStampIndex int) error {
	_, err := ing.dataStore.Update(ing.docID, ing.exp, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		var body Body
		if err := base.JSONUnmarshal(current, &body); err != nil || body == nil {
			// Ignore error (or empty doc); just clear the body to recover
			body = Body{}
		}

		ts_new, _ := decodeTimestamp(entry[timeStampIndex])
		tsStart, hasStart := decodeTimestamp(body["ts_start"])
		if ing.shouldRotate(len(current), tsStart, ts_new) {
			// Time to roll over the document -- save to a new key, and clear the contents
			var added bool
			added, err = ing.rotateTimeSeries(body)
			if err == nil && !added {
				// Race condition: someone else rotated the doc already. Reload and retry.
				err = sgbucket.ErrCasFailureShouldRetry
			}
			if err != nil {
				return
			}
			// Now that we've saved the time series, clear the doc and restart:
			body = map[string]any{}
			hasStart = false
		}

		body[seriesKey] = addToTimeSeries(body[seriesKey], entry, timeStampIndex)

		// Update start/end timestamps:
		if !hasStart || ts_new < tsStart {
			body["ts_start"] = ts_new
		}
		if tsEnd, ok := decodeTimestamp(body["ts_end"]); !ok || ts_new > tsEnd {
			body["ts_end"] = ts_new
		}

		if *ing.sub.Model == ModelSpaceTimeSeries {
			// Update low/high geohashes:
			sp_new := entry[0].(string)
			if sp_low, ok := body["sp_low"].(string); !ok || sp_new < sp_low {
				body["sp_low"] = sp_new
			}
			if sp_high, ok := body["sp_high"].(string); !ok || sp_new > sp_high {
				body["sp_high"] = sp_new
			}
		}

		// Update other properties, if configured:
		if props := ing.sub.TimeSeries.OtherProperties; len(props) > 0 {
			props, err = applyStateTemplate(props, ing.payload, ing.timestamp, ing.topic)
			if err != nil {
				return
			}
			for k, v := range props {
				if k != seriesKey && k != "ts_start" && k != "ts_end" && k != "sp_low" && k != "sp_high" {
					body[k] = v
				}
			}
		}

		updated, err = base.JSONMarshal(body)
		return
	})
	if err == nil {
		base.InfofCtx(ing.ctx, base.KeyMQTT, "Appended msg to %s doc %q in db %s", *ing.sub.Model, base.UD(ing.docID), base.UD(ing.dataStore.GetName()))
	}
	return err
}

// Adds an entry to a (space-)time-series array, in chronological order, unless it's a dup.
func addToTimeSeries(seriesProp any, entry []any, timeStampIndex int) []any {
	newTimeStamp, _ := decodeTimestamp(entry[timeStampIndex])
	series, _ := seriesProp.([]any)
	for i, item := range series {
		if oldEntry, ok := item.([]any); ok && len(oldEntry) > timeStampIndex {
			oldTimeStamp, _ := decodeTimestamp(oldEntry[timeStampIndex])
			if newTimeStamp < oldTimeStamp {
				// New item comes before this one:
				return slices.Insert(series, i, any(entry))
			} else if newTimeStamp == oldTimeStamp && reflect.DeepEqual(entry, oldEntry) {
				// It's a duplicate!
				return series
			}
		}
	}
	return append(series, entry)
}

// Returns true if it's time to rotate a time-series doc, given its size and earliest timestamp.
func (ing *ingester) shouldRotate(docSize int, startTime float64, curTime float64) bool {
	var rotationMaxSize int
	var rotationInterval time.Duration
	if ing.sub.TimeSeries != nil {
		rotationMaxSize = ing.sub.TimeSeries.RotationMaxSize
		rotationInterval = ing.sub.TimeSeries.rotationInterval
	} else if ing.sub.SpaceTimeSeries != nil {
		rotationMaxSize = ing.sub.SpaceTimeSeries.RotationMaxSize
		rotationInterval = ing.sub.SpaceTimeSeries.rotationInterval
	}

	if rotationMaxSize > 0 && docSize >= rotationMaxSize {
		return true
	} else if rotationInterval > 0 && startTime > 0 && curTime-startTime >= rotationInterval.Seconds() {
		return true
	}
	return false
}

// Called when a (parsed) time series doc is too large to add to.
// Copies the existing data to a new doc whose key has the first timestamp appended.
func (ing *ingester) rotateTimeSeries(body Body) (added bool, err error) {
	var startStr string
	if start, ok := decodeTimestamp(body["ts_start"]); ok {
		m, _ := timeFromTimestamp(start).MarshalText()
		startStr = string(m)
	}
	docID := fmt.Sprintf("%s @ %s", ing.docID, startStr)
	return ing.dataStore.Add(docID, ing.exp, body)
}
