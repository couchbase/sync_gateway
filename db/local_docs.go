//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

func (db *Database) GetLocal(docid string) (Body, error) {

	body := Body{}
	bytes, err := db.GetLocalBytes(docid)
	if err != nil {
		return nil, err
	}
	err = body.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}
	return body, err
}

func (db *DatabaseContext) GetLocalBytes(docid string) ([]byte, error) {
	key := LocalDocID(docid)

	if key == "" {
		return nil, base.HTTPErrorf(400, "Invalid doc ID")
	}

	var rawDocBytes []byte
	var err error
	if db.Options.LocalDocExpirySecs > 0 {
		rawDocBytes, _, err = db.Bucket.GetAndTouchRaw(key, base.SecondsToCbsExpiry(int(db.Options.LocalDocExpirySecs)))
	} else {
		rawDocBytes, _, err = db.Bucket.GetRaw(key)
	}
	if err != nil {
		return nil, err
	}
	return rawDocBytes, nil
}

// Updates or deletes a local document.
func (db *Database) putLocal(docid string, matchRev string, body Body) (string, error) {
	key := LocalDocID(docid)
	if key == "" {
		return "", base.HTTPErrorf(400, "Invalid doc ID")
	}
	var revid string

	expiry := base.SecondsToCbsExpiry(int(db.DatabaseContext.Options.LocalDocExpirySecs))
	_, err := db.Bucket.Update(key, expiry, func(value []byte) ([]byte, *uint32, bool, error) {
		if len(value) == 0 {
			if matchRev != "" || body == nil {
				return nil, nil, false, base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
			}
		} else {
			prevBody := Body{}
			if err := prevBody.Unmarshal(value); err != nil {
				return nil, nil, false, err
			}
			if matchRev != prevBody[BodyRev] {
				return nil, nil, false, base.HTTPErrorf(http.StatusConflict, "Document update conflict")
			}
		}

		if body != nil {
			// Updating:
			var generation uint
			if matchRev != "" {
				_, _ = fmt.Sscanf(matchRev, "0-%d", &generation)
			}
			revid = fmt.Sprintf("0-%d", generation+1)
			body[BodyRev] = revid
			bodyBytes, marshalErr := base.JSONMarshal(body)
			return bodyBytes, nil, false, marshalErr
		} else {
			// Deleting:
			return nil, nil, true, nil
		}
	})

	return revid, err
}

func (db *Database) PutLocal(docid string, body Body) (string, error) {
	matchRev, _ := body[BodyRev].(string)
	body, _ = stripAllSpecialProperties(body)
	return db.putLocal(docid, matchRev, body)
}

func (db *Database) DeleteLocal(docid string, revid string) error {
	_, err := db.putLocal(docid, revid, nil)
	return err
}

// LocalDocID returns a document ID for a "local" (non-replicated) document.
func LocalDocID(docid string) string {
	return base.DocTypeLocalPrefix + docid
}
