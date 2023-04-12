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
	"github.com/couchbase/sync_gateway/document"
)

const DocTypeLocal = "local"

func (c *DatabaseCollection) GetSpecial(doctype string, docid string) (Body, error) {

	body := Body{}
	bytes, err := c.GetSpecialBytes(doctype, docid)
	if err != nil {
		return nil, err
	}
	err = body.Unmarshal(bytes)
	if err != nil {
		return nil, err
	}
	return body, err
}

func (c *DatabaseCollection) GetSpecialBytes(doctype string, docid string) ([]byte, error) {
	return getSpecialBytes(c.dataStore, doctype, docid, int(c.localDocExpirySecs()))
}

func getWithTouch(dataStore base.DataStore, docID string, expirySecs int) ([]byte, error) {
	var rawDocBytes []byte
	var err error
	if expirySecs > 0 {
		expiry := base.SecondsToCbsExpiry(expirySecs)
		rawDocBytes, _, err = dataStore.GetAndTouchRaw(docID, expiry)
	} else {
		rawDocBytes, _, err = dataStore.GetRaw(docID)
	}
	if err != nil {
		return nil, err
	}
	return rawDocBytes, nil
}

func getSpecialBytes(dataStore base.DataStore, doctype string, docID string, localDocExpirySecs int) ([]byte, error) {
	key := RealSpecialDocID(doctype, docID)
	if key == "" {
		return nil, base.HTTPErrorf(400, "Invalid doc ID")
	}
	// only local docs should have expiry based on localDocExpirySecs
	if doctype != DocTypeLocal {
		localDocExpirySecs = 0
	}
	return getWithTouch(dataStore, key, localDocExpirySecs)
}

// Updates or deletes a document with BodyRev-based version control.
func putDocWithRevision(dataStore base.DataStore, docID string, matchRev string, body Body, expirySecs int) (newRevID string, err error) {
	var revid string

	var expiry uint32
	if expirySecs > 0 {
		expiry = base.SecondsToCbsExpiry(expirySecs)
	}
	_, err = dataStore.Update(docID, expiry, func(value []byte) ([]byte, *uint32, bool, error) {
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

func putSpecial(dataStore base.DataStore, doctype string, docid string, matchRev string, body Body, localDocExpirySecs int) (string, error) {
	key := RealSpecialDocID(doctype, docid)
	if key == "" {
		return "", base.HTTPErrorf(400, "Invalid doc ID")
	}
	// only local docs should have expiry based on localDocExpirySecs
	if doctype != DocTypeLocal {
		localDocExpirySecs = 0
	}
	return putDocWithRevision(dataStore, key, matchRev, body, localDocExpirySecs)
}

// Updates or deletes a special document.
func (c *DatabaseCollection) putSpecial(doctype string, docid string, matchRev string, body Body) (string, error) {
	return putSpecial(c.dataStore, doctype, docid, matchRev, body, int(c.localDocExpirySecs()))
}

func (c *DatabaseCollection) PutSpecial(doctype string, docid string, body Body) (string, error) {
	matchRev, _ := body[BodyRev].(string)
	body, _ = document.StripAllSpecialProperties(body)
	return c.putSpecial(doctype, docid, matchRev, body)
}

func (c *DatabaseCollection) DeleteSpecial(doctype string, docid string, revid string) error {
	_, err := c.putSpecial(doctype, docid, revid, nil)
	return err
}

func RealSpecialDocID(doctype string, docid string) string {
	return base.SyncDocPrefix + doctype + ":" + docid
}
