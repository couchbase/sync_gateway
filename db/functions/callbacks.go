package functions

import (
	"context"
	"log"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

//////// DATABASE EVALUATOR DELEGATE:

type databaseDelegate struct {
	dbc *db.Database    // The database (with user)
	ctx context.Context // Context for logging, timeouts etc.
}

func (d *databaseDelegate) query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rows []any, err error) {
	return nil, base.HTTPErrorf(http.StatusNotImplemented, "query unimplemented")
}

func (d *databaseDelegate) get(docID string, asAdmin bool) (doc map[string]any, err error) {
	if err := d.dbc.CheckTimeout(d.ctx); err != nil {
		return nil, err
	} else if asAdmin {
		user := d.dbc.User()
		d.dbc.SetUser(nil)
		defer func() { d.dbc.SetUser(user) }()
	}
	rev, err := d.dbc.GetRev(d.ctx, docID, "", false, nil)
	if err != nil {
		status, _ := base.ErrorAsHTTPStatus(err)
		if status == http.StatusNotFound {
			// Not-found is not an error; just return null.
			return nil, nil
		}
		return nil, err
	}
	body, err := rev.Body()
	if err != nil {
		return nil, err
	}
	body["_id"] = docID
	body["_rev"] = rev.RevID
	return body, nil
}

func (d *databaseDelegate) save(body map[string]any, docID string, asAdmin bool) (bool, error) {
	if err := d.dbc.CheckTimeout(d.ctx); err != nil {
		return false, err
	} else if asAdmin {
		user := d.dbc.User()
		d.dbc.SetUser(nil)
		defer func() { d.dbc.SetUser(user) }()
	}

	delete(body, "_id")
	if _, found := body["_rev"]; found {
		// If caller provided `_rev` property, use MVCC as normal:
		if _, _, err := d.dbc.Put(d.ctx, docID, body); err != nil {
			if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
				err = nil // conflict: no error, but returns false
			}
			return false, err
		}

	} else {
		// If caller didn't provide a `_rev` property, fall back to "last writer wins":
		// get the current revision if any, and pass it to Put so that the save always succeeds.
		for {
			rev, err := d.dbc.GetRev(d.ctx, docID, "", false, []string{})
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusNotFound {
					return false, err
				} else if del, found := body["_deleted"].(bool); found && del {
					// Deleting nonexistent doc: success
					return true, nil
				}
			}
			if rev.RevID == "" {
				delete(body, "_rev")
			} else {
				body["_rev"] = rev.RevID
			}

			_, _, err = d.dbc.Put(d.ctx, docID, body)
			if err == nil {
				break // success!
			} else if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusConflict {
				return false, err
			}
			// on conflict (race condition), retry...
		}
	}
	// success
	return true, nil
}

func (d *databaseDelegate) delete(docID string, revID string, asAdmin bool) (bool, error) {
	tombstone := map[string]any{"_deleted": true}
	if revID != "" {
		tombstone["_rev"] = revID
	}
	return d.save(tombstone, docID, asAdmin)
}

func (d *databaseDelegate) log(level base.LogLevel, message string) {
	log.Printf("JS LOG: (%d) %s", level, message) //TEMP
	base.LogfTo(d.ctx, level, base.KeyJavascript, "%s", base.UD(message))
}
