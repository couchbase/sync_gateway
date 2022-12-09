package functions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

type n1qlUserArgument struct {
	Name     *string  `json:"name,omitempty"`
	Channels []string `json:"channels,omitempty"`
	Roles    []string `json:"roles,omitempty"`
}

//////// DATABASE EVALUATOR DELEGATE:

// The "real" implementation of EvaluatorDelegate.
type databaseDelegate struct {
	db   *db.Database     // The database (with user)
	ctx  context.Context  // Context for timeouts etc.
	user *userCredentials // User's info
}

func (d *databaseDelegate) init(dbc *db.Database, ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("missing Context")
	}
	if err := db.CheckTimeout(ctx); err != nil {
		return err
	}
	d.db = dbc
	d.ctx = ctx
	if dbUser := dbc.User(); dbUser != nil {
		d.user = &userCredentials{
			Name:     dbUser.Name(),
			Roles:    dbUser.RoleNames().AllKeys(),
			Channels: dbUser.Channels().AllKeys(),
		}
	}
	return nil
}

func (d *databaseDelegate) getCollection(collectionName string) (*db.DatabaseCollectionWithUser, error) {
	return d.db.GetDatabaseCollectionWithUser(base.DefaultScope, collectionName)
}

// Temporarily gives the `db.Database` admin powers. Returns a fn that will revert the upgrade.
// Must be called as `defer d.asAdmin()()`
func (d *databaseDelegate) asAdmin() func() {
	user := d.db.User()
	d.db.SetUser(nil)
	return func() { d.db.SetUser(user) } // <-- this is what will be run by `defer`
}

func (d *databaseDelegate) checkTimeout() error {
	return db.CheckTimeout(d.ctx)
}

func (d *databaseDelegate) get(docID string, collectionName string, asAdmin bool) (doc map[string]any, err error) {
	if asAdmin {
		defer d.asAdmin()()
	}
	collection, err := d.getCollection(collectionName)
	if err != nil {
		return nil, err
	}
	rev, err := collection.GetRev(d.ctx, docID, "", false, nil)
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

func (d *databaseDelegate) save(body map[string]any, docID string, collectionName string, asAdmin bool) (bool, error) {
	if asAdmin {
		defer d.asAdmin()()
	}
	delete(body, "_id")
	collection, err := d.getCollection(collectionName)
	if err != nil {
		return false, err
	}
	if _, found := body["_rev"]; found {
		// If caller provided `_rev` property, use MVCC as normal:
		if _, _, err := collection.Put(d.ctx, docID, body); err != nil {
			if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
				err = nil // conflict: no error, but returns false
			}
			return false, err
		}

	} else {
		// If caller didn't provide a `_rev` property, fall back to "last writer wins":
		// get the current revision if any, and pass it to Put so that the save always succeeds.
		for {
			rev, err := collection.GetRev(d.ctx, docID, "", false, []string{})
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

			_, _, err = collection.Put(d.ctx, docID, body)
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

func (d *databaseDelegate) delete(docID string, revID string, collectionName string, asAdmin bool) (bool, error) {
	tombstone := map[string]any{"_deleted": true}
	if revID != "" {
		tombstone["_rev"] = revID
	}
	return d.save(tombstone, docID, collectionName, asAdmin)
}

func (d *databaseDelegate) query(fnName string, n1ql string, args map[string]any, asAdmin bool) (string, error) {
	collection, err := d.db.GetDefaultDatabaseCollection()
	if err != nil {
		return "", err
	}

	var userArg n1qlUserArgument
	if asAdmin {
		defer d.asAdmin()()
	} else if d.user != nil {
		userArg.Name = base.StringPtr(d.user.Name)
		userArg.Channels = d.user.Channels
		userArg.Roles = d.user.Roles
	}
	if args == nil {
		args = map[string]any{}
	}
	args["user"] = &userArg

	// Run the N1QL query:
	rows, err := collection.Query(
		d.ctx,
		db.QueryTypeUserFunctionPrefix+fnName,
		n1ql,
		args,
		base.RequestPlus,
		false)

	var rowsJSON string
	if err == nil {
		// JSON-encode the iterator (see below):
		rowsJSON, err = d.writeRowsToJSON(rows)
	}
	if err != nil {
		// Return a friendlier error:
		var qe *gocb.QueryError
		if errors.As(err, &qe) {
			base.WarnfCtx(d.ctx, "Error running query %q: %v", fnName, err)
			return "", base.HTTPErrorf(http.StatusInternalServerError, "Query %q: %s", fnName, qe.Errors[0].Message)
		} else {
			base.WarnfCtx(d.ctx, "Unknown error running query %q: %T %#v", fnName, err, err)
			return "", base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query %q (see logs)", fnName)
		}
	}
	return rowsJSON, nil
}

// Subroutine of `query` that encodes the QueryResultIterator as a JSON array.
// Guarantees to close the iterator.
func (d *databaseDelegate) writeRowsToJSON(rows sgbucket.QueryResultIterator) (string, error) {
	defer func() {
		if rows != nil {
			_ = rows.Close() // only called if there's an error already
		}
	}()

	var out bytes.Buffer
	out.Write([]byte(`[`))
	first := true
	var row interface{}
	for rows.Next(&row) {
		if first {
			first = false
		} else {
			if _, err := out.Write([]byte(`,`)); err != nil {
				return "", err
			}
		}
		enc := base.JSONEncoderCanonical(&out)
		if err := enc.Encode(row); err != nil {
			return "", err
		}
		// The iterator streams results as the query engine produces them, so this loop may take most of the query's time; check for timeout after each iteration:
		if err := db.CheckTimeout(d.ctx); err != nil {
			return "", err
		}
	}
	err := rows.Close()
	rows = nil // prevent double-close on exit
	if err != nil {
		return "", err
	}
	if _, err := out.Write([]byte("]\n")); err != nil {
		return "", err
	}
	return out.String(), nil
}
