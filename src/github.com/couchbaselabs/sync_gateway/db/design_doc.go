package db

import (
	"net/http"
	"strings"

	"github.com/couchbaselabs/sync_gateway/base"
)

func isInternalDDoc(ddocName string) bool {
	return strings.HasPrefix(ddocName, "sync_")
}

// Enforces access by admins only, and not to the built-in Sync Gateway design docs:
func (db *Database) checkDDocAccess(ddocName string) error {
	if db.user != nil || isInternalDDoc(ddocName) {
		return base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}
	return nil
}

func (db *Database) GetDesignDoc(ddocName string, result interface{}) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.GetDDoc(ddocName, result)
	}
	return
}

func (db *Database) PutDesignDoc(ddocName string, body Body) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.PutDDoc(ddocName, body)
	}
	return
}

func (db *Database) DeleteDesignDoc(ddocName string) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.DeleteDDoc(ddocName)
	}
	return
}

func (db *Database) QueryDesignDoc(ddocName string, viewName string, options map[string]interface{}, result interface{}) error {
	// Query has slightly different access control than checkDDocAccess():
	// * Admins can query any design doc including the internal ones
	// * Regular users can query non-internal design docs
	//NOTE: Restricting query access to admins until we implement proper result-set filtering.
	if db.user != nil /*&& isInternalDDoc(ddocName)*/ {
		return base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}
	return db.Bucket.ViewCustom(ddocName, viewName, options, result)
}
