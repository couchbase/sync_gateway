// localdocs.go

package basecouch

// Gets a local document.
func (db *Database) GetLocal(docid string) (Body, error) {
	key := db.realLocalDocID(docid)
	if key == "" {
		return nil, &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}

	body := Body{}
	err := db.bucket.Get(key, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Updates a local document.
func (db *Database) PutLocal(docid string, body Body) error {
	key := db.realLocalDocID(docid)
	if key == "" {
		return &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}

	return db.bucket.Set(db.realLocalDocID(docid), 0, body)
}

// Deletes a local document.
func (db *Database) DeleteLocal(docid string) error {
	key := db.realLocalDocID(docid)
	if key == "" {
		return &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	return db.bucket.Delete(db.realLocalDocID(docid))
}

func (db *Database) realLocalDocID(docid string) string {
	return db.realDocID("_local/" + docid)
}
