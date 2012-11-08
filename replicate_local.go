// replicate_local.go

package basecouch

import (
	"log"
	"net/http"
	"github.com/couchbaselabs/go-couchbase"
)

func (db *Database) localReplicateFrom(source *Database) error {
	since, err := db.getCheckpoint(source)
	if err != nil {return err}
	if LogRequestsVerbose {
		log.Printf("Replicating %s -> %s from #%d ...", source.Name, db.Name, since)
	}
	options := ChangesOptions{Since: since, includeDocMeta: true}
	changes, err := source.ChangesFeed(options)
	if err != nil {
		return err
	}
	lastSeq := since
	for change := range changes {
		// Update one document:
		srcDoc := change.docMeta
		_, err = db.updateDoc(change.ID, func(dstDoc *document)(Body, error) {
			if !dstDoc.History.mergeWith(srcDoc.History) {
				return nil, couchbase.UpdateCancel	// no change to dstDoc
			}
			return nil, nil
		})
		if err != nil {
			log.Printf("Error replicating %s -> %s at #%d: %v", source.Name, db.Name, lastSeq, err)
			break
		}
		lastSeq = change.Seq
		if LogRequestsVerbose && lastSeq % 100 == 0 {
			log.Printf("\t#%d ...", lastSeq)
		}
	}
	if lastSeq > since {
		db.setCheckpoint(source, lastSeq)
	}
	if LogRequestsVerbose {
		log.Printf("Finished %s -> %s; checkpoint = #%d.", source.Name, db.Name, lastSeq)
	}
	return nil
}

func (db *Database) getCheckpoint(source *Database) (uint64, error) {
	key := source.UUID()
	checkpoints, err := db.GetLocal("__checkpoints")
	if isMissingDocError(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	checkpoint,_ := checkpoints[key].(float64)
	return uint64(checkpoint), nil
}

func (db *Database) setCheckpoint(source *Database, checkpoint uint64) (error) {
	key := source.UUID()
	var err error
	for {
		checkpoints, err := db.GetLocal("__checkpoints")
		if isMissingDocError(err) {
			checkpoints = Body{}
		} else if err != nil {
			break
		}
		curCheckpoint,_ := checkpoints[key].(float64)
		if uint64(curCheckpoint) >= checkpoint {
			break
		}
		checkpoints[key] = checkpoint
		_,err = db.PutLocal("__checkpoints", checkpoints)
		if err == nil {
			break
		}
		status,_ := err.(*HTTPError)
		if status.Status != http.StatusConflict {
			break
		}
		// ... on conflict, loop again
	}
	return err
}
