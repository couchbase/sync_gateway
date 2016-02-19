package indexwriter

import (
	"sync"

	"github.com/couchbase/sync_gateway/db"
)

const (
	maxCacheUpdate        = 2000
	minCacheUpdate        = 1
	maxUnmarshalProcesses = 16
)

type IndexWriter struct {
	dbIndexWriters    map[string]*kvChangeIndexWriter // Map of index writers, indexed by db name
	dbIndexWriterLock sync.RWMutex                    // Coordinates access to channel index writer map.
}

// An Index Writer coordinates index writing handling across multiple databases.
func NewIndexWriter() *IndexWriter {
	return &IndexWriter{
		dbIndexWriters: make(map[string]*kvChangeIndexWriter),
	}
}

func (i *IndexWriter) GetDbIndexWriter(dbName string) *kvChangeIndexWriter {
	i.dbIndexWriterLock.RLock()
	defer i.dbIndexWriterLock.RUnlock()
	writer, ok := i.dbIndexWriters[dbName]
	if ok {
		return writer
	} else {
		return nil
	}
}

func (i *IndexWriter) AddDbIndexWriter(context *db.DatabaseContext, cbgtContext *CbgtContext) (*kvChangeIndexWriter, error) {
	i.dbIndexWriterLock.Lock()
	defer i.dbIndexWriterLock.Unlock()

	writer := &kvChangeIndexWriter{}
	err := writer.Init(context, cbgtContext)
	if err != nil {
		return nil, err
	}
	i.dbIndexWriters[context.Name] = writer

	return writer, nil
}
