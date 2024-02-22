package rest

import (
	"log"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/db"
)

// HTTP handler for a GET of a document
func (h *handler) handleGetDocChannels() error {
	docid := h.PathVar("docid")

	doc, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalSync)
	if err != nil {
		return err
	}
	if doc == nil {
		return kNotFoundError
	}
	resp := make(map[string][]auth.GrantHistorySequencePair, len(doc.Channels))

	for _, chanSetInfo := range doc.SyncData.ChannelSet {
		resp[chanSetInfo.Name] = append(resp[chanSetInfo.Name], auth.GrantHistorySequencePair{StartSeq: chanSetInfo.Start, EndSeq: chanSetInfo.End})
		for _, hist := range doc.SyncData.ChannelSetHistory {
			if hist.Name == chanSetInfo.Name {
				resp[chanSetInfo.Name] = append(resp[chanSetInfo.Name], auth.GrantHistorySequencePair{StartSeq: hist.Start, EndSeq: hist.End})
				continue
			}
		}
	}
	log.Print(resp)

	h.writeJSON(resp)
	return nil
}
