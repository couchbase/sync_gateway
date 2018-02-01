package base

import (
	"encoding/json"
)

type AttachmentMap map[string]*DocAttachment

type DocAttachment struct {
	ContentType string `json:"content_type,omitempty"`
	Digest      string `json:"digest,omitempty"`
	Length      int    `json:"length,omitempty"`
	Revpos      int    `json:"revpos,omitempty"`
	Stub        bool   `json:"stub,omitempty"`
	Data        []byte `json:-` // tell json marshal/unmarshal to ignore this field
}

// --------------------------------------------

type RestDocument map[string]interface{}

func NewRestDocument() *RestDocument {
	emptyBody := make(map[string]interface{})
	restDoc := RestDocument(emptyBody)
	return &restDoc
}

func (d RestDocument) ID() string {
	rawID, hasID := d["_id"]
	if !hasID {
		return ""
	}
	return rawID.(string)

}

func (d RestDocument) SetID(docId string) {
	d["_id"] = docId
}

func (d RestDocument) RevID() string {
	rawRev, hasRev := d["_rev"]
	if !hasRev {
		return ""
	}
	return rawRev.(string)
}

func (d RestDocument) SetRevID(revId string) {
	d["_rev"] = revId
}

func (d RestDocument) SetAttachments(attachments AttachmentMap) {
	d["_attachments"] = attachments
}

func (d RestDocument) GetAttachments() (AttachmentMap, error) {

	rawAttachments, hasAttachments := d["_attachments"]

	// If the map doesn't even have the _attachments key, return an empty attachments map
	if !hasAttachments {
		return AttachmentMap{}, nil
	}

	// Otherwise, create an AttachmentMap from the value in the raw map
	attachmentMap := AttachmentMap{}
	switch v := rawAttachments.(type) {
	case AttachmentMap:
		// If it's already an AttachmentMap (maybe due to previous call to SetAttachments), then return as-is
		return v, nil
	default:
		rawAttachmentsMap := v.(map[string]interface{})
		for attachmentName, attachmentVal := range rawAttachmentsMap {

			// marshal attachmentVal into a byte array, then unmarshal into a DocAttachment
			attachmentValMarshalled, err := json.Marshal(attachmentVal)
			if err != nil {
				return AttachmentMap{}, err
			}
			docAttachment := DocAttachment{}
			if err := json.Unmarshal(attachmentValMarshalled, &docAttachment); err != nil {
				return AttachmentMap{}, err
			}

			attachmentMap[attachmentName] = &docAttachment
		}

		// Avoid the unnecessary re-Marshal + re-Unmarshal
		d.SetAttachments(attachmentMap)
	}

	return attachmentMap, nil

}
