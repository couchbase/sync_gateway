package base

type RestBody map[string]interface{}

type RestDocument struct {
	RestBody
	Attachments AttachmentMap `json:"_attachments,omitempty"`
}

type AttachmentMap map[string]*DocAttachment

type DocAttachment struct {
	ContentType string `json:"content_type,omitempty"`
	Digest      string `json:"digest,omitempty"`
	Length      int    `json:"length,omitempty"`
	Revpos      int    `json:"revpos,omitempty"`
	Stub        bool   `json:"stub,omitempty"`
	Data        []byte  // TODO: add tag to tell json marshal/unmarshal to ignore this field
}

func NewRestDocument() *RestDocument {
	emptyBody := make(map[string]interface{})
	return &RestDocument{
		RestBody: emptyBody,
	}
}

func (d *RestDocument) SetID(docId string) {
	d.RestBody["_id"] = docId
}

func (d *RestDocument) ID() string {
	return d.RestBody["_id"].(string)
}

func (d *RestDocument) RevID() string {
	return d.RestBody["_rev"].(string)
}


func (d *RestDocument) SetRevID(revId string) {
	d.RestBody["_rev"] = revId
}

func (d *RestDocument) SetAttachments(attachments AttachmentMap) {
	d.Attachments = attachments
}

