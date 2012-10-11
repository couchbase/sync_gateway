// attachment.go

package basecouch

import (
	"crypto/sha1"
    "encoding/base64"
	"fmt"
)

// Key for retrieving an attachment.
type AttachmentKey string

type Attachments map[string]Body

func (db *Database) storeAttachments(body Body, generation int, parentKey RevKey) error {
    atts, exists := body["_attachments"].(Attachments)
    if !exists {
        return nil
    }
    var parentAttachments Attachments
    for name,value := range(atts) {
        meta := value
        data, exists := meta["data"]
        if exists {
            key, err := db.setAttachmentBase64(data.(string))
            if err != nil {
                return err
            }
            delete(meta, "data")
            meta["digest"] = string(key)
            meta["revpos"] = generation
        } else {
            // No data given; look it up from the parent revision:
            if parentAttachments == nil {
                parent, err := db.getRevision("", "", parentKey)  // Don't care about docid,revid
                if err != nil {
                    return err
                }
                parentAttachments, exists = parent["_attachments"].(Attachments)
                if !exists {
                    return &HTTPError{Status: 400, Message: "Unknown attachment"}
                }
            }
            value = parentAttachments[name]
            if value == nil {
                return &HTTPError{Status: 400, Message: "Unknown attachment"}
            }
            atts[name] = value
        }
    }
    return nil
}

func (db *Database) fillInAttachments(body Body) error {
    atts, exists := body["_attachments"].(Attachments)
    if !exists {
        return nil
    }
    for _,meta := range(atts) {
        key := AttachmentKey(meta["digest"].(string))
        data, err := db.getAttachmentBase64(key)
        if err != nil {
            return err
        }
        meta["data"] = data
    }
    return nil
}

// Retrieves an attachment, base64-encoded, given its key.
func (db *Database) getAttachmentBase64(key AttachmentKey) (string, error) {
    //FIX: Should get attachment in raw form; how do I get a non-JSON document?
	var wrapper map[string]string
	err := db.bucket.Get(attachmentKeyToString(key), &wrapper)
	if err != nil {
		return "", err
	}
    return wrapper["data"], nil
}

// Stores a base64-encoded attachment and returns the key to get it by.
func (db *Database) setAttachmentBase64(encoded string) (AttachmentKey, error) {
    attachment, err := base64.StdEncoding.DecodeString(encoded)
    if err != nil {
        return "", err
    }
	digester := sha1.New()
	digester.Write(attachment)
    digest := base64.StdEncoding.EncodeToString(digester.Sum(nil))
    
	key := AttachmentKey(fmt.Sprintf("sha1-", digest))
    //FIX: Should store raw attachment; how do I set a non-JSON document?
    wrapper := map[string]string {"data": encoded}
	return key, db.bucket.Set(attachmentKeyToString(key), 0, wrapper)
}

//////// HELPERS:

func attachmentKeyToString(key AttachmentKey) string {
	return "att:" + string(key)
}
