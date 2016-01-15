package channels

import (
	"os"
	"strings"
    "github.com/xeipuuv/gojsonschema"
	"github.com/robertkrimen/otto"
)

type SchemaWrapper struct {
	schema            *gojsonschema.Schema
	exists            bool
}

func (runner *SyncRunner) validateDocument(documentValue otto.Value, urlValue otto.Value, schemata map[string]SchemaWrapper) (bool, string) {
	urlString, err := urlValue.ToString()
	if err != nil{
		return false, "Validation Error - failed to convert schema url"
	}
	document, err := documentValue.Export()
	if err != nil{
		return false, "Validation Error - failed to convert document"
	}
	return validate(document, urlString, schemata)

}

func validate(document interface{}, url string, schemata map[string]SchemaWrapper) (bool, string){


    schemaWrapper := schemata[url]

	var sch *gojsonschema.Schema
	var err error
	var mungedUrl string

    if schemaWrapper.exists{
        sch = schemaWrapper.schema
    }else{

		if strings.HasPrefix(url, "http"){
			mungedUrl = url
		} else {
			path, _ := os.Getwd()
			mungedUrl = "file://" + path + "/" + url
		}

        schemaLoader := gojsonschema.NewReferenceLoader(mungedUrl)
        sch, err = gojsonschema.NewSchema(schemaLoader)

        if err != nil {
            return false, "Validation Error - Failed to load schema for url: " + url
        } else {
			sw := SchemaWrapper{sch, true}
            schemata[url] = sw
        }
	}

	documentLoader := gojsonschema.NewGoLoader(document)
    result, err := sch.Validate(documentLoader)

    if err != nil {
        return false, "Validation Error - Error during validation"
    }

    if result.Valid() {
        return true, "ok"
    } else {
        return false, "Document in invalid"
    }

}
