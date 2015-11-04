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

func (runner *SyncRunner) validateDocument(document otto.Value, url otto.Value, schemata map[string]SchemaWrapper) (bool, string) {
	docString, err := document.ToString()
	if err != nil{
		otto.FalseValue()
	}
	urlString, err := url.ToString()
	if err != nil{
		otto.FalseValue()
	}
	return validate(schemata, docString, urlString)

}

func validate(schemata map[string]SchemaWrapper, doc_string string, url string) (bool, string){

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

	documentLoader := gojsonschema.NewStringLoader(doc_string)
    result, err := sch.Validate(documentLoader)

    if err != nil {
        return false, "Validation Error - "
    }

    if result.Valid() {
        return true, "ok"
    } else {
        return false, "Document failed validation"
    }

}
