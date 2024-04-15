package db

import sgbucket "github.com/couchbase/sg-bucket"

// mouXattrName is the name of the xattr that stores metadata only updates.
const mouXattrName = "_mou"

// Mou represents a metadata only update.
type Mou struct {
	Cas  uint64 `json:"cas"`  // cas value associated with latest metadata update
	pSeq uint64 `json:"pSeq"` // revSeqNo of the document associated with the previous non-metadata-only update
}

// mouExpandSpec returns the macro expansion spec for the _mou xattr
func mouExpandSpec() sgbucket.MacroExpansionSpec {
	return sgbucket.NewMacroExpansionSpec(mouXattrName+".cas", sgbucket.MacroCas)
}
