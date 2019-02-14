package base

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	// This is intentionally brittle (hardcoded redaction tags)
	// We'd probably want to know if this got changed by accident...
	systemDataPrefix = "<sd>"
	systemDataSuffix = "</sd>"
)

func TestSystemDataRedact(t *testing.T) {
	clusterName := "My Super Secret IP"
	systemdata := SystemData(clusterName)

	RedactSystemData = true
	assert.Equal(t, systemDataPrefix+clusterName+systemDataSuffix, systemdata.Redact())

	RedactSystemData = false
	assert.Equal(t, clusterName, systemdata.Redact())
}

func TestSD(t *testing.T) {
	RedactSystemData = true
	defer func() { RedactSystemData = false }()

	//Base string test
	sd := SD("hello world")
	assert.Equal(t, systemDataPrefix+"hello world"+systemDataSuffix, sd.Redact())

	//Big Int
	sd = SD(big.NewInt(1234))
	assert.Equal(t, systemDataPrefix+"1234"+systemDataSuffix, sd.Redact())

	//Struct
	sd = SD(struct{}{})
	assert.Equal(t, systemDataPrefix+"{}"+systemDataSuffix, sd.Redact())

	//String slict
	sd = SD([]string{"hello", "world", "o/"})
	assert.Equal(t, "[ "+systemDataPrefix+"hello"+systemDataSuffix+" "+systemDataPrefix+"world"+systemDataSuffix+" "+systemDataPrefix+"o/"+systemDataSuffix+" ]", sd.Redact())
}
