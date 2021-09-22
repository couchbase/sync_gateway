package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackgroundStatus(t *testing.T) {
	resync := ResyncBackgroundManager{}
	status, err := resync.GetJSONStatus()
	assert.NoError(t, err)
	fmt.Println(string(status))

	resync.Stop()

}
