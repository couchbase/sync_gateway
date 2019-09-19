package base

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TransformBucketCredentials(inputUsername, inputPassword, inputBucketname string) (username, password, bucketname string) {

func TestTransformBucketCredentials(t *testing.T) {

	inputUsername := "foo"
	inputPassword := "bar"
	inputBucketName := "baz"

	username, password, bucketname := TransformBucketCredentials(
		inputUsername,
		inputPassword,
		inputBucketName,
	)
	assert.Equal(t, username, inputUsername)
	assert.Equal(t, password, inputPassword)
	assert.Equal(t, bucketname, inputBucketName)

	inputUsername2 := ""
	inputPassword2 := "bar"
	inputBucketName2 := "baz"

	username2, password2, bucketname2 := TransformBucketCredentials(
		inputUsername2,
		inputPassword2,
		inputBucketName2,
	)

	assert.Equal(t, username2, inputBucketName2)
	assert.Equal(t, password2, inputPassword2)
	assert.Equal(t, bucketname2, inputBucketName2)

}

func TestDCPKeyFilter(t *testing.T) {

	assert.True(t, dcpKeyFilter([]byte("doc123")))
	assert.True(t, dcpKeyFilter([]byte(UserPrefix+"user1")))
	assert.True(t, dcpKeyFilter([]byte(RolePrefix+"role2")))
	assert.True(t, dcpKeyFilter([]byte(UnusedSeqPrefix+"1234")))

	assert.False(t, dcpKeyFilter([]byte(SyncSeqKey)))
	assert.False(t, dcpKeyFilter([]byte(SyncPrefix+"unusualSeq")))
	assert.False(t, dcpKeyFilter([]byte(SyncDataKey)))
	assert.False(t, dcpKeyFilter([]byte(DCPCheckpointPrefix+"12")))
}

// Compare Atoi vs map lookup for partition conversion
//    BenchmarkPartitionToVbNo/map-16         	100000000	        10.4 ns/op
//    BenchmarkPartitionToVbNo/atoi-16        	500000000	         3.85 ns/op
//    BenchmarkPartitionToVbNo/parseUint-16   	300000000	         5.04 ns/op
func BenchmarkPartitionToVbNo(b *testing.B) {

	//Initialize lookup map
	vbNos := make(map[string]uint16, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
		vbNos[vbucketIdStrings[i]] = uint16(i)
	}

	// We'd expect a minor performance hit when redaction is enabled.
	b.Run("map", func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			value := uint16(vbNos["23"])
			if value != uint16(23) {
				b.Fail()
			}
		}
	})

	b.Run("atoi", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			valueInt, err := strconv.Atoi("23")
			value := uint16(valueInt)
			if err != nil || value != uint16(23) {
				b.Fail()
			}
		}
	})

	b.Run("parseUint", func(bn *testing.B) {
		RedactUserData = true
		for i := 0; i < bn.N; i++ {
			valueUint64, err := strconv.ParseUint("23", 10, 0)
			value := uint16(valueUint64)
			if err != nil || value != uint16(23) {
				b.Fail()
			}
		}
	})

}
