package cmd

import (
	"fmt"

	"bufio"
	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"log"
)

var (
	inputFilePath string
)

// setCmd represents the set command
var setCmd = &cobra.Command{
	Use:   "set <metakv-key> STDIN",
	Short: "Set a value in metakv",
	Long: `The metakv-key argument should not contain any spaces.  The value
will be read from STDIN unless the --input-file-path flag is specified.

Example:

cat config.json | sg config metakv set /path/to/my/key 

Example:

sg config metakv set /path/to/my/key --input-file-path file.json

Example:

echo "{\"MyKey\": 2}" | sg config metakv set /path/to/my/key

`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("set called with args: %+v", args)

		var val []byte
		var err error

		if inputFilePath != "" {
			val, err = ioutil.ReadFile(inputFilePath)

		} else {
			reader := bufio.NewReader(os.Stdin)
			val, err = ioutil.ReadAll(reader)
		}

		if err != nil {
			panic(fmt.Sprintf("Error reading from stdin: %v", err))
		}

		metakvHelper := sync_gateway.NewMetaKVClient()
		key := args[0]
		if err := metakvHelper.Upsert(key, val); err != nil {
			panic(fmt.Sprintf("Error setting config.  Key: %v Error: %v Val: %v", key, err, val))
		}

		log.Printf("Successfully set key: %v.  Value size: %d bytes", key, len(val))

	},
}

func init() {

	metakvCmd.AddCommand(setCmd)

	setCmd.Flags().StringVarP(
		&inputFilePath,
		"input-file-path",
		"i",
		"Specify a path to an input file rather than using STDIN",
		"-i file.json",
	)

}

//func UpsertKey(key, val []byte) {
//
//	assetJson, err := Asset(assetPath)
//	if err != nil {
//		panic(fmt.Sprintf("Asset not found"))
//	}
//
//	log.Printf("assetJson: %s", string(assetJson))
//
//	serverConfigKey, _  := a.gateway.GrpcClient.MetaKVGet(a.context, &mobile_service.MetaKVPath{
//		Path: key,
//	})
//
//	if serverConfigKey.Rev == "" {
//		_, err = a.gateway.GrpcClient.MetaKVAdd(a.context, &mobile_service.MetaKVPair{
//			Path:  key,
//			Value: assetJson,
//		})
//
//	} else {
//		_, err = a.gateway.GrpcClient.MetaKVSet(a.context, &mobile_service.MetaKVPair{
//			Path:  key,
//			Rev: serverConfigKey.Rev,
//			Value: assetJson,
//		})
//	}
//
//	if err != nil {
//		log.Printf("Error adding/setting key: %v. Err: %v", key, err)
//	}
//}
