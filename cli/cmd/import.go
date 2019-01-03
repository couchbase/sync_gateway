package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	msgrpc "github.com/couchbase/mobile-service/mobile_service_grpc"
	"github.com/spf13/cobra"
)

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import the SG config into JSON",
	Long:  `The value will be read from STDIN unless the --input-file-path flag is specified.

Example:

cat config.json | sg config import 

Example:

sg config import --input-file-path file.json

Example:

echo "{\"MyKey\": 2}" | sg config import`,
	Run: func(cmd *cobra.Command, args []string) {

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

		bootstrapConfig, err := BootstrapConfigFromParams()
		if err != nil {
			panic(fmt.Sprintf("Error getting bootstrap config: %v", err))
		}

		// Set up a connection to the server.
		adminClient, err := bootstrapConfig.NewMobileServiceAdminClient()
		if err != nil {
			panic(fmt.Sprintf("Error connecting via grpc to mobile service: %v", err))
		}

		configJson := &msgrpc.ConfigJson{}
		configJson.Body = string(val)

		_, err = adminClient.ImportConfig(context.Background(), configJson)
		if err != nil {
			panic(fmt.Sprintf("Error importing Sync Gateway config: %v", err))
		}

		fmt.Printf("Sync Gateway configuration has been successfully imported")

	},
}

func init() {
	configCmd.AddCommand(importCmd)

	importCmd.Flags().StringVarP(
		&inputFilePath,
		"input-file-path",
		"i",
		"Specify a path to an input file rather than using STDIN",
		"-i file.json",
	)

}
