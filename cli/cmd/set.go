package cmd

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
)

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

		metakvHelper := rest.NewMetaKVClient(bootstrapConfig)
		key := args[0]
		if err := metakvHelper.Upsert(key, val); err != nil {
			panic(fmt.Sprintf("Error setting config.  Key: %v Error: %v Val: %v", key, err, val))
		}

		fmt.Printf("Successfully set key: %v.  Value size: %d bytes\n", key, len(val))

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
