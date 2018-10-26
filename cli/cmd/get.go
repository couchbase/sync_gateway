package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <metakv-key>",
	Short: "Get the value of the given metakv key",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		bootstrapConfig, err := BootstrapConfigFromParams()
		if err != nil {
			panic(fmt.Sprintf("Error getting bootstrap config: %v", err))
		}

		metakvHelper := rest.NewMetaKVClient(bootstrapConfig)
		key := args[0]
		value, err := metakvHelper.Get(key)

		if err != nil {
			panic(fmt.Sprintf("Error getting metak keys.  Key: %v Error: %v", key, err))
		}
		fmt.Printf("%v\n", string(value))

	},
}

func init() {
	metakvCmd.AddCommand(getCmd)
}
