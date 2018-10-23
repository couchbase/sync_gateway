package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway"
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

		metakvHelper := sync_gateway.NewMetaKVClient(bootstrapConfig)
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

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// getCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
