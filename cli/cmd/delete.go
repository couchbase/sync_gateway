package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete <metakv-key>",
	Short: "Delete a key in metakv",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		metakvHelper := sync_gateway.NewMetaKVClient()
		key := args[0]
		err := metakvHelper.Delete(key)
		if err != nil {
			panic(fmt.Sprintf("Error deleting key: %v.  Err: %v", key, err))
		}
		fmt.Printf("Deleted key %v\n", key)
	},
}

func init() {
	metakvCmd.AddCommand(deleteCmd)

}
