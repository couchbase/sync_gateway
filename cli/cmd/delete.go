package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <metakv-key>",
	Short: "Delete a key in metakv",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		bootstrapConfig, err := BootstrapConfigFromParams()
		if err != nil {
			panic(fmt.Sprintf("Error getting bootstrap config: %v", err))
		}

		metakvHelper := rest.NewMetaKVClient(bootstrapConfig)
		key := args[0]

		if RecursiveDelete {
			err = metakvHelper.RecursiveDelete(key)
			if err != nil {
				panic(fmt.Sprintf("Error recursively deleting key: %+v.  Err: %v", key, err))
			}
			fmt.Printf("Recursively deleted key %v\n", key)
		} else {
			err = metakvHelper.Delete(key)
			if err != nil {
				panic(fmt.Sprintf("Error deleting key: %v.  Err: %+v", key, err))
			}
			fmt.Printf("Deleted key %v\n", key)
		}

	},
}

func init() {

	metakvCmd.AddCommand(deleteCmd)

	rootCmd.Flags().BoolVar(&RecursiveDelete, "--recursive-delete", false, "Perform a recursive delete")

}
