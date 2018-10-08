package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list <metakv-key>",
	Short: "List all child keys recursively of a given metakv",
	Long:  `The metakv key must end in /`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		metakvHelper := sync_gateway.NewMetaKVClient()
		key := args[0]
		metakvPairs, err := metakvHelper.ListAllChildren(key)

		if err != nil {
			panic(fmt.Sprintf("Error listing metakv keys.  Key: %v Error: %v", key, err))
		}
		for _, metakvPair := range metakvPairs {
			fmt.Printf("%v\n", metakvPair.Path)
		}

	},
}

func init() {
	metakvCmd.AddCommand(listCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// listCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
