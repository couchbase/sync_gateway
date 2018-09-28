package cmd

import (
	"fmt"

	"github.com/couchbase/sync_gateway/rest"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list <metakv-key>",
	Short: "List all child keys recursively of a given metakv",
	Long:  `The metakv key must end in /`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		bootstrapConfig, err := BootstrapConfigFromParams()
		if err != nil {
			panic(fmt.Sprintf("Error getting bootstrap config: %v", err))
		}

		metakvHelper := rest.NewMetaKVClient(bootstrapConfig)
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
}
