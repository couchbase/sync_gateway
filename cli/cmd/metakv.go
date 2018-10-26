package cmd

import (
	"github.com/spf13/cobra"
)

// metakvCmd represents the metakv command
var metakvCmd = &cobra.Command{
	Use:   "metakv",
	Short: "Directly manipulate configuration data in metakv",
	Long:  ``,
}

func init() {
	configCmd.AddCommand(metakvCmd)
}
