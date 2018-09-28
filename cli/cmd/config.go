package cmd

import (
	"github.com/spf13/cobra"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Configure Sync Gateway",
	Long:  ``,
}

func init() {
	rootCmd.AddCommand(configCmd)
}
