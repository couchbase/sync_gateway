package cmd

import "github.com/spf13/cobra"

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Sync Gateway status",
	Long:  ``,
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

