
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

// metakvCmd represents the metakv command
var metakvCmd = &cobra.Command{
	Use:   "metakv",
	Short: "Directly manipulate configuration data in metakv",
	Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("metakv called")
	},
}

func init() {
	configCmd.AddCommand(metakvCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// metakvCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// metakvCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
