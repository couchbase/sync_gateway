package cmd

import (
	"fmt"
	"os"

	"github.com/couchbase/sync_gateway"
	"github.com/spf13/cobra"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "sg",
	Short: "Sync Gateway -- Couchbase Mobile",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// Legacy mode
		pathToConfigFile := args[0]
		sync_gateway.RunGatewayLegacyMode(pathToConfigFile)

	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	rootCmd.PersistentFlags().StringVarP(&GoCBConnstr, "connstr", "c", "http://localhost:9000,localhost:9001", "The Couchbase server(s) to connect to with an http:// or couchbase:// URI")

	rootCmd.PersistentFlags().StringVarP(&CBUsername, "username", "u", "username", "The Couchbase username to connect as")

	rootCmd.PersistentFlags().StringVarP(&CBPassword, "password", "p", "xxxxxxxx", "The password for the given username")

}
