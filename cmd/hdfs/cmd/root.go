package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "hdfs",
	Short: "GoHDFS is a very fast client for HDFS clusters",
	RunE:  rootRun,
}

func rootRun(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
