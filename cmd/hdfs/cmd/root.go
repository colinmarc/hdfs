package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gohdfs",
	Short: "GoHDFS is a very fast client for HDFS clusters",
	RunE:  rootRun,
}

func rootRun(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}

func Execute(version string) {
	if version == "" {
		version = "local-" + time.Now().String()
	}
	rootCmd.Version = version
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
