package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "gohdfs",
	Short:        "GoHDFS is a very fast client for HDFS clusters",
	RunE:         rootRun,
	SilenceUsage: true,
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
		os.Exit(1)
	}
}
