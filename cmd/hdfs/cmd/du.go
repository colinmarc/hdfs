package cmd

import (
	"fmt"
	"io"
	"os"
	"path"
	"text/tabwriter"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(duCmd)
	duCmd.PersistentFlags().BoolVarP(&duSummarizeOpt, "summarize", "s", false, "display only a total for each argument")
	duCmd.PersistentFlags().BoolVarP(&duHumanReadableOpt, "human-readable", "h", false, "print human-readable sizes")
	duCmd.Flags().BoolVarP(&duHelpOpt, "help", "", false, "display help")
}

var (
	duSummarizeOpt     bool
	duHumanReadableOpt bool
	duHelpOpt          bool
)

var duCmd = &cobra.Command{
	Use:   "du [-sh] FILE...",
	Short: "estimate HDFS space usage",
	RunE:  duRun,
	DisableFlagsInUseLine: true,
}

func duRun(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return cmd.Help()
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 0, ' ', 0)
	defer tw.Flush()

	for _, p := range expanded {
		info, err := client.Stat(p)
		if err != nil {
			return err
		}

		var size int64
		if info.IsDir() {
			if duSummarizeOpt {
				cs, err := client.GetContentSummary(p)
				if err != nil {
					return err
				}

				size = cs.Size()
			} else {
				size, err = duDir(client, tw, p, duHumanReadableOpt)
				if err != nil {
					return err
				}
			}
		} else {
			size = info.Size()
		}

		printSize(tw, size, p, duHumanReadableOpt)
	}
	return nil
}

func duDir(client *hdfs.Client, tw *tabwriter.Writer, dir string, humanReadable bool) (int64, error) {
	dirReader, err := client.Open(dir)
	if err != nil {
		return 0, err
	}

	var partial []os.FileInfo
	var dirSize int64
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			return 0, err
		}

		for _, child := range partial {
			childPath := path.Join(dir, child.Name())
			info, err := client.Stat(childPath)
			if err != nil {
				return 0, err
			}

			var size int64
			if info.IsDir() {
				size, err = duDir(client, tw, childPath, humanReadable)
				if err != nil {
					return 0, err
				}
			} else {
				size = info.Size()
			}

			printSize(tw, size, childPath, humanReadable)
			dirSize += size
		}
	}

	return dirSize, nil
}

func printSize(tw *tabwriter.Writer, size int64, name string, humanReadable bool) {
	if humanReadable {
		formattedSize := formatBytes(uint64(size))
		fmt.Fprintf(tw, "%s \t%s\n", formattedSize, name)
	} else {
		fmt.Fprintf(tw, "%d \t%s\n", size, name)
	}
}
