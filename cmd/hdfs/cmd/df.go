package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/colinmarc/hdfs"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(dfCmd)
	dfCmd.PersistentFlags().BoolVarP(&dfHumanReadableOpt, "human-readable", "h", false, "print human-readable sizes")
	dfCmd.Flags().BoolVarP(&dfHelpOpt, "help", "", false, "display help")
}

var (
	dfHumanReadableOpt bool
	dfHelpOpt          bool
)

var dfCmd = &cobra.Command{
	Use:   "df [-h]",
	Short: "concatenate HDFS files and print on the standard output",
	RunE:  dfRun,
	DisableFlagsInUseLine: true,
}

func dfRun(cmd *cobra.Command, args []string) error {
	if dfHelpOpt {
		return cmd.Help()
	}
	client, err := getClient("")
	if err != nil {
		return err
	}

	var fs hdfs.FsInfo

	fs, err = client.StatFs()
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "Filesystem \tSize \tUsed \tAvailable \t Use%%\n")
	if dfHumanReadableOpt {
		fmt.Fprintf(tw, "%v \t%v \t%v \t%v \t%d%%\n",
			os.Getenv("HADOOP_NAMENODE"),
			formatBytes(fs.Capacity),
			formatBytes(fs.Used),
			formatBytes(fs.Remaining),
			100*fs.Used/fs.Capacity)
	} else {
		fmt.Fprintf(tw, "%v \t%v \t %v \t %v \t%d%%\n",
			os.Getenv("HADOOP_NAMENODE"),
			fs.Capacity,
			fs.Used,
			fs.Remaining,
			100*fs.Used/fs.Capacity)
	}
	tw.Flush()
	return nil
}
