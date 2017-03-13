package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/colinmarc/hdfs"
)

func df(humanReadable bool) {
	client, err := getClient("")
	if err != nil {
		fatal(err)
	}

	var fs hdfs.FsInfo

	fs, err = client.StatFs()
	if err != nil {
		fatal(err)
	}

	tw := tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "Filesystem \tSize \tUsed \tAvailable \t Use%%\n")
	if humanReadable {
		fmt.Fprintf(tw, "%v \t%v \t%v \t%v \t%d%%\n",
			os.Getenv("HADOOP_NAMENODE"),
			formatBytes(fs.Capacity),
			formatBytes(fs.Used),
			formatBytes(fs.Remaining),
			100 * fs.Used / fs.Capacity)
	} else {
		fmt.Fprintf(tw, "%v \t%v \t %v \t %v \t%d%%\n",
			os.Getenv("HADOOP_NAMENODE"),
			fs.Capacity,
			fs.Used,
			fs.Remaining,
			100 * fs.Used / fs.Capacity)
	}
	tw.Flush()
}
