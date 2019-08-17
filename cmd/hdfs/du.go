package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"text/tabwriter"

	"github.com/colinmarc/hdfs"
)

func du(args []string, summarize, humanReadable bool) {
	if len(args) == 0 {
		printHelp()
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		fatal(err)
	}

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 0, ' ', 0)
	defer tw.Flush()

	for _, p := range expanded {
		info, err := client.Stat(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		var size int64
		if info.IsDir() {
			if summarize {
				cs, err := client.GetContentSummary(p)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					status = 1
					continue
				}

				size = cs.Size()
			} else {
				size = duDir(client, tw, p, humanReadable)
			}
		} else {
			size = info.Size()
		}

		printSize(tw, size, p, humanReadable)
	}
}

func duDir(client *hdfs.Client, tw *tabwriter.Writer, dir string, humanReadable bool) int64 {
	dirReader, err := client.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 0
	}

	var partial []os.FileInfo
	var dirSize int64
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return dirSize
		}

		for _, child := range partial {
			childPath := path.Join(dir, child.Name())
			info, err := client.Stat(childPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 0
			}

			var size int64
			if info.IsDir() {
				size = duDir(client, tw, childPath, humanReadable)
			} else {
				size = info.Size()
			}

			printSize(tw, size, childPath, humanReadable)
			dirSize += size
		}
	}

	return dirSize
}

func printSize(tw *tabwriter.Writer, size int64, name string, humanReadable bool) {
	if humanReadable {
		formattedSize := formatBytes(uint64(size))
		fmt.Fprintf(tw, "%s \t%s\n", formattedSize, name)
	} else {
		fmt.Fprintf(tw, "%d \t%s\n", size, name)
	}
}
