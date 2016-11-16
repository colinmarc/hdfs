package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"text/tabwriter"

	"github.com/colinmarc/hdfs"
)

func du(args []string, summarize, humanReadable, withCount bool) {
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
		var dc, fc int
		if info.IsDir() {
			if summarize {
				cs, err := client.GetContentSummary(p)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					status = 1
					continue
				}

				size = cs.Size()
				dc = cs.DirectoryCount()
				fc = cs.FileCount()
			} else {
				size, dc, fc = duDir(client, tw, p, humanReadable, withCount)
			}
		} else {
			size = info.Size()
			dc = 0
			fc = 1
		}

		printSize(tw, size, dc, fc, p, humanReadable, withCount)
	}
}

func duDir(client *hdfs.Client, tw *tabwriter.Writer, dir string, humanReadable, withCount bool) (int64, int, int) {
	dirReader, err := client.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 0, 0, 0
	}

	var partial []os.FileInfo
	var dirSize int64
	var ddc, dfc int
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return dirSize, ddc, dfc
		}

		for _, child := range partial {
			childPath := path.Join(dir, child.Name())
			info, err := client.Stat(childPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 0, 0, 0
			}

			var size int64
			var dc, fc int
			if info.IsDir() {
				size, dc, fc = duDir(client, tw, childPath, humanReadable, withCount)
			} else {
				size = info.Size()
				fc = 1
				dc = 0
			}

			printSize(tw, size, dc, fc, childPath, humanReadable, withCount)
			dirSize += size
			ddc += dc
			dfc += fc
		}
	}

	return dirSize, ddc, dfc
}

func printSize(tw *tabwriter.Writer, size int64, dc, fc int, name string, humanReadable, withCount bool) {
	var c string
	if withCount {
		c = fmt.Sprintf("%d\t%d\t", dc, fc)
	} else {
		c = ""
	}
	if humanReadable {
		formattedSize := formatBytes(size)
		fmt.Fprintf(tw, "%s%s \t%s\n", c, formattedSize, name)
	} else {
		fmt.Fprintf(tw, "%s%d \t%s\n", c, size, name)
	}
}
