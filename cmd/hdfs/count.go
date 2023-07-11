package main

import (
	"fmt"
	"os"
)

const SNAPSHOT_FORMAT = "%18d %24d %24s %28s \n"

func count(args []string, humanReadable bool) {
	if len(args) == 0 {
		fatalWithUsage()
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		fatal(err)
	}

	for _, p := range expanded {
		_, err := client.Stat(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		cs, err := client.GetContentSummary(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		contentSize := formatBytesHuman(uint64(cs.Size()), humanReadable)
		fmt.Printf(SNAPSHOT_FORMAT, cs.DirectoryCount(), cs.FileCount(), contentSize, p)
	}
}
