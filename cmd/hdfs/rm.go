package main

import (
	"fmt"
	"os"
)

func rm(paths []string, recursive bool) int {
	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	status := 0
	for _, p := range expanded {
		info, err := stat(client, p)
		if err == os.ErrNotExist {
			fmt.Fprintf(os.Stderr, "%s: no such file or directory\n", p)
			status = 1
			continue
		} else if err != nil {
			fatal(err)
		}

		if !recursive && info.IsDir() {
			fmt.Fprintf(os.Stderr, "%s: is a directory\n", p)
			status = 1
			continue
		}

		err = client.Remove(p)
		if err != nil {
			fatal(err)
		}
	}

	return status
}
