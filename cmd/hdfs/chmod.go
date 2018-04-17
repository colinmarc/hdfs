package main

import (
	"fmt"
	"os"
	"strconv"
)

func chmod(args []string, recursive bool) {
	if len(args) < 2 {
		printHelp()
	}

	mode, err := strconv.ParseUint(args[0], 8, 32)
	if err != nil {
		fatal("invalid octal mode:", args[0])
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		fatal(err)
	}

	visit := func(p string, fi os.FileInfo, err error) error {
		err = client.Chmod(p, os.FileMode(mode))

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
			return err
		}
		return nil
	}

	for _, p := range expanded {
		if recursive {
			err = client.Walk(p, visit)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				status = 1
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
				fatal(err)
			}

			visit(p, info, nil)
		}
	}
}
