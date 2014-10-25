package main

import (
	"os"
	"time"
)

func touch(paths []string, noCreate bool) int {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		printHelp()
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	for _, p := range paths {
		if hasGlob(p) {
			fatal(fileError(p, os.ErrNotExist))
		}

		_, err := client.Stat(p)
		exists := (err != os.ErrNotExist)
		if (err != nil && exists) || (!exists && noCreate) {
			fatal(fileError(p, err))
		}

		if exists {
			now := time.Now()
			mtime := now
			atime := now

			err = client.Chtimes(p, mtime, atime)
		} else {
			err = client.CreateEmptyFile(p)
		}

		if err != nil {
			fatal(fileError(p, err))
		}
	}

	return 0
}
