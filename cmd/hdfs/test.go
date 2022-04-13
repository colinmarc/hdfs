package main

import (
	"errors"
	"fmt"
	"os"
)

type testFn func(fi os.FileInfo) bool

func test(args []string, exists, file, dir, empty, nonempty bool) {
	if len(args) == 0 {
		fatalWithUsage()
	}

	numFlags := 0
	for _, b := range []bool{exists, dir, file, nonempty, empty} {
		if b {
			numFlags += 1
		}
	}

	if numFlags != 1 {
		fatal("exactly one test flag must be specified")
	}

	var f func(fi os.FileInfo) bool
	switch {
	case exists:
		f = func(fi os.FileInfo) bool { return fi != nil }
	case dir:
		f = func(fi os.FileInfo) bool { return fi.IsDir() }
	case file:
		f = func(fi os.FileInfo) bool { return !fi.IsDir() }
	case nonempty:
		f = func(fi os.FileInfo) bool { return fi.Size() != 0 }
	case empty:
		f = func(fi os.FileInfo) bool { return fi.Size() == 0 }
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		fatal(err)
	}

	for _, p := range expanded {
		fi, err := client.Stat(p)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		if !f(fi) {
			status = 1
		}
	}
}
