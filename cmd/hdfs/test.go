package main

import (
	"fmt"
	"os"
)

type testCommandFlags struct {
	exists     bool
	isDir      bool
	isFile     bool
	isNonEmpty bool
	isEmpty    bool
}

func test(args []string, flags testCommandFlags) {
	if len(args) == 0 {
		fatalWithUsage()
	}

	expanded, client, err := getClientAndExpandedPaths(args)
	if err != nil {
		fatal(err)
	}

	for _, p := range expanded {
		fi, err := client.Stat(p)
		if err != nil {
			//fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		if !doTest(fi, flags) {
			status = 1
			continue
		}
	}
}

func doTest(fi os.FileInfo, flags testCommandFlags) bool {
	var result bool
	var flags_count int

	if flags.exists {
		result = true
		flags_count++
	}
	if flags.isDir {
		result = fi.IsDir()
		flags_count++
	}
	if flags.isFile {
		result = !fi.IsDir()
		flags_count++
	}
	if flags.isNonEmpty {
		result = fi.Size() > 0
		flags_count++
	}
	if flags.isEmpty {
		result = fi.Size() == 0
		flags_count++
	}

	if flags_count != 1 {
		fmt.Fprintln(os.Stderr, "Only one test flag is allowed")
		return false
	}

	return result
}
