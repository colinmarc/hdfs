package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

var knownCommands = []string{
	"ls",
	"rm",
	"mv",
	"mkdir",
	"touch",
	"chmod",
	"chown",
	"cat",
	"head",
	"tail",
	"du",
	"checksum",
	"get",
	"getmerge",
}

func complete(args []string) {
	if len(args) == 2 {
		words := strings.Split(args[1], " ")[1:]

		if len(words) <= 1 {
			fmt.Println(strings.Join(knownCommands, " "))
		} else {
			completePath(words[len(words)-1])
		}
	} else {
		fmt.Println(strings.Join(knownCommands, " "))
	}
}

func completePath(fragment string) {
	paths, namenode, err := normalizePaths([]string{fragment})
	if err != nil {
		return
	}

	fullPath := paths[0]
	if hasGlob(fullPath) {
		return
	}

	client, err := getClient(namenode)
	if err != nil {
		return
	}

	var dir, prefix string
	if strings.HasSuffix(fragment, "/") {
		dir = fullPath
		prefix = ""
	} else {
		dir, prefix = path.Split(fullPath)
	}

	dirReader, err := client.Open(dir)
	if err != nil {
		return
	}

	// 1000 entries should align with what HDFS returns. If not, well, it was
	// going to be slow anyway.
	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(1000) {
		if err != nil {
			return
		}

		for _, fi := range partial {
			name := fi.Name()

			if strings.HasPrefix(name, prefix) {
				p := path.Join(dir, name)
				if fi.IsDir() {
					p += "/"
				}

				fmt.Print(" " + p)
			}
		}
	}

	fmt.Println()
}
