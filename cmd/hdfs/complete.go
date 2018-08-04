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
	"put",
	"df",
}

func complete(args []string) {
	if len(args) == 2 {
		words := strings.Split(args[1], " ")[1:]

		if len(words) <= 1 {
			fmt.Println(strings.Join(knownCommands, " "))
		} else if isKnownCommand(words[0]) {
			position := countPosition(words)
			completeArg(words[0], words[len(words)-1], position)
		}
	} else {
		fmt.Println(strings.Join(knownCommands, " "))
	}
}

func completeArg(command, fragment string, position int) {
	if (command == "put" && position == 1) ||
		((command == "get" || command == "getmerge") && position == 2) {
		fmt.Println("_FILE_") // The bash_completion bit knows about this special string.
	} else if (command == "chmod" || command == "chown") && position == 1 {
		return
	} else if !strings.HasPrefix(fragment, "-") {
		completePath(fragment)
	}
}

func completePath(fragment string) {
	paths, namenode, err := normalizePaths([]string{fragment})
	if err != nil {
		return
	}

	client, err := getClient(namenode)
	if err != nil {
		return
	}

	fullPath := paths[0]
	if fullPath == "" {
		fullPath = userDir(client) + "/"
	} else if hasGlob(fullPath) {
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

func isKnownCommand(command string) bool {
	for _, c := range knownCommands {
		if command == c {
			return true
		}
	}
	return false
}

func countPosition(words []string) int {
	var position int
	for _, w := range words {
		if !strings.HasPrefix(w, "-") {
			position++
		}
	}

	return position - 1
}
