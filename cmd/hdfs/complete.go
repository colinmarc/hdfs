package main

import (
	"path"
	"strings"
)

var knownCommands = []string{"ls", "rm", "mv"}

func complete(args []string) []string {
	if len(args) <= 1 {
		return knownCommands
	} else {
		return completePath(args[len(args)-1])
	}
}

func completePath(fragment string) []string {
	paths, namenode, err := normalizePaths([]string{fragment})
	if err != nil {
		return nil
	}

	fullPath := paths[0]
	if hasGlob(fullPath) {
		return nil
	}

	client, err := getClient(namenode)
	if err != nil {
		return nil
	}

	var dir, prefix string
	if strings.HasSuffix(fragment, "/") {
		dir = fullPath
		prefix = ""
	} else {
		dir, prefix = path.Split(fullPath)
	}

	res, err := client.ReadDir(dir)
	if err != nil {
		return nil
	}

	matches := make([]string, len(res))
	for _, fi := range res {
		name := fi.Name()

		if strings.HasPrefix(name, prefix) {
			p := path.Join(dir, name)
			if fi.IsDir() {
				p += "/"
			}

			matches = append(matches, p)
		}
	}

	return matches
}
