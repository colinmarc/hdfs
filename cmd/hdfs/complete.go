package main

import (
	"path"
	"os"
	"strings"
)

func complete(args []string) []string {
	if len(args) == 0 {
		return knownCommands
	} else if len(args) == 1 {
		for _, cmd := range knownCommands {
			if args[0] == cmd {
				return completePath("")
			}
		}

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

	fragment = paths[0]
	if hasGlob(fragment) {
		return nil
	}

	client, err := getClient(namenode)
	if err != nil {
		return nil
	}

	var dir, prefix string
	info, err := client.Stat(fragment)
	if err != nil && err != os.ErrNotExist {
		return nil
	} else if err == nil {
		if info.IsDir() {
			dir = fragment
			prefix = ""
		} else {
			dir = rootPath
			prefix = ""
		}
	} else {
		dir, prefix = path.Split(fragment)
	}

	res, err := client.ReadDir(dir)
	if err != nil {
		return nil
	}

	matches := make([]string, len(res))
	for _, fi := range res {
		name := fi.Name()

		if strings.HasPrefix(name, prefix) {
			fullPath := path.Join(dir, name)
			if fi.IsDir() {
				fullPath += "/"
			}

			matches = append(matches, fullPath)
		}
	}

	return matches
}
