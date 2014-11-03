package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
	"io"
	"net/url"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"
)

var (
	multipleNamenodeUrls = errors.New("Multiple namenode URLs specified")
	rootPath             = userDir()
)

func userDir() string {
	currentUser, err := user.Current()
	if err != nil || currentUser.Username == "" {
		return "/"
	} else {
		return path.Join("/user", currentUser.Username)
	}
}

// normalizePaths parses the hosts out of HDFS URLs, and turns relative paths
// into absolute ones (by appending /user/<user>). If multiple HDFS urls with
// differing hosts are passed in, it returns an error.
func normalizePaths(paths []string) ([]string, string, error) {
	namenode := ""
	cleanPaths := make([]string, 0, len(paths))

	for _, rawurl := range paths {
		url, err := url.Parse(rawurl)
		if err != nil {
			return nil, "", err
		}

		if url.Host != "" {
			if namenode != "" && namenode != url.Host {
				return nil, "", multipleNamenodeUrls
			} else {
				namenode = url.Host
			}
		}

		p := path.Clean(url.Path)
		if !path.IsAbs(url.Path) {
			p = path.Join(rootPath, p)
		}

		cleanPaths = append(cleanPaths, p)
	}

	return cleanPaths, namenode, nil
}

func getClientAndExpandedPaths(paths []string) ([]string, *hdfs.Client, error) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		return nil, nil, err
	}

	client, err := getClient(nn)
	if err != nil {
		return nil, nil, err
	}

	expanded, err := expandPaths(client, paths)
	if err != nil {
		return nil, nil, err
	}

	return expanded, client, nil
}

// TODO: not really sure checking for a leading \ is the way to test for
// escapedness.
func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
	return match
}

// expandGlobs recursively expands globs in a filepath. It assumes the paths
// are already cleaned and normalize (ie, absolute).
func expandGlobs(client *hdfs.Client, globbedPath string) ([]string, error) {
	if !hasGlob(globbedPath) {
		return []string{globbedPath}, nil
	}

	parts := strings.Split(globbedPath, "/")[1:]
	res := make([]string, 0)
	splitAt := 0
	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	base := "/" + path.Join(parts[:splitAt]...)
	glob := parts[splitAt]
	remainder := path.Join(parts[splitAt+1:]...)
	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		newPath := path.Join(base, fi.Name(), remainder)
		children, err := expandGlobs(client, newPath)
		if err != nil {
			return nil, err
		}

		res = append(res, children...)
	}

	return res, nil
}

func expandPaths(client *hdfs.Client, paths []string) ([]string, error) {
	res := make([]string, 0)

	for _, p := range paths {
		expanded, err := expandGlobs(client, p)
		if err != nil {
			return nil, err
		}

		res = append(res, expanded...)
	}

	return res, nil
}

type walkFunc func(string, os.FileInfo)

func walk(client *hdfs.Client, root string, visit walkFunc) error {
	rootInfo, err := client.Stat(root)
	if err != nil {
		return err
	}

	visit(root, rootInfo)
	if rootInfo.IsDir() {
		err = walkDir(client, root, visit)
		if err != nil {
			return err
		}
	}

	return nil
}

func walkDir(client *hdfs.Client, dir string, visit walkFunc) error {
	dirReader, err := client.Open(dir)
	if err != nil {
		return err
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			return err
		}

		for _, child := range partial {
			childPath := path.Join(dir, child.Name())
			visit(childPath, child)

			if child.IsDir() {
				err = walkDir(client, childPath, visit)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
