package main

import (
	"errors"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/colinmarc/hdfs"
)

var (
	errMultipleNamenodeUrls = errors.New("Multiple namenode URLs specified")
	rootPath                = userDir()
)

func userDir() string {
	currentUser, err := hdfs.Username()
	if err != nil || currentUser == "" {
		return "/"
	}

	return path.Join("/user", currentUser)
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
				return nil, "", errMultipleNamenodeUrls
			}

			namenode = url.Host
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
	parts := strings.Split(globbedPath, "/")[1:]
	var res []string
	var splitAt int

	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	var base, glob, next, remainder string
	base = "/" + path.Join(parts[:splitAt]...)
	glob = parts[splitAt]

	if len(parts) > splitAt+1 {
		next = parts[splitAt+1]
		remainder = path.Join(parts[splitAt+2:]...)
	} else {
		next = ""
		remainder = ""
	}

	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		if !hasGlob(next) {
			_, err := client.Stat(path.Join(base, fi.Name(), next))
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			} else if os.IsNotExist(err) {
				continue
			}
		}

		newPath := path.Join(base, fi.Name(), next, remainder)
		if hasGlob(newPath) {
			children, err := expandGlobs(client, newPath)
			if err != nil {
				return nil, err
			}

			res = append(res, children...)
		} else {
			res = append(res, newPath)
		}
	}

	return res, nil
}

func expandPaths(client *hdfs.Client, paths []string) ([]string, error) {
	var res []string

	for _, p := range paths {
		if hasGlob(p) {
			expanded, err := expandGlobs(client, p)
			if err != nil {
				return nil, err
			}

			res = append(res, expanded...)
		} else {
			res = append(res, p)
		}
	}

	return res, nil
}
