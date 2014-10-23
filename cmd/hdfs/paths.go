package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
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

	statCache    = make(map[string]os.FileInfo)
	readDirCache = make(map[string][]os.FileInfo)
)

func userDir() string {
	currentUser, err := user.Current()
	if err != nil || currentUser.Username == "" {
		return "/"
	} else {
		return path.Join("/user", currentUser.Username)
	}
}

// returns absolute paths, and an namenode host, if a single one was found
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

func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`[^\\][[*?]`, fragment)
	return match
}

func filterByGlob(paths []string, fragment string) []string {
	res := make([]string, 0, len(paths))
	for _, p := range paths {
		_, name := path.Split(p)
		match, _ := path.Match(fragment, name)
		if match {
			res = append(res, p)
		}
	}

	return res
}

// recursively expands globs. this assumes the path is already absolute
func expandGlobs(client *hdfs.Client, p string) ([]string, error) {
	if !hasGlob(p) {
		return []string{p}, nil
	}

	parts := strings.Split(p, "/")[1:]
	res := make([]string, 0)
	splitAt := 0
	for splitAt, _ = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	base := "/" + path.Join(parts[:splitAt]...)
	glob := parts[splitAt]
	remainder := path.Join(parts[splitAt+1:]...)
	list, err := readDir(client, base, glob)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
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

func stat(client *hdfs.Client, fullPath string) (os.FileInfo, error) {
	if cachedRes, exists := statCache[fullPath]; exists {
		return cachedRes, nil
	}

	res, err := client.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	statCache[fullPath] = res
	return res, nil
}

func readDir(client *hdfs.Client, dir string, glob string) ([]os.FileInfo, error) {
	if cachedRes, exists := readDirCache[dir]; exists {
		return cachedRes, nil
	}

	res, err := client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	readDirCache[dir] = res
	for _, fi := range res {
		childPath := path.Join(dir, fi.Name())
		statCache[childPath] = fi
	}

	if glob != "" {
		matched := make([]os.FileInfo, 0, len(res))
		for _, fi := range res {
			match, _ := path.Match(glob, fi.Name())
			if match {
				matched = append(matched, fi)
			}
		}

		return matched, nil
	} else {
		return res, nil
	}
}
