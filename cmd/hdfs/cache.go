package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
	"os"
	"path"
)

var (
	cachedClient *hdfs.Client

	statCache    = make(map[string]os.FileInfo)
	readDirCache = make(map[string][]os.FileInfo)
)

func getClient(namenode string) (*hdfs.Client, error) {
	if cachedClient != nil {
		return cachedClient, nil
	}

	if namenode == "" {
		namenode = os.Getenv("HADOOP_NAMENODE")
	}

	if namenode == "" {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths, or set HADOOP_NAMENODE in your environment.")
	}

	c, err := hdfs.New(namenode)
	if err != nil {
		return nil, err
	}

	cachedClient = c
	return cachedClient, nil
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

func readDir(client *hdfs.Client, dir string) ([]os.FileInfo, error) {
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

	return res, nil
}
