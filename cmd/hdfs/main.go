package main

import "github.com/colinmarc/hdfs/cmd/hdfs/cmd"

var version string

func main() {
	cmd.Execute(version)
}
