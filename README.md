HDFS for Go
===========

[![build](https://travis-ci.org/colinmarc/hdfs.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

`hdfs` is a native go client for hdfs, using the protocol buffers interface the
namenode provides. It implements protocol version 9, which means it supports
Hadoop 2.0.0 and up (including CDH5).

It attempts to be as idiomatic as possible by aping the stdlib `os` package
where possible. This includes implementing `os.FileInfo` for file status, and
returning `os.ErrNotExist` for missing files, for example.

The best place to get started is the
[Godoc](https://godoc.org/github.com/colinmarc/hdfs).

```go

client, _ := hdfs.New("namenode:8020")

file, _ := client.Open("/mobydick.txt")

buf := make([]byte, 59)
file.ReadAt(buf, 48847)

log.Println(string(buf))
// => Abominable are the tumblers into which he pours his poison.
```
