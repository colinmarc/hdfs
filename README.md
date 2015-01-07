HDFS for Go
===========

[![GoDoc](https://godoc.org/github.com/colinmarc/hdfs/web?status.svg)](https://godoc.org/github.com/colinmarc/hdfs/web) [![build](https://travis-ci.org/colinmarc/hdfs.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

`hdfs` is a native go client for hdfs, using the protocol buffers interface the
namenode provides. It implements protocol version 9, which means it supports
Hadoop 2.0.0 and up (including CDH5).

It tries to be idiomatic by aping the stdlib `os` package where possible. This
includes implementing `os.FileInfo` for file status, and returning errors of
type `os.PathErrors` for missing files, for example.

The best place to get started is the
[Godoc][1].

```go

client, _ := hdfs.New("namenode:8020")

file, _ := client.Open("/mobydick.txt")

buf := make([]byte, 59)
file.ReadAt(buf, 48847)

fmt.Println(string(buf))
// => Abominable are the tumblers into which he pours his poison.
```

The `hdfs` Binary
-----------------

The library also ships with a command line client for hdfs. Like the library,
its primary aim is to be idiomatic, by enabling your favorite unix verbs:


    $ hdfs --help
    Usage: ./hdfs COMMAND
    The flags available are a subset of the POSIX ones, but should behave similarly.

    Valid commands:
      ls [-la] [FILE]...
      rm [-r] FILE...
      mv [-fT] SOURCE... DEST
      mkdir [-p] FILE...
      touch [-amc] FILE...
      chmod [-R] OCTAL-MODE FILE...
      chown [-R] OWNER[:GROUP] FILE...
      cat SOURCE...
      head [-n LINES | -c BYTES] SOURCE...
      tail [-n LINES | -c BYTES] SOURCE...
      get SOURCE [DEST]
      getmerge SOURCE DEST

It's also pretty fast compared to `hadoop -fs`. How much faster, you ask?

    $ time hadoop fs -ls / > /dev/null

    real  0m2.218s
    user  0m2.500s
    sys 0m0.376s

    $ time hdfs ls / > /dev/null

    real  0m0.015s
    user  0m0.004s
    sys 0m0.004s

Best of all, it comes with bash tab completion!

Installing
----------

To install the library, once you have Go [all set up][2]:

    $ go get github.com/colinmarc/hdfs

And to install the commandline client:

    $ go get github.com/colinmarc/hdfs/cmd/hdfs

Alternatively, to create an `hdfs` binary that you can install wherever you
want:

    $ git clone https://github.com/colinmarc/hdfs
    $ cd hdfs
    $ make

Finally, you'll want to add two lines to your `.bashrc` or `.profile`:

    source $GOPATH/src/github.com/colinmarc/hdfs/cmd/hdfs/bash_completion
    HADOOP_NAMENODE="namenode:8020"

Or, to install tab completion globally on linux:

    ln -sT $GOPATH/src/github.com/colinmarc/hdfs/cmd/hdfs/bash_completion /etc/bash_completion.d/gohdfs

Acknowledgements
----------------

This library is heavily indebted to [snakebite][3].

[1]: https://godoc.org/github.com/colinmarc/hdfs
[2]: https://golang.org/doc/install
[3]: https://github.com/spotify/snakebite
