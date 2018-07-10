HDFS for Go
===========

[![GoDoc](https://godoc.org/github.com/colinmarc/hdfs/web?status.svg)](https://godoc.org/github.com/colinmarc/hdfs) [![build](https://travis-ci.org/colinmarc/hdfs.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

This is a native golang client for hdfs. It connects directly to the namenode using
the protocol buffers API.

It tries to be idiomatic by aping the stdlib `os` package, where possible, and
implements the interfaces from it, including `os.FileInfo` and `os.PathError`.

Here's what it looks like in action:

```go
client, _ := hdfs.New("namenode:8020")

file, _ := client.Open("/mobydick.txt")

buf := make([]byte, 59)
file.ReadAt(buf, 48847)

fmt.Println(string(buf))
// => Abominable are the tumblers into which he pours his poison.
```

For complete documentation, check out the [Godoc][1].

The `hdfs` Binary
-----------------

Along with the library, this repo contains a commandline client for HDFS. Like
the library, its primary aim is to be idiomatic, by enabling your favorite unix
verbs:


    $ hdfs --help
    Usage: hdfs COMMAND
    The flags available are a subset of the POSIX ones, but should behave similarly.

    Valid commands:
      ls [-lah] [FILE]...
      rm [-rf] FILE...
      mv [-fT] SOURCE... DEST
      mkdir [-p] FILE...
      touch [-amc] FILE...
      chmod [-R] OCTAL-MODE FILE...
      chown [-R] OWNER[:GROUP] FILE...
      cat SOURCE...
      head [-n LINES | -c BYTES] SOURCE...
      tail [-n LINES | -c BYTES] SOURCE...
      du [-sh] FILE...
      checksum FILE...
      get SOURCE [DEST]
      getmerge SOURCE DEST
      put SOURCE DEST
      
      To alter the default locations from which configurations are loaded, 
      the following environment variables may be used:

        - HADOOP_CONF_DIR     hadoop configuration directory. Default: %s
        - HADOOP_KRB_CONF     kerberos configuration file. Default: %s
        - HADOOP_CCACHE       credential cache to use. Defaults: to "/tmp/krb5cc_{user_uid}"
        - HADOOP_KEYTAB       if set, the specified keytab is used and the credential cache is ignored.

Since it doesn't have to wait for the JVM to start up, it's also a lot faster
`hadoop -fs`:

    $ time hadoop fs -ls / > /dev/null

    real  0m2.218s
    user  0m2.500s
    sys 0m0.376s

    $ time hdfs ls / > /dev/null

    real  0m0.015s
    user  0m0.004s
    sys 0m0.004s

Best of all, it comes with bash tab completion for paths!

Installing the library
----------------------

To install the library, once you have Go [all set up][2]:

    $ go get -u github.com/colinmarc/hdfs

Installing the commandline client
---------------------------------

Grab a tarball from the [releases page](https://github.com/colinmarc/hdfs/releases)
and unzip it wherever you like.

You'll want to add the following line to your `.bashrc` or `.profile`:

    export HADOOP_NAMENODE="namenode:8020"

To install tab completion globally on linux, copy or link the `bash_completion`
file which comes with the tarball into the right place:

    ln -sT bash_completion /etc/bash_completion.d/gohdfs

By default, the HDFS user is set to the currently-logged-in user. You can
override this in your `.bashrc` or `.profile`:

    export HADOOP_USER_NAME=username


Kerberos support
----------------
Authentication via Kerberos (and authentication only) is supported.

The binary will check the default locations for your kerberos and hadoop configurations. These can be overridden via environment variables `HADOOP_KRB_CONF`, and `HADOOP_CONF_DIR`.

You will need either a kinit’ed credential cache, which is expected to live at `/tmp/krb5cc_$(id -u $(whoami))` — override via `HADOOP_CCACHE` — or a keytab specified through `HADOOP_KEYTAB`.

This has only been tested on one or two different kerberized clusters: if you have trouble using it, feedback is more than welcome.



Compatibility
-------------

This library uses "Version 9" of the HDFS protocol, which means it should work
with hadoop distributions based on 2.2.x and above. The tests run against CDH
5.x and HDP 2.x.

Acknowledgements
----------------

This library is heavily indebted to [snakebite][3].

[1]: https://godoc.org/github.com/colinmarc/hdfs
[2]: https://golang.org/doc/install
[3]: https://github.com/spotify/snakebite
