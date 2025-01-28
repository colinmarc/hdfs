<img src="docs/gopher.png" alt="gopher" align="right" width="200"/>

HDFS for Go

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

To configure the client, make sure one or both of these environment variables
point to your Hadoop configuration (`core-site.xml` and `hdfs-site.xml`). On
systems with Hadoop installed, they should already be set.

    $ export HADOOP_HOME="/etc/hadoop"
    $ export HADOOP_CONF_DIR="/etc/hadoop/conf"


By default on non-kerberized clusters, the HDFS user is set to the
currently-logged-in user. You can override this with another environment
variable:

    $ export HADOOP_USER_NAME=username

Using the commandline client with Kerberos authentication
---------------------------------------------------------

Like `hadoop fs`, the commandline client expects a `ccache` file in the default
location: `/tmp/krb5cc_<uid>`. That means it should 'just work' to use `kinit`:

    $ kinit bob@EXAMPLE.com
    $ hdfs ls /

If that doesn't work, try setting the `KRB5CCNAME` environment variable to
wherever you have the `ccache` saved.

Compatibility
-------------

This library uses "Version 9" of the HDFS protocol, which means it should work
with hadoop distributions based on 2.2.x and above, as well as 3.x.

Acknowledgements
----------------

This library is heavily indebted to [snakebite][3].

This library is a fork of the [hdfs](4). This is maintained by [Acceldata](5)


[1]: https://godoc.org/github.com/colinmarc/hdfs
[2]: https://golang.org/doc/install
[3]: https://github.com/spotify/snakebite
[4]: https://github.com/colinmarc/hdfs  
[5]: https://acceldata.io
