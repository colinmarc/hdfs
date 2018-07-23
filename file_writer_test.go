package hdfs

import (
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/colinmarc/hdfs/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const abcException = "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException"

func TestFileWrite(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/1.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.Create("/_test/create/1.txt")
	require.NoError(t, err)

	n, err := writer.Write([]byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = writer.Write([]byte("bar"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/1.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "foobar", string(bytes))
}

func TestFileBigWrite(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/2.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.Create("/_test/create/2.txt")
	require.NoError(t, err)

	mobydick, err := os.Open("testdata/mobydick.txt")
	require.NoError(t, err)

	n, err := io.Copy(writer, mobydick)
	require.NoError(t, err)
	assert.EqualValues(t, 1257276, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/2.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	n, err = io.Copy(hash, reader)
	assert.Nil(t, err)
	assert.EqualValues(t, 1257276, n)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
}

func TestFileBigWriteMultipleBlocks(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/3.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.CreateFile("/_test/create/3.txt", 1, 1048576, 0755)
	require.NoError(t, err)

	mobydick, err := os.Open("testdata/mobydick.txt")
	require.NoError(t, err)

	n, err := io.Copy(writer, mobydick)
	require.NoError(t, err)
	assert.EqualValues(t, 1257276, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/3.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	n, err = io.Copy(hash, reader)
	assert.Nil(t, err)
	assert.EqualValues(t, 1257276, n)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
}

func TestFileBigWriteWeirdBlockSize(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/4.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.CreateFile("/_test/create/4.txt", 1, 1050000, 0755)
	require.NoError(t, err)

	mobydick, err := os.Open("testdata/mobydick.txt")
	require.NoError(t, err)

	n, err := io.Copy(writer, mobydick)
	require.NoError(t, err)
	assert.EqualValues(t, 1257276, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/4.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	n, err = io.Copy(hash, reader)
	assert.Nil(t, err)
	assert.EqualValues(t, 1257276, n)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
}

func TestFileBigWriteReplication(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/5.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.CreateFile("/_test/create/5.txt", 3, 1048576, 0755)
	require.NoError(t, err)

	mobydick, err := os.Open("testdata/mobydick.txt")
	require.NoError(t, err)

	n, err := io.Copy(writer, mobydick)
	require.NoError(t, err)
	assert.EqualValues(t, 1257276, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/5.txt")
	require.NoError(t, err)

	hash := crc32.NewIEEE()
	n, err = io.Copy(hash, reader)
	assert.Nil(t, err)
	assert.EqualValues(t, 1257276, n)
	assert.EqualValues(t, 0x199d1ae6, hash.Sum32())
}

func TestFileWriteSmallFlushes(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/create/6.txt")
	mkdirp(t, "/_test/create")
	writer, err := client.Create("/_test/create/6.txt")
	require.NoError(t, err)

	// Do a normal write, then a bunch of small flushes, then a normal write again.
	expected := strings.Repeat("b", 65536+rand.Intn(1024)) + "\n"

	n, err := writer.Write([]byte(expected))
	require.NoError(t, err, "initial write of %d bytes", len(expected))
	assert.Equal(t, len(expected), n)

	for i := 0; i < 100; i++ {
		s := strings.Repeat("b", rand.Intn(1024)) + "\n"
		expected += s

		n, err := writer.Write([]byte(s))
		require.NoError(t, err, "write #%d of %d bytes", i, len(s))
		assert.Equal(t, len(s), n)

		err = writer.Flush()
		require.NoError(t, err, "flush #%d", i)
	}

	s := strings.Repeat("b", 65536+rand.Intn(1024)) + "\n"
	expected += s

	n, err = writer.Write([]byte(s))
	require.NoError(t, err, "final write of %d bytes", len(s))
	assert.Equal(t, len(s), n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/create/6.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, len(expected), len(bytes))
	assert.Equal(t, expected, string(bytes))
}

func TestCreateEmptyFile(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/emptyfile")

	err := client.CreateEmptyFile("/_test/emptyfile")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/emptyfile")
	require.NoError(t, err)
	assert.False(t, fi.IsDir())
	assert.EqualValues(t, 0, fi.Size())

	err = client.CreateEmptyFile("/_test/emptyfile")
	assertPathError(t, err, "create", "/_test/emptyfile", os.ErrExist)
}

func TestCreateEmptyFileWithoutParent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.CreateEmptyFile("/_test/nonexistent/emptyfile")
	assertPathError(t, err, "create", "/_test/nonexistent/emptyfile", os.ErrNotExist)

	_, err = client.Stat("/_test/nonexistent/emptyfile")
	assertPathError(t, err, "stat", "/_test/nonexistent/emptyfile", os.ErrNotExist)
}

func TestCreateEmptyFileWithoutPermission(t *testing.T) {
	client := getClient(t)
	client2 := getClientForUser(t, "gohdfs2")

	mkdirp(t, "/_test/accessdenied")
	baleet(t, "/_test/accessdenied/emptyfile")

	err := client2.CreateEmptyFile("/_test/accessdenied/emptyfile")
	assertPathError(t, err, "create", "/_test/accessdenied/emptyfile", os.ErrPermission)

	_, err = client.Stat("/_test/accessdenied/emptyfile")
	assertPathError(t, err, "stat", "/_test/accessdenied/emptyfile", os.ErrNotExist)
}

func TestFileAppend(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/append/1.txt")
	mkdirp(t, "/_test/append")
	writer, err := client.Create("/_test/append/1.txt")
	require.NoError(t, err)

	n, err := writer.Write([]byte("foobar\n"))
	require.NoError(t, err)
	assert.Equal(t, 7, n)

	err = writer.Close()
	require.NoError(t, err)

	writer, err = client.Append("/_test/append/1.txt")
	require.NoError(t, err)

	n, err = writer.Write([]byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = writer.Write([]byte("baz"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/append/1.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "foobar\nfoobaz", string(bytes))
}

func TestFileAppendEmptyFile(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/append/2.txt")
	mkdirp(t, "/_test/append")
	err := client.CreateEmptyFile("/_test/append/2.txt")
	require.NoError(t, err)

	writer, err := client.Append("/_test/append/2.txt")
	require.NoError(t, err)

	n, err := writer.Write([]byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = writer.Write([]byte("bar"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/append/2.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "foobar", string(bytes))
}

func TestFileAppendLastBlockFull(t *testing.T) {
	mobydick, err := os.Open("testdata/mobydick.txt")
	require.NoError(t, err)

	client := getClient(t)

	baleet(t, "/_test/append/3.txt")
	mkdirp(t, "/_test/append")

	writer, err := client.CreateFile("/_test/append/3.txt", 3, 1048576, 0644)
	require.NoError(t, err)

	wn, err := io.CopyN(writer, mobydick, 1048576)
	require.NoError(t, err)
	assert.EqualValues(t, 1048576, wn)

	err = writer.Close()
	require.NoError(t, err)

	writer, err = client.Append("/_test/append/3.txt")
	require.NoError(t, err)

	n, err := writer.Write([]byte("\nfoo"))
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	err = writer.Close()
	require.NoError(t, err)

	reader, err := client.Open("/_test/append/3.txt")
	require.NoError(t, err)

	err = reader.getBlocks()
	require.NoError(t, err)

	assert.Equal(t, 2, len(reader.blocks))

	buf := make([]byte, 4)
	n, err = reader.ReadAt(buf, 1048576)
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	assert.Equal(t, "\nfoo", string(buf))
}

func TestFileAppendRepeatedly(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/append/4.txt")
	mkdirp(t, "/_test/append")
	writer, err := client.Create("/_test/append/4.txt")
	require.NoError(t, err)

	n, err := writer.Write([]byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	err = writer.Close()
	require.NoError(t, err)

	expected := "foo"
	for i := 0; i < 20; i++ {
		writer, err = client.Append("/_test/append/4.txt")

		// This represents a bug in the HDFS append implementation, as far as I can tell,
		// and is safe to skip.
		if pathErr, ok := err.(*os.PathError); ok {
			if nnErr, ok := pathErr.Err.(*rpc.NamenodeError); ok && nnErr.Exception == abcException {
				t.Log("Ignoring AlreadyBeingCreatedException from append")
				continue
			}
		}

		require.NoError(t, err)

		s := strings.Repeat("b", rand.Intn(1024)) + "\n"
		expected += s

		n, err = writer.Write([]byte(s))
		require.NoError(t, err, "append #%d of %d bytes", i, len(s))
		assert.Equal(t, len(s), n)

		err = writer.Close()
		require.NoError(t, err)
	}

	reader, err := client.Open("/_test/append/4.txt")
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, len(expected), len(bytes))
	assert.Equal(t, expected, string(bytes))
}
