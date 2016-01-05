package hdfs

import (
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	mobydick, err := os.Open("test/mobydick.txt")
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

	mobydick, err := os.Open("test/mobydick.txt")
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

	mobydick, err := os.Open("test/mobydick.txt")
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

	mobydick, err := os.Open("test/mobydick.txt")
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
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	baleet(t, "/_test/accessdenied/emptyfile")

	err := otherClient.CreateEmptyFile("/_test/accessdenied/emptyfile")
	assertPathError(t, err, "create", "/_test/accessdenied/emptyfile", os.ErrPermission)

	_, err = client.Stat("/_test/accessdenied/emptyfile")
	assertPathError(t, err, "stat", "/_test/accessdenied/emptyfile", os.ErrNotExist)
}
