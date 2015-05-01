package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"
)

var cachedClients = make(map[string]*Client)

func getClient(t *testing.T) *Client {
	currentUser, _ := user.Current()
	return getClientForUser(t, currentUser.Username)
}

func getClientForUser(t *testing.T, user string) *Client {
	if c, ok := cachedClients[user]; ok {
		return c
	}

	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	client, err := NewForUser(nn, user)
	if err != nil {
		t.Fatal(err)
	}

	cachedClients[user] = client
	return client
}

func touch(t *testing.T, path string) {
	c := getClient(t)

	err := c.CreateEmptyFile(path)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func mkdirp(t *testing.T, path string) {
	c := getClient(t)

	err := c.MkdirAll(path, 0644)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func baleet(t *testing.T, path string) {
	c := getClient(t)

	err := c.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func assertPathError(t *testing.T, err error, op, path string, wrappedErr error) {
	require.NotNil(t, err)

	expected := &os.PathError{op, path, wrappedErr}
	require.Equal(t, expected.Error(), err.Error())
	require.Equal(t, expected, err)
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/_test/foo.txt")
	assert.Nil(t, err)
	assert.EqualValues(t, "bar\n", string(bytes))
}

func TestCopyToLocal(t *testing.T) {
	client := getClient(t)

	dir, _ := ioutil.TempDir("", "hdfs-test")
	tmpfile := filepath.Join(dir, "foo.txt")
	err := client.CopyToLocal("/_test/foo.txt", tmpfile)
	require.NoError(t, err)

	f, err := os.Open(tmpfile)
	require.NoError(t, err)

	bytes, _ := ioutil.ReadAll(f)
	assert.EqualValues(t, "bar\n", string(bytes))
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
