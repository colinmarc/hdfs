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
	if err != nil && err != os.ErrExist {
		t.Fatal(err)
	}
}

func mkdirp(t *testing.T, path string) {
	c := getClient(t)

	err := c.MkdirAll(path, 0644)
	if err != nil && err != os.ErrExist {
		t.Fatal(err)
	}
}

func baleet(t *testing.T, path string) {
	c := getClient(t)

	err := c.Remove(path)
	if err != nil && err != os.ErrNotExist {
		t.Fatal(err)
	}
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/_test/foo.txt")
	assert.Nil(t, err)
	assert.Equal(t, "bar\n", string(bytes))
}

func TestCopyToLocal(t *testing.T) {
	client := getClient(t)

	dir, _ := ioutil.TempDir("", "hdfs-test")
	tmpfile := filepath.Join(dir, "foo.txt")
	err := client.CopyToLocal("/_test/foo.txt", tmpfile)
	require.Nil(t, err)

	f, err := os.Open(tmpfile)
	require.Nil(t, err)

	bytes, _ := ioutil.ReadAll(f)
	assert.Equal(t, "bar\n", string(bytes))
}

func TestCreateEmptyFile(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/emptyfile")

	err := client.CreateEmptyFile("/_test/emptyfile")
	require.Nil(t, err)

	fi, err := client.Stat("/_test/emptyfile")
	require.Nil(t, err)
	assert.False(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())

	err = client.CreateEmptyFile("/_test/emptyfile")
	assert.Equal(t, os.ErrExist, err)
}

func TestCreateEmptyFileWithoutParent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")

	err := client.CreateEmptyFile("/_test/nonexistent/emptyfile")
	assert.Equal(t, os.ErrNotExist, err)

	_, err = client.Stat("/_test/nonexistent/emptyfile")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestCreateEmptyFileWithoutPermission(t *testing.T) {
	client := getClient(t)
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	baleet(t, "/_test/accessdenied/emptyfile")

	err := otherClient.CreateEmptyFile("/_test/accessdenied/emptyfile")
	assert.Equal(t, os.ErrPermission, err)

	_, err = client.Stat("/_test/accessdenied/emptyfile")
	assert.Equal(t, os.ErrNotExist, err)
}
