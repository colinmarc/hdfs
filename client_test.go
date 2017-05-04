package hdfs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var cachedClients = make(map[string]*Client)

func getClient(t *testing.T) *Client {
	username, err := Username()
	if err != nil {
		t.Fatal(err)
	}

	return getClientForUser(t, username)
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

func TestNewWithMultipleNodes(t *testing.T) {
	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}
	_, err := NewClient(ClientOptions{
		Addresses: []string{"localhost:80", nn},
	})
	assert.Nil(t, err)
}

func TestNewWithFailingNode(t *testing.T) {
	_, err := New("localhost:80")
	assert.NotNil(t, err)
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/_test/foo.txt")
	assert.NoError(t, err)
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

func TestCopyToRemote(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/copytoremote.txt")
	err := client.CopyToRemote("test/foo.txt", "/_test/copytoremote.txt")
	require.NoError(t, err)

	bytes, err := client.ReadFile("/_test/copytoremote.txt")
	require.NoError(t, err)

	assert.EqualValues(t, "bar\n", string(bytes))
}
