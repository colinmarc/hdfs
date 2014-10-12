package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var cachedClient *Client

func getClient(t *testing.T) *Client {
	if cachedClient != nil {
		return cachedClient
	}

	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	client, err := New(nn)
	if err != nil {
		t.Fatal(err)
	}

	cachedClient = client
	return cachedClient
}

func touch(t *testing.T, path string) {
	c := getClient(t)

	err := c.CreateEmptyFile(path)
	if err != nil && err != os.ErrExist {
		log.Printf("%#v", err)
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
