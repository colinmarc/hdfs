package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func getClient(t *testing.T) *Client {
	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	client, err := New(nn)
	if err != nil {
		t.Fatal(err)
	}

	return client
}

func TestStat(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/foo.txt")
	require.Nil(t, err)

	assert.Equal(t, "/foo.txt", resp.Name())
	assert.False(t, resp.IsDir())
	assert.Equal(t, 4, resp.Size())
	assert.Equal(t, time.Now().Year(), resp.ModTime().Year())
	assert.Equal(t, time.Now().Month(), resp.ModTime().Month())
}

func TestStatNotExists(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
	assert.Nil(t, resp)
}

func TestStatDir(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/full")
	require.Nil(t, err)

	assert.Equal(t, "/full", resp.Name())
	assert.True(t, resp.IsDir())
	assert.Equal(t, 0, resp.Size(), 0)
	assert.Equal(t, time.Now().Year(), resp.ModTime().Year())
	assert.Equal(t, time.Now().Month(), resp.ModTime().Month())
}

func TestReadDir(t *testing.T) {
	client := getClient(t)

	res, err := client.ReadDir("/full")
	assert.Nil(t, err)
	require.Equal(t, len(res), 4)

	assert.Equal(t, "/full/1", res[0].Name())
	assert.False(t, res[0].IsDir())
	assert.Equal(t, 4, res[0].Size())

	assert.Equal(t, "/full/2", res[1].Name())
	assert.False(t, res[1].IsDir())
	assert.Equal(t, 4, res[1].Size())

	assert.Equal(t, "/full/3", res[2].Name())
	assert.False(t, res[2].IsDir())
	assert.Equal(t, 4, res[2].Size())

	assert.Equal(t, "/full/dir", res[3].Name())
	assert.True(t, res[3].IsDir())
	assert.Equal(t, 0, res[3].Size())
}

func TestReadDirTrailingSlash(t *testing.T) {
	client := getClient(t)

	res, err := client.ReadDir("/full/")
	assert.Nil(t, err)
	require.Equal(t, len(res), 4)

	assert.Equal(t, "/full/1", res[0].Name())
	assert.False(t, res[0].IsDir())
	assert.Equal(t, 4, res[0].Size())

	assert.Equal(t, "/full/2", res[1].Name())
	assert.False(t, res[1].IsDir())
	assert.Equal(t, 4, res[1].Size())

	assert.Equal(t, "/full/3", res[2].Name())
	assert.False(t, res[2].IsDir())
	assert.Equal(t, 4, res[2].Size())

	assert.Equal(t, "/full/dir", res[3].Name())
	assert.True(t, res[3].IsDir())
	assert.Equal(t, 0, res[3].Size())
}

func TestReadEmptyDir(t *testing.T) {
	client := getClient(t)

	res, err := client.ReadDir("/empty")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/foo.txt")
	assert.Nil(t, err)
	assert.Equal(t, "bar\n", string(bytes))
}
