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

func TestMkdir(t *testing.T) {
	client := getClient(t)

	err := client.Mkdir("/test", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/test")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())
}

func TestMkdirNested(t *testing.T) {
	client := getClient(t)

	err := client.Mkdir("/test2/foo", 777)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err := client.Stat("/test2/foo")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err = client.Stat("/test2")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestMkdirAllNested(t *testing.T) {
	client := getClient(t)

	err := client.MkdirAll("/test3/foo", 777)
	assert.Nil(t, err)

	fi, err := client.Stat("/test3/foo")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())

	fi, err = client.Stat("/test3")
	assert.Nil(t, err)
	assert.True(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())
}

func TestRemove(t *testing.T) {
	client := getClient(t)

	err := client.Remove("/todelete")
	assert.Nil(t, err)

	fi, err := client.Stat("/todelete")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRemoveNotExistent(t *testing.T) {
	client := getClient(t)

	err := client.Remove("/nonexistent")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRename(t *testing.T) {
	client := getClient(t)

	err := client.Rename("/tomove", "/tomovedest")
	assert.Nil(t, err)

	fi, err := client.Stat("/tomove")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err = client.Stat("/tomovedest")
	assert.Nil(t, err)
}

func TestRenameSrcNotExistent(t *testing.T) {
	client := getClient(t)

	err := client.Rename("/nonexistent", "/nonexistent2")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRenameDestExists(t *testing.T) {
	client := getClient(t)

	err := client.Rename("/foo.txt", "/mobydick.txt")
	assert.Equal(t, os.ErrExist, err)
}
