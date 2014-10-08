package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"time"
	"testing"
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

	resp, err := client.Stat("/foo")
	assert.Nil(t, err)

	assert.Equal(t, resp.Name(), "/foo")
	assert.False(t, resp.IsDir())
	assert.Equal(t, resp.Size(), 4)
	assert.Equal(t, resp.ModTime().Year(), time.Now().Year())
	assert.Equal(t, resp.ModTime().Month(), time.Now().Month())
}

func TestStatNotExists(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/nonexistent")
	assert.Equal(t, err, os.ErrNotExist)
	assert.Nil(t, resp)
}

func TestStatDir(t *testing.T) {
	client := getClient(t)

	resp, err := client.Stat("/full")
	assert.Nil(t, err)

	assert.Equal(t, resp.Name(), "/full")
	assert.True(t, resp.IsDir())
	assert.Equal(t, resp.Size(), 0)
	assert.Equal(t, resp.ModTime().Year(), time.Now().Year())
	assert.Equal(t, resp.ModTime().Month(), time.Now().Month())
}

func TestReadDir(t *testing.T) {
	client := getClient(t)

	res, err := client.ReadDir("/full")
	assert.Nil(t, err)
	assert.Equal(t, len(res), 4)
	if len(res) != 4 {
		t.FailNow()
	}

	assert.Equal(t, res[0].Name(), "/full/1")
	assert.False(t, res[0].IsDir())
	assert.Equal(t, res[0].Size(), 4)

	assert.Equal(t, res[1].Name(), "/full/2")
	assert.False(t, res[1].IsDir())
	assert.Equal(t, res[1].Size(), 4)

	assert.Equal(t, res[2].Name(), "/full/3")
	assert.False(t, res[2].IsDir())
	assert.Equal(t, res[2].Size(), 4)

	assert.Equal(t, res[3].Name(), "/full/dir")
	assert.True(t, res[3].IsDir())
	assert.Equal(t, res[3].Size(), 0)
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/foo")
	assert.Nil(t, err)
	assert.Equal(t, string(bytes), "bar\n")
}
