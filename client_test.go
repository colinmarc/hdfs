package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
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

func TestCreateEmptyFile(t *testing.T) {
	client := getClient(t)

	err := client.CreateEmptyFile("/testemptyfile")
	assert.Nil(t, err)

	fi, err := client.Stat("/testemptyfile")
	require.Nil(t, err)
	assert.False(t, fi.IsDir())
	assert.Equal(t, 0, fi.Size())

	err = client.CreateEmptyFile("/testemptyfile")
	assert.Equal(t, os.ErrExist, err)
}

func TestCreateEmptyFileWithoutParent(t *testing.T) {
	client := getClient(t)

	err := client.CreateEmptyFile("/nonexistent/testemptyfile")
	assert.Equal(t, os.ErrNotExist, err)

	_, err = client.Stat("/nonexistent/testemptyfile")
	assert.Equal(t, os.ErrNotExist, err)
}
