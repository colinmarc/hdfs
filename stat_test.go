package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

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
