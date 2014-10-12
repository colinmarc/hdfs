package hdfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

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
