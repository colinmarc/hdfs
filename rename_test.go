package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

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
