package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRename(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove")
	baleet(t, "/_test/tomovedest")

	err := client.Rename("/_test/tomove", "/_test/tomovedest")
	assert.Nil(t, err)

	fi, err := client.Stat("/_test/tomove")
	assert.Nil(t, fi)
	assert.Equal(t, os.ErrNotExist, err)

	fi, err = client.Stat("/_test/tomovedest")
	assert.Nil(t, err)
}

func TestRenameSrcNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")
	baleet(t, "/_test/nonexistent2")

	err := client.Rename("/_test/nonexistent", "/_test/nonexistent2")
	assert.Equal(t, os.ErrNotExist, err)
}

func TestRenameDestExists(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove2")
	touch(t, "/_test/tomovedest2")

	err := client.Rename("/_test/tomove2", "/_test/tomovedest2")
	assert.Equal(t, os.ErrExist, err)
}

func TestRemoveWithoutPermissionForSrc(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	err := otherClient.Rename("/_test/accessdenied/foo", "/_test/tomovedest3")
	assert.Equal(t, os.ErrPermission, err)
}

func TestRemoveWithoutPermissionForDest(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	baleet(t, "/_test/ownedbyother")

	err := otherClient.CreateEmptyFile("/_test/ownedbyother")
	assert.Nil(t, err)

	err = otherClient.Rename("/_test/ownedbyother", "/_test/accessdenied/tomovedest4")
	assert.Equal(t, os.ErrPermission, err)
}
