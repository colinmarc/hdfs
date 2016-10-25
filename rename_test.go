package hdfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRename(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove")
	baleet(t, "/_test/tomovedest")

	err := client.Rename("/_test/tomove", "/_test/tomovedest")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/tomove")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/tomove", os.ErrNotExist)

	fi, err = client.Stat("/_test/tomovedest")
	require.NoError(t, err)
}

func TestRenameSrcNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")
	baleet(t, "/_test/nonexistent2")

	err := client.Rename("/_test/nonexistent", "/_test/nonexistent2")
	assertPathError(t, err, "rename", "/_test/nonexistent", os.ErrNotExist)
}

func TestRenameDestExists(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove2")
	touch(t, "/_test/tomovedest2")

	err := client.Rename("/_test/tomove2", "/_test/tomovedest2")
	require.NoError(t, err)
}

func TestRenameWithoutPermissionForSrc(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	mkdirp(t, "/_test/accessdenied")
	touch(t, "/_test/accessdenied/foo")

	err := otherClient.Rename("/_test/accessdenied/foo", "/_test/tomovedest3")
	assertPathError(t, err, "rename", "/_test/accessdenied/foo", os.ErrPermission)
}

func TestRenameWithoutPermissionForDest(t *testing.T) {
	otherClient := getClientForUser(t, "other")

	baleet(t, "/_test/ownedbyother2")

	err := otherClient.CreateEmptyFile("/_test/ownedbyother2")
	require.NoError(t, err)

	err = otherClient.Rename("/_test/ownedbyother2", "/_test/accessdenied/tomovedest4")
	assertPathError(t, err, "rename", "/_test/accessdenied/tomovedest4", os.ErrPermission)
}
