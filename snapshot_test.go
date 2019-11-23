package hdfs

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeAndVerifyTestFile(t *testing.T, snapshotDir, filepath string) {
	c := getClientForSuperUser(t)

	baleet(t, filepath)
	mkdirp(t, snapshotDir)

	f, err := c.CreateFile(filepath, 1, 1048576, 0744)
	require.NoError(t, err)

	// fill the file a bit
	b := make([]byte, 128)
	for i := 0; i < 128; i++ {
		b[i] = 'a'
	}

	_, err = f.Write(b)
	require.NoError(t, err)
	f.Close()

	nf, err := c.Open(filepath)
	require.NoError(t, err)

	br, err := ioutil.ReadAll(nf)
	require.NoError(t, err)
	nf.Close()

	assert.Equal(t, b, br)
}

func baleetSnapshot(t *testing.T, dir, snapshot string) {
	c := getClientForSuperUser(t)
	c.DeleteSnapshot(dir, snapshot)
}

func TestAllowSnapshot(t *testing.T) {
	c := getClientForSuperUser(t)
	baleetSnapshot(t, "/_test/allowsnaps", "snap")
	mkdirp(t, "/_test/allowsnaps")
	err := c.AllowSnapshots("/_test/allowsnaps")
	require.NoError(t, err)
	path, err := c.CreateSnapshot("/_test/allowsnaps", "snap")
	require.NoError(t, err)
	assert.Equal(t, "/_test/allowsnaps/.snapshot/snap", path)
}

func TestDisallowSnapshot(t *testing.T) {
	c := getClientForSuperUser(t)
	baleetSnapshot(t, "/_test/allowsnaps", "snap")
	mkdirp(t, "/_test/allowsnaps")
	err := c.DisallowSnapshots("/_test/allowsnaps")
	require.NoError(t, err)
	_, err = c.CreateSnapshot("/_test/allowsnaps", "snap")
	require.Error(t, err)
}

func TestSnapshot(t *testing.T) {
	const name = "TestSnapshot"
	const dir = "/_test/snapshot"
	const filename = "file_to_restore.txt"
	const filepath = "/_test/snapshot/file_to_restore.txt"

	c := getClientForSuperUser(t)
	baleetSnapshot(t, dir, name)

	writeAndVerifyTestFile(t, dir, filepath)

	err := c.AllowSnapshots(dir)
	require.NoError(t, err)

	snapshotPath, err := c.CreateSnapshot(dir, name)
	require.NoError(t, err)

	err = c.Remove(filepath)
	require.NoError(t, err)

	_, err = c.Stat(filepath)
	assertPathError(t, err, "stat", filepath, os.ErrNotExist)

	st, err := c.Stat(path.Join(snapshotPath, filename))
	require.NoError(t, err)
	assert.Equal(t, int64(128), st.Size())
}

func TestDeleteSnapshot(t *testing.T) {
	c := getClientForSuperUser(t)
	baleetSnapshot(t, "/_test/deletesnaps", "snap")
	mkdirp(t, "/_test/deletesnaps")
	err := c.AllowSnapshots("/_test/deletesnaps")
	require.NoError(t, err)
	path, err := c.CreateSnapshot("/_test/deletesnaps", "snap")
	require.NoError(t, err)

	fs, err := c.Stat(path)
	assert.True(t, fs.IsDir())

	err = c.DeleteSnapshot("/_test/deletesnaps", "snap")
	require.NoError(t, err)

	_, err = c.Stat(path)
	assertPathError(t, err, "stat", path, os.ErrNotExist)
}
