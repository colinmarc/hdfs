package hdfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitTruncate(t *testing.T, client *Client, name string, expectedSize int64) {
	for i := 0; i < 5; i++ {
		stat, err := client.Stat(name)
		require.NoError(t, err)

		if stat.Size() == expectedSize {
			assert.EqualValues(t, expectedSize, stat.Size())
			break
		}

		time.Sleep(500 * time.Millisecond)
		t.Log("Waiting for truncate to finish")
	}
}

func TestTruncate(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/truncate/1.txt")
	mkdirp(t, "/_test/truncate")
	f, err := client.Create("/_test/truncate/1.txt")
	require.NoError(t, err)

	n, err := f.Write([]byte("foobar\nfoobar\n"))
	assert.Equal(t, 14, n)
	require.NoError(t, err)

	assertClose(t, f)

	err = client.Truncate("/_test/truncate/1.txt", 4)

	if pe, ok := err.(*os.PathError); ok && pe.Err == ErrTruncateAsync {
		waitTruncate(t, client, "/_test/truncate/1.txt", 4)
	} else {
		require.NoError(t, err)
	}
}

func TestTruncateToZero(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/truncate/2.txt")
	mkdirp(t, "/_test/truncate")
	f, err := client.Create("/_test/truncate/2.txt")
	require.NoError(t, err)

	n, err := f.Write([]byte("foobarbaz"))
	assert.Equal(t, 9, n)
	require.NoError(t, err)

	assertClose(t, f)

	err = client.Truncate("/_test/truncate/2.txt", 0)

	if err == ErrTruncateAsync {
		waitTruncate(t, client, "/_test/truncate/2.txt", 0)
	} else {
		require.NoError(t, err)
	}
}

func TestTruncateSizeTooBig(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/truncate/3.txt")
	mkdirp(t, "/_test/truncate")

	f, err := client.Create("/_test/truncate/3.txt")
	require.NoError(t, err)

	n, err := f.Write([]byte("foo"))
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	assertClose(t, f)

	err = client.Truncate("/_test/truncate/3.txt", 100)
	assertPathError(t, err, "truncate", "/_test/truncate/3.txt", os.ErrInvalid)
}

func TestTruncateSizeNegative(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/truncate/4.txt")
	mkdirp(t, "/_test/truncate")

	f, err := client.Create("/_test/truncate/4.txt")
	require.NoError(t, err)

	n, err := f.Write([]byte("foo"))
	assert.Equal(t, 3, n)
	require.NoError(t, err)

	assertClose(t, f)

	err = client.Truncate("/_test/truncate/4.txt", -10)
	assertPathError(t, err, "truncate", "/_test/truncate/4.txt", os.ErrInvalid)
}

func TestTruncateNoExist(t *testing.T) {
	client := getClient(t)

	err := client.Truncate("/_test/nonexistent", 100)
	assertPathError(t, err, "truncate", "/_test/nonexistent", os.ErrNotExist)
}

func TestTruncateDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/truncate")

	err := client.Truncate("/_test/truncate", 100)
	assertPathError(t, err, "truncate", "/_test/truncate", os.ErrNotExist)
}

func TestTruncateWithoutPermission(t *testing.T) {
	client := getClient(t)
	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/truncate/5.txt")
	mkdirp(t, "/_test/truncate")

	f, err := client.Create("/_test/truncate/5.txt")
	require.NoError(t, err)

	n, err := f.Write([]byte("barbar"))
	assert.Equal(t, 6, n)
	require.NoError(t, err)

	assertClose(t, f)

	err = client2.Truncate("/_test/truncate/5.txt", 1)
	assertPathError(t, err, "truncate", "/_test/truncate/5.txt", os.ErrPermission)

	stat, err := client.Stat("/_test/truncate/5.txt")
	require.NoError(t, err)
	assert.EqualValues(t, 6, stat.Size())
}
