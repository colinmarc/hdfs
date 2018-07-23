package hdfs

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var cachedClients = make(map[string]*Client)

func getClient(t *testing.T) *Client {
	return getClientForUser(t, "gohdfs1")
}

func getClientForSuperUser(t *testing.T) *Client {
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	return getClientForUser(t, u.Username)
}

func getClientForUser(t *testing.T, username string) *Client {
	if c, ok := cachedClients[username]; ok {
		return c
	}

	conf := LoadHadoopConf("")
	options, _ := ClientOptionsFromConf(conf)
	if options.Addresses == nil {
		t.Fatal("No hadoop configuration found at HADOOP_CONF_DIR")
	}

	options.User = username
	client, err := NewClient(options)
	if err != nil {
		t.Fatal(err)
	}

	cachedClients[username] = client
	return client
}

func touch(t *testing.T, path string) {
	touchMask(t, path, 0)
}

func touchMask(t *testing.T, path string, mask os.FileMode) {
	c := getClient(t)

	err := c.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = c.CreateEmptyFile(path)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}

	if mask != 0 {
		err = c.Chmod(path, mask)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func mkdirp(t *testing.T, path string) {
	mkdirpMask(t, path, 0755)
}

func mkdirpMask(t *testing.T, path string, mask os.FileMode) {
	c := getClient(t)

	err := c.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = c.MkdirAll(path, mask)
	if err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}
}

func baleet(t *testing.T, path string) {
	c := getClient(t)

	err := c.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func assertPathError(t *testing.T, err error, op, path string, wrappedErr error) {
	require.NotNil(t, err)

	expected := &os.PathError{op, path, wrappedErr}
	require.Equal(t, expected.Error(), err.Error())
	require.Equal(t, expected, err)
}

func TestNewWithMultipleNodes(t *testing.T) {
	conf := LoadHadoopConf("")
	nns, err := conf.Namenodes()
	if err != nil {
		t.Fatal("No hadoop configuration found at HADOOP_CONF_DIR")
	}

	nns = append([]string{"localhost:100"}, nns...)
	_, err = NewClient(ClientOptions{Addresses: nns, User: "gohdfs1"})
	assert.Nil(t, err)
}

func TestNewWithFailingNode(t *testing.T) {
	_, err := New("localhost:100")
	assert.NotNil(t, err)
}

func TestReadFile(t *testing.T) {
	client := getClient(t)

	bytes, err := client.ReadFile("/_test/foo.txt")
	assert.NoError(t, err)
	assert.EqualValues(t, "bar\n", string(bytes))
}

func TestCopyToLocal(t *testing.T) {
	client := getClient(t)

	dir, _ := ioutil.TempDir("", "hdfs-test")
	tmpfile := filepath.Join(dir, "foo.txt")
	err := client.CopyToLocal("/_test/foo.txt", tmpfile)
	require.NoError(t, err)

	f, err := os.Open(tmpfile)
	require.NoError(t, err)

	bytes, _ := ioutil.ReadAll(f)
	assert.EqualValues(t, "bar\n", string(bytes))
}

func TestCopyToRemote(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/copytoremote.txt")
	err := client.CopyToRemote("testdata/foo.txt", "/_test/copytoremote.txt")
	require.NoError(t, err)

	bytes, err := client.ReadFile("/_test/copytoremote.txt")
	require.NoError(t, err)

	assert.EqualValues(t, "bar\n", string(bytes))
}
