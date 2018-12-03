package hdfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	krb "gopkg.in/jcmturner/gokrb5.v5/client"
	"gopkg.in/jcmturner/gokrb5.v5/config"
	"gopkg.in/jcmturner/gokrb5.v5/credentials"
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

	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil || conf == nil {
		t.Fatal("Couldn't load ambient config", err, conf)
	}

	options := ClientOptionsFromConf(conf)
	if options.Addresses == nil {
		t.Fatal("Missing namenode addresses in ambient config")
	}

	if options.KerberosClient != nil {
		options.KerberosClient = getKerberosClient(t, username)
	} else {
		options.User = username
	}

	client, err := NewClient(options)
	if err != nil {
		t.Fatal(err)
	}

	cachedClients[username] = client
	return client
}

// getKerberosClient expects a ccache file for each user mentioned in the tests
// to live at /tmp/krb5cc_gohdfs_<username>, and krb5.conf to live at
// /etc/krb5.conf
func getKerberosClient(t *testing.T, username string) *krb.Client {
	cfg, err := config.Load("/etc/krb5.conf")
	if err != nil {
		t.Skip("Couldn't load krb config:", err)
	}

	ccache, err := credentials.LoadCCache(fmt.Sprintf("/tmp/krb5cc_gohdfs_%s", username))
	if err != nil {
		t.Skipf("Couldn't load keytab for user %s: %s", username, err)
	}

	client, err := krb.NewClientFromCCache(ccache)
	if err != nil {
		t.Fatal("Couldn't initialize krb client:", err)
	}

	return client.WithConfig(cfg)
}

func touch(t *testing.T, path string) {
	touchMask(t, path, 0)
}

func touchMask(t *testing.T, path string, mask os.FileMode) {
	c := getClient(t)

	err := c.RemoveAll(path)
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

	err := c.RemoveAll(path)
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

	err := c.RemoveAll(path)
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
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		t.Fatal("Couldn't load ambient config", err)
	}

	nns := conf.Namenodes()

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
