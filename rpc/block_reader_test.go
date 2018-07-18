package rpc

import (
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"testing"
	"testing/iotest"
	"time"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var cachedNamenode *NamenodeConnection

func getUsername(*testing.T) (string, error) {
	username := os.Getenv("HADOOP_USER_NAME")
	if username != "" {
		return username, nil
	}
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}
func getNamenode(t *testing.T) *NamenodeConnection {
	if cachedNamenode != nil {
		return cachedNamenode
	}

	nn := os.Getenv("HADOOP_NAMENODE")
	if nn == "" {
		t.Fatal("HADOOP_NAMENODE not set")
	}

	username, err := getUsername(t)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := NewNamenodeConnection(nn, username)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func getBlocks(t *testing.T, name string) []*hdfs.LocatedBlockProto {
	namenode := getNamenode(t)

	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String(name),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(1257276),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err := namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		t.Fatal(err)
	}

	// add a duplicate location to failover to
	return resp.GetLocations().GetBlocks()
}

func getBlockReader(t *testing.T, name string) (*BlockReader, string) {
	// clear the failure cache
	datanodeFailures = make(map[string]time.Time)
	block := getBlocks(t, name)[0]

	br := NewBlockReader(block, 0, "test-"+string(newClientID()))
	dn := br.datanodes.datanodes[0]
	err := br.connectNext()
	require.NoError(t, err)

	return br, dn
}

func TestReadFailsOver(t *testing.T) {
	br, dn := getBlockReader(t, "/_test/mobydick.txt")
	datanodes := br.datanodes.numRemaining()
	if datanodes < 2 {
		t.Skip("not enough datanodes to test failover")
	}

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)

	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.EqualValues(t, 1048576, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
	assert.EqualValues(t, datanodes-1, br.datanodes.numRemaining())

	_, exist := datanodeFailures[dn]
	assert.True(t, exist)
}

func TestReadFailsOverMidRead(t *testing.T) {
	br, dn := getBlockReader(t, "/_test/mobydick.txt")
	datanodes := br.datanodes.numRemaining()
	if datanodes < 2 {
		t.Skip("not enough datanodes to test failover")
	}

	hash := crc32.NewIEEE()
	_, err := io.CopyN(hash, br, 10000)
	require.NoError(t, err)

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)

	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.EqualValues(t, 1048576-10000, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
	assert.EqualValues(t, datanodes-1, br.datanodes.numRemaining())

	_, exist := datanodeFailures[dn]
	assert.True(t, exist)
}

func TestReadFailsOverAndThenDies(t *testing.T) {
	br, _ := getBlockReader(t, "/_test/mobydick.txt")
	datanodes := br.datanodes.numRemaining()

	for br.datanodes.numRemaining() > 0 {
		br.stream.reader = iotest.TimeoutReader(br.stream.reader)
		_, err := io.CopyN(ioutil.Discard, br, 1000)
		require.NoError(t, err)
		require.Equal(t, datanodes-1, br.datanodes.numRemaining())
		datanodes--
	}

	br.stream.reader = iotest.TimeoutReader(br.stream.reader)
	_, err := io.Copy(ioutil.Discard, br)
	assert.EqualValues(t, iotest.ErrTimeout, err)
}
