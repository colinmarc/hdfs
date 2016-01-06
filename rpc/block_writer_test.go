package rpc

import (
	"hash/crc32"
	"io"
	"os"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createBlock(t *testing.T, name string) *BlockWriter {
	namenode := getNamenode(t)
	blockSize := int64(1048576)

	createReq := &hdfs.CreateRequestProto{
		Src:          proto.String(name),
		Masked:       &hdfs.FsPermissionProto{Perm: proto.Uint32(uint32(0644))},
		ClientName:   proto.String(namenode.ClientName()),
		CreateFlag:   proto.Uint32(1),
		CreateParent: proto.Bool(false),
		Replication:  proto.Uint32(uint32(3)),
		BlockSize:    proto.Uint64(uint64(blockSize)),
	}
	createResp := &hdfs.CreateResponseProto{}

	err := namenode.Execute("create", createReq, createResp)
	require.NoError(t, err)

	addBlockReq := &hdfs.AddBlockRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(namenode.ClientName()),
		Previous:   nil,
	}
	addBlockResp := &hdfs.AddBlockResponseProto{}

	err = namenode.Execute("addBlock", addBlockReq, addBlockResp)
	require.NoError(t, err)

	block := addBlockResp.GetBlock()
	return NewBlockWriter(block, namenode, blockSize)
}

func finishBlock(t *testing.T, name string, bw *BlockWriter) {
	namenode := getNamenode(t)

	err := bw.Close()
	require.NoError(t, err)

	completeReq := &hdfs.CompleteRequestProto{
		Src:        proto.String(name),
		ClientName: proto.String(namenode.ClientName()),
		Last:       bw.block.GetB(),
	}
	completeResp := &hdfs.CompleteResponseProto{}

	err = namenode.Execute("complete", completeReq, completeResp)
	require.NoError(t, err)
}

func baleet(t *testing.T, name string) {
	namenode := getNamenode(t)

	req := &hdfs.DeleteRequestProto{
		Src:       proto.String(name),
		Recursive: proto.Bool(true),
	}
	resp := &hdfs.DeleteResponseProto{}

	err := namenode.Execute("delete", req, resp)
	require.NoError(t, err)
	require.NotNil(t, resp.Result)
}

func TestWriteFailsOver(t *testing.T) {
	t.Skip("Write failover isn't implemented")

	name := "/_test/create/6.txt"
	baleet(t, name)

	mobydick, err := os.Open("../test/mobydick.txt")
	require.NoError(t, err)

	bw := createBlock(t, name)
	bw.connectNext()
	bw.stream.ackError = ackError{0, 0, hdfs.Status_ERROR}

	_, err = io.CopyN(bw, mobydick, 1048576)
	require.NoError(t, err)
	finishBlock(t, name, bw)

	br, _ := getBlockReader(t, name)
	hash := crc32.NewIEEE()
	n, err := io.Copy(hash, br)
	require.NoError(t, err)
	assert.EqualValues(t, 1048576, n)
	assert.EqualValues(t, 0xb35a6a0e, hash.Sum32())
}

func (s *blockWriteStream) flushInvalidSeqno(force bool) error {
	for s.buf.Len() > 0 && (force || s.buf.Len() >= outboundPacketSize) {
		packet := s.makePacket()
		s.packets <- packet
		s.offset += int64(len(packet.data))
		s.seqno++
		packet.seqno = + 10
		err := s.writePacket(packet)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestWriteRaceCondition(t *testing.T) {
	ok := false
	go func() {
		data := []byte("TestWriteRaceCondition")
		name := "/_test/TestWriteRaceCondition.txt"
		baleet(t, name)
		bw := createBlock(t, name)
		bw.connectNext()
		s := bw.stream
		require.NoError(t, s.ackError)
		n, _ := s.buf.Write(data)
		assert.True(t, n > 0)
		err := s.flushInvalidSeqno(true)
		require.NoError(t, err)
		time.Sleep(100 * time.Microsecond)
		for i := 0; i < 2 * maxPacketsInQueue; i++ {
			n, _ = s.buf.Write(data)
			assert.True(t, n > 0)
			err = s.flush(true)
			require.NoError(t, err)
		}
		ok = true
	}()
	time.Sleep(3 * time.Second)
	assert.True(t, ok)
}
