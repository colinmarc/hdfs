package hdfs

import (
	"errors"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
)

var StatFsError = errors.New("Failed to get HDFS usage")

// FsInfo provides information about HDFS
type FsInfo struct {
	capacity              uint64
	used                  uint64
	remaining             uint64
	underReplicated       uint64
	corruptBlocks         uint64
	missingBlocks         uint64
	missingReplOneBlocks  uint64
	blocksInFuture        uint64
	pendingDeletionBlocks uint64
}

func (c *Client) StatFs() (FsInfo, error) {
	fs, err := c.getFsInfo()
	if err != nil {
		err = StatFsError
	}

	return fs, err
}

func (c *Client) getFsInfo() (FsInfo, error) {
	req  := &hdfs.GetFsStatusRequestProto{}
	resp := &hdfs.GetFsStatsResponseProto{}

	err := c.namenode.Execute("getFsStats", req, resp)
	if err != nil {
		return FsInfo{}, err
	}

	var fs FsInfo
	fs.capacity              = resp.GetCapacity()
	fs.used                  = resp.GetUsed()
	fs.remaining             = resp.GetRemaining()
	fs.underReplicated       = resp.GetUnderReplicated()
	fs.corruptBlocks         = resp.GetCorruptBlocks()
	fs.missingBlocks         = resp.GetMissingBlocks()
	fs.missingReplOneBlocks  = resp.GetMissingReplOneBlocks()
	fs.blocksInFuture        = resp.GetBlocksInFuture()
	fs.pendingDeletionBlocks = resp.GetPendingDeletionBlocks()

	return fs, nil
}

func (fs *FsInfo) Capacity() uint64 {
	return fs.capacity
}

func (fs *FsInfo) Used() uint64 {
	return fs.used
}

func (fs *FsInfo) Remaining() uint64 {
	return fs.remaining
}
