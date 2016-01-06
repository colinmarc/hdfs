package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/golang/protobuf/proto"
)

var ErrEndOfBlock = errors.New("The amount of data to be written is more than is left in the block.")

// BlockWriter implements io.WriteCloser for writing a block to a datanode.
// Given a block location, it handles pipeline construction and failures,
// including communicating with the namenode if need be.
type BlockWriter struct {
	clientName string
	block      *hdfs.LocatedBlockProto
	blockSize  int64

	namenode *NamenodeConnection
	conn     net.Conn
	stream   *blockWriteStream
	offset   int64
	closed   bool
}

// NewBlockWriter returns a BlockWriter for the given block. It will lazily
// set up a replication pipeline, and connect to the "best" datanode based on
// any previously seen failures.
func NewBlockWriter(block *hdfs.LocatedBlockProto, namenode *NamenodeConnection, blockSize int64) *BlockWriter {
	s := &BlockWriter{
		clientName: namenode.ClientName(),
		block:      block,
		blockSize:  blockSize,
		namenode:   namenode,
	}

	return s
}

// Write implements io.Writer.
//
// In the case that a failure (such as a timeout) occurs while reading, the
// BlockWriter will transparently set up a new pipeline, and start writing to
// that.
//
// Any datanode failures are recorded in a global cache, so subsequent reads,
// even reads for different blocks, will prioritize them lower.
func (bw *BlockWriter) Write(b []byte) (int, error) {
	var blockFull bool
	if bw.offset >= bw.blockSize {
		return 0, ErrEndOfBlock
	} else if (bw.offset + int64(len(b))) > bw.blockSize {
		blockFull = true
		b = b[:bw.blockSize-bw.offset]
	}

	if bw.stream == nil {
		err := bw.connectNext()
		// TODO: handle failures, set up recovery pipeline
		if err != nil {
			return 0, err
		}
	}

	// TODO: handle failures, set up recovery pipeline
	n, err := bw.stream.Write(b)
	bw.offset += int64(n)
	if err == nil && blockFull {
		err = ErrEndOfBlock
	}

	return n, err
}

// Close implements io.Closer. It flushes any unwritten packets out to the
// datanode, and sends a final packet indicating the end of the block.
func (bw *BlockWriter) Close() error {
	bw.closed = true
	if bw.conn != nil {
		defer bw.conn.Close()
	}

	if bw.stream != nil {
		// TODO: handle failures, set up recovery pipeline
		err := bw.stream.finish()
		if err != nil {
			return err
		}

		// We need to tell the namenode what the final block length is.
		err = bw.finalizeBlock(bw.offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bw *BlockWriter) connectNext() error {
	address := getDatanodeAddress(bw.currentPipeline()[0])

	conn, err := net.DialTimeout("tcp", address, connectTimeout)
	if err != nil {
		return err
	}

	err = bw.writeBlockWriteRequest(conn)
	if err != nil {
		return err
	}

	resp, err := readBlockOpResponse(conn)
	if err != nil {
		return err
	} else if resp.GetStatus() != hdfs.Status_SUCCESS {
		return fmt.Errorf("Error from datanode: %s (%s)", resp.GetStatus().String(), resp.GetMessage())
	}

	bw.conn = conn
	bw.stream = newBlockWriteStream(conn)
	return nil
}

func (bw *BlockWriter) currentPipeline() []*hdfs.DatanodeInfoProto {
	// TODO: we need to be able to reconfigure the pipeline when a node fails.
	//
	// targets := make([]*hdfs.DatanodeInfoProto, 0, len(br.pipeline))
	// for _, loc := range s.block.GetLocs() {
	// 	addr := getDatanodeAddress(loc)
	// 	for _, pipelineAddr := range br.pipeline {
	// 		if ipAddr == addr {
	// 			append(targets, loc)
	// 		}
	// 	}
	// }
	//
	// return targets

	return bw.block.GetLocs()
}

func (bw *BlockWriter) currentStage() hdfs.OpWriteBlockProto_BlockConstructionStage {
	// TODO: this should be PIPELINE_SETUP_STREAMING_RECOVERY for recovery
	return hdfs.OpWriteBlockProto_PIPELINE_SETUP_CREATE
}

func (bw *BlockWriter) generationTimestamp() int64 {
	// TODO: ???
	return 0
}

func (bw *BlockWriter) finalizeBlock(length int64) error {
	bw.block.GetB().NumBytes = proto.Uint64(uint64(length))
	updateReq := &hdfs.UpdateBlockForPipelineRequestProto{
		Block:      bw.block.GetB(),
		ClientName: proto.String(bw.clientName),
	}
	updateResp := &hdfs.UpdateBlockForPipelineResponseProto{}

	err := bw.namenode.Execute("updateBlockForPipeline", updateReq, updateResp)
	if err != nil {
		return err
	}

	return nil
}

func (bw *BlockWriter) writeBlockWriteRequest(w io.Writer) error {
	targets := bw.currentPipeline()[1:]

	op := &hdfs.OpWriteBlockProto{
		Header: &hdfs.ClientOperationHeaderProto{
			BaseHeader: &hdfs.BaseHeaderProto{
				Block: bw.block.GetB(),
				Token: bw.block.GetBlockToken(),
			},
			ClientName: proto.String(bw.clientName),
		},
		Targets:               targets,
		Stage:                 bw.currentStage().Enum(),
		PipelineSize:          proto.Uint32(uint32(len(targets))),
		MinBytesRcvd:          proto.Uint64(bw.block.GetB().GetNumBytes()),
		MaxBytesRcvd:          proto.Uint64(uint64(bw.offset)), // I don't understand these two fields
		LatestGenerationStamp: proto.Uint64(uint64(bw.generationTimestamp())),
		RequestedChecksum: &hdfs.ChecksumProto{
			Type:             hdfs.ChecksumTypeProto_CHECKSUM_CRC32.Enum(),
			BytesPerChecksum: proto.Uint32(outboundChunkSize),
		},
	}

	return writeBlockOpRequest(w, writeBlockOp, op)
}
