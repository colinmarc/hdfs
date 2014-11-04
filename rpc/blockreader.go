package rpc

import (
	"fmt"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"io"
	"time"
)

// a global map of address -> last failure
var datanodeFailures = make(map[string]time.Time)

// BlockReader implements io.ReadCloser, for reading a block. It abstracts over
// reading from multiple datanodes, in order to be robust to failures.
type BlockReader struct {
	block *hdfs.LocatedBlockProto

	datanodes       []string
	lastError       error

	stream *blockStream
	offset uint64
	closed bool
}

// NewBlockReader returns a new BlockReader, given the block information and
// security token from the namenode. It will connect (lazily) to one of the
// provided datanode locations based on which datanodes have seen failures.
func NewBlockReader(block *hdfs.LocatedBlockProto, offset uint64) *BlockReader {
	locs := block.GetLocs()
	datanodes := make([]string, len(locs))
	for i, loc := range locs {
		dn := loc.GetId()
		datanodes[i] = fmt.Sprintf("%s:%d", dn.GetIpAddr(), dn.GetXferPort())
	}

	return &BlockReader{
		block:     block,
		datanodes: datanodes,
		offset:    offset,
	}
}

// connectNext pops a datanode from the list based on previous failures, and
// connects to it.
func (br *BlockReader) connectNext() error {
	address := br.nextDatanode()
	stream, err := newBlockStream(address, br.block, br.offset)
	if err != nil {
		return err
	}

	br.stream = stream
	return nil
}

func (br *BlockReader) nextDatanode() string {
	var picked int = -1
	var oldestFailure time.Time

	for i, address := range br.datanodes {
		failedAt, hasFailed := datanodeFailures[address]

		if !hasFailed {
			picked = i
			break
		} else if oldestFailure.IsZero() || failedAt.Before(oldestFailure) {
			picked = i
			oldestFailure = failedAt
		}
	}

	address := br.datanodes[picked]
	br.datanodes = append(br.datanodes[:picked], br.datanodes[picked+1:]...)

	return address
}

func (br *BlockReader) recordFailure(err error) {
	datanodeFailures[br.stream.address] = time.Now()
	br.lastError = err
	br.stream = nil
}

// Read implements io.Reader.
//
// In the case that a failure (such as a disconnect) occurs while reading, the
// BlockReader will failover to another datanode and continue reading
// transparently. In the case that all the datanodes fail, the error
// from the most recent attempt will be returned.
//
// Any datanode failures are recorded in a global cache, so subsequent reads,
// even reads for different blocks, will prioritize them lower.
func (br *BlockReader) Read(b []byte) (int, error) {
	if br.closed {
		return 0, io.ErrClosedPipe
	} else if br.offset >= br.block.GetB().GetNumBytes() {
		br.Close()
		return 0, io.EOF
	}

	// the main retry loop
	for br.stream != nil || len(br.datanodes) > 0 {
		if br.stream == nil {
			err := br.connectNext()
			if err != nil {
				br.recordFailure(err)
				continue
			}
		}

		n, err := br.stream.Read(b)
		if err != nil && err != io.EOF {
			br.recordFailure(err)
			if n > 0 {
				br.offset += uint64(n)
				return n, nil
			} else {
				continue
			}
		}

		return n, err
	}

	return 0, br.lastError
}

// Close implements io.Closer.
func (br *BlockReader) Close() error {
	br.closed = true

	if br.stream != nil {
		br.stream.Close()
	}

	return nil
}
