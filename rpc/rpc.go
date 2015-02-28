// Package rpc implements some of the lower-level functionality required to
// communicate with the namenode and datanodes.
package rpc

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
	"fmt"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"time"
)

// ClientName is passed into the namenode on requests, and identifies this
// client to the namenode.
const (
	ClientName          = "go-hdfs"
	dataTransferVersion = 0x1c
	writeBlockOp        = 0x50
	readBlockOp         = 0x51
	checksumBlockOp     = 0x55
)

var (
	connectTimeout  = 1 * time.Second
	namenodeTimeout = 3 * time.Second
	datanodeTimeout = 3 * time.Second
)

func makeDelimitedMsg(msg proto.Message) ([]byte, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, 10)
	n := binary.PutUvarint(lengthBytes, uint64(len(msgBytes)))
	return append(lengthBytes[:n], msgBytes...), nil
}

func makeRPCPacket(msgs ...proto.Message) ([]byte, error) {
	packet := make([]byte, 4, 128)

	length := 0
	for _, msg := range msgs {
		b, err := makeDelimitedMsg(msg)
		if err != nil {
			return nil, err
		}

		packet = append(packet, b...)
		length += len(b)
	}

	binary.BigEndian.PutUint32(packet, uint32(length))
	return packet, nil
}

// Doesn't include the uint32 length
func readRPCPacket(b []byte, msgs ...proto.Message) error {
	reader := bytes.NewReader(b)
	for _, msg := range msgs {
		msgLength, err := binary.ReadUvarint(reader)
		if err != nil {
			return err
		}

		if msgLength != 0 {
			msgBytes := make([]byte, msgLength)
			_, err = reader.Read(msgBytes)
			if err != nil {
				return err
			}

			err = proto.Unmarshal(msgBytes, msg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getDatanodeAddress(datanode *hdfs.DatanodeInfoProto) string {
	id := datanode.GetId()
	return fmt.Sprintf("%s:%d", id.GetIpAddr(), id.GetXferPort())
}
