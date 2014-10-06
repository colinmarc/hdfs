package rpc

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"crypto/rand"
	"encoding/binary"
	"errors"
	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"io"
)

const (
	rpcVersion           = 0x09
	serviceClass         = 0x0
	authProtocol         = 0x0
	protocolClass        = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
	protocolClassVersion = 1
	handshakeCallId      = -3
)

var clientId = randomClientId()

// A request packet:
// +---------------------------------------------------------------------+
// |  uint32 length of the next three parts                              |
// +---------------------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                              |
// +---------------------------------------------------------------------+
// |  varint length + RequestHeaderProto                                 |
// +---------------------------------------------------------------------+
// |  varint length + Request                                            |
// +---------------------------------------------------------------------+
func makeRequest(callId int, method string, msg proto.Message) ([]byte, error) {
	rrh, err := makeRpcRequestHeader(callId)
	if err != nil {
		return nil, err
	}

	rh, err := makeRequestHeader(method)
	if err != nil {
		return nil, err
	}

	req, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	rrhLength := proto.EncodeVarint(uint64(len(rrh)))
	rhLength := proto.EncodeVarint(uint64(len(rh)))
	reqLength := proto.EncodeVarint(uint64(len(req)))

	lengthTotal := len(rrhLength) + len(rrh) +
		len(rhLength) + len(rh) +
		len(reqLength) + len(req)

	packetLength := make([]byte, 4)
	binary.BigEndian.PutUint32(packetLength, uint32(lengthTotal))

	buf := new(bytes.Buffer)
	buf.Grow(lengthTotal + 4)
	buf.Write(packetLength)
	buf.Write(rrhLength)
	buf.Write(rrh)
	buf.Write(rhLength)
	buf.Write(rh)
	buf.Write(reqLength)
	buf.Write(req)

	return buf.Bytes(), nil
}

// A response from the namenode:
// +-----------------------------------------------------------+
// |  Length of the RPC resonse (4 bytes/32 bit int)           |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + Response                                 |
// +-----------------------------------------------------------+
func readResponse(callId int, reader io.Reader, msg proto.Message) error {
	var responseLength uint32
	err := binary.Read(reader, binary.BigEndian, &responseLength)
	if err != nil {
		return err
	}

	response := make([]byte, responseLength)
	_, err = reader.Read(response)
	if err != nil {
		return err
	}

	rrhLength, n := proto.DecodeVarint(response)
	if n == 0 {
		return errors.New("Error reading response: unexpected EOF")
	}

	rrh := &hadoop.RpcResponseHeaderProto{}
	err = proto.Unmarshal(response[n:int(rrhLength)+n], rrh)
	if err != nil {
		return err
	} else if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return errors.New("TODO failed rpc call")
	} else if int(rrh.GetCallId()) != callId {
		return errors.New("Error reading response: unexpected sequence number")
	}

	response = response[int(rrhLength)+n:]
	msgLength, n := proto.DecodeVarint(response)
	if n == 0 {
		return errors.New("Error reading response: unexpected EOF")
	}

	err = proto.Unmarshal(response[n:int(msgLength)+n], msg)
	if err != nil {
		return err
	}

	return nil
}

// A handshake packet:
// +---------------------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                           |
// +---------------------------------------------------------------------+
// |  Version, 1 byte (default verion 9)                                 |
// +---------------------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                                   |
// +---------------------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x0)                     |
// +---------------------------------------------------------------------+
// |  uint32 length of the next two parts                                |
// +---------------------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                              |
// +---------------------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                          |
// +---------------------------------------------------------------------+
func makeConnectionHandshake(user string) ([]byte, error) {
	rpcHeader := []byte{
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		rpcVersion, serviceClass, authProtocol,
	}

	buf := bytes.NewBuffer(rpcHeader)

	rrh, err := makeRpcRequestHeader(handshakeCallId)
	if err != nil {
		return nil, err
	}

	cc, err := makeConnectionContext(user)
	if err != nil {
		return nil, err
	}

	rrhLength := proto.EncodeVarint(uint64(len(rrh)))
	ccLength := proto.EncodeVarint(uint64(len(cc)))

	lengthTotal := len(rrhLength) + len(rrh) + len(ccLength) + len(cc)
	packetLength := make([]byte, 4)
	binary.BigEndian.PutUint32(packetLength, uint32(lengthTotal))

	buf.Grow(lengthTotal + 4)
	buf.Write(packetLength)
	buf.Write(rrhLength)
	buf.Write(rrh)
	buf.Write(ccLength)
	buf.Write(cc)

	return buf.Bytes(), nil
}

func makeRpcRequestHeader(callId int) ([]byte, error) {
	rrh := &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(int32(callId)),
		ClientId: clientId,
	}

	return proto.Marshal(rrh)
}

func makeRequestHeader(methodName string) ([]byte, error) {
	rh := &hadoop.RequestHeaderProto{
		MethodName:                 proto.String(methodName),
		DeclaringClassProtocolName: proto.String(protocolClass),
		ClientProtocolVersion:      proto.Uint64(uint64(protocolClassVersion)),
	}

	return proto.Marshal(rh)
}

func makeConnectionContext(user string) ([]byte, error) {
	cc := &hadoop.IpcConnectionContextProto{
		UserInfo: &hadoop.UserInformationProto{
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protocolClass),
	}

	return proto.Marshal(cc)
}

func randomClientId() []byte {
	uuid := make([]byte, 16)
	rand.Read(uuid)

	return uuid
}
