package rpc

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
	"github.com/golang/protobuf/proto"
	"gopkg.in/jcmturner/gokrb5.v5/crypto"
	"gopkg.in/jcmturner/gokrb5.v5/gssapi"
	"gopkg.in/jcmturner/gokrb5.v5/iana/keyusage"
	krbtypes "gopkg.in/jcmturner/gokrb5.v5/types"
)

// To check interface validity
var rpcWriter RpcWriter = &BasicRpcWriter{}
var rpcReader RpcReader = &BasicRpcReader{}
var saslRpcReader RpcReader = &SaslRpcReader{}

// RpcWriter is an interface for sending RPC payload
type RpcWriter interface {
	WriteRequest(w io.Writer, method string, requestID int32, req proto.Message) error
}

// RpcReader is an interface for receiving RPC payload
type RpcReader interface {
	ReadResponse(r io.Reader, method string, requestID int32, resp proto.Message) error
}

// BasicRpcWriter is a basic RPC writer
type BasicRpcWriter struct {
	// ClientID is the client ID of this writer
	ClientID []byte
}

// BasicRpcReader is a basic RPC reader
type BasicRpcReader struct {
}

// SaslRpcReader is a RPC reader which wrap payload with SASL
type SaslRpcReader struct {
	// SessionKey is a encryption key used to decrypt payload
	SessionKey krbtypes.EncryptionKey
	// Confidentiality is a flag of message encryption
	Confidentiality bool
}

// WriteRequest writes a request message
//
// A request packet:
// +-----------------------------------------------------------+
// |  uint32 length of the next three parts                    |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + RequestHeaderProto                       |
// +-----------------------------------------------------------+
// |  varint length + Request                                  |
// +-----------------------------------------------------------+
func (writer *BasicRpcWriter) WriteRequest(w io.Writer, method string, requestID int32, req proto.Message) error {
	rrh := newRPCRequestHeader(requestID, writer.ClientID)
	rh := newRequestHeader(method)

	reqBytes, err := makeRPCPacket(rrh, rh, req)
	if err != nil {
		return err
	}

	_, err = w.Write(reqBytes)
	return err
}

// ReadResponse reads a response message
//
// A response from the namenode:
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + Response                                 |
// +-----------------------------------------------------------+
func (reader *BasicRpcReader) ReadResponse(r io.Reader, method string, requestID int32, resp proto.Message) error {
	rrh := &hadoop.RpcResponseHeaderProto{}
	err := readRPCPacket(r, rrh, resp)
	if err != nil {
		return err
	} else if int32(rrh.GetCallId()) != requestID {
		return errors.New("unexpected sequence number")
	} else if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return &NamenodeError{
			method:    method,
			message:   rrh.GetErrorMsg(),
			code:      int(rrh.GetErrorDetail()),
			exception: rrh.GetExceptionClassName(),
		}
	}

	return nil
}

// ReadResponse reads a response message wrapped by SASL
//
// A response from the namenode:
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcResponseHeaderProto                   |
// +-----------------------------------------------------------+
// |  varint length + RpcSaslProto                             |
// |       +---------------------------------------------------+
// |       |  uint32 length of the next two parts              |
// |       +---------------------------------------------------+
// |       |  varint length + RpcResponseHeaderProto           |
// |       +---------------------------------------------------+
// |       |  varint length + Response                         |
// +-----------------------------------------------------------+
func (reader *SaslRpcReader) ReadResponse(r io.Reader, method string, requestID int32, resp proto.Message) error {
	// read SASL payload first
	rrh := &hadoop.RpcResponseHeaderProto{}
	sasl := &hadoop.RpcSaslProto{}
	err := readRPCPacket(r, rrh, sasl)
	if err != nil {
		return err
	} else if sasl.GetState() != hadoop.RpcSaslProto_WRAP {
		return fmt.Errorf("unexpected SASL state: %s", sasl.GetState().String())
	}

	// unwrap the token
	var wrapToken gssapi.WrapToken
	err = wrapToken.Unmarshal(sasl.GetToken(), true)
	if err != nil {
		return err
	}

	if reader.Confidentiality {
		// decrypt the payload
		decrypted, err := crypto.DecryptMessage(wrapToken.Payload, reader.SessionKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
		if err != nil {
			return err
		}
		// read the decrypted message as a response
		err = readRPCPacket(bytes.NewReader(decrypted), rrh, resp)
		if err != nil {
			return err
		}
	} else {
		// verify checksum
		_, err = wrapToken.VerifyCheckSum(reader.SessionKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
		if err != nil {
			return fmt.Errorf("invalid server token: %s", err)
		}
		// read the original message as a response
		err = readRPCPacket(bytes.NewReader(wrapToken.Payload), rrh, resp)
		if err != nil {
			return err
		}
	}
	if int32(rrh.GetCallId()) != requestID {
		return errors.New("unexpected sequence number")
	} else if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return &NamenodeError{
			method:    method,
			message:   rrh.GetErrorMsg(),
			code:      int(rrh.GetErrorDetail()),
			exception: rrh.GetExceptionClassName(),
		}
	}
	return nil
}
