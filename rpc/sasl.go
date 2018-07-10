package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"gopkg.in/jcmturner/gokrb5.v4/client"
	"gopkg.in/jcmturner/gokrb5.v4/gssapi"
	"gopkg.in/jcmturner/gokrb5.v4/iana/keyusage"
	"gopkg.in/jcmturner/gokrb5.v4/types"
)

const saslRpcCallId int32 = -33

// doKerberosHandshake runs a HRPC compliant Kerberos handshake with an hadoop Namenode
// that is expected to live on the other side of the socket.
// Assumes an HRPC header has already been sent.
func doKerberosHandshake(c *NamenodeConnection) error {
	// Send the initial request for a SASL negotiation
	err := sendSASLNegotiationRequest(c)
	if err != nil {
		return err
	}
	// The reply will contain a list of supported mechanisms.
	mechanisms, err := readFirstReply(c)
	if err != nil {
		return err
	}
	// Check that kerberos is supported and extract the mechanism
	kerbMech := getKerberosAuthMech(mechanisms)
	if kerbMech == nil {
		return fmt.Errorf("kerberos is not a supported auth mechanism: supported mechanisms: %+v", mechanisms)
	}
	// Send the initial token of the authentication handshake
	sessionKey, err := sendInitialToken(c, kerbMech)
	if err != nil {
		return err
	}
	// Verify that the server was happy with the token
	replyToken, err := readAuthReply(c)
	if err != nil {
		return err
	}
	// Verify the checksum to authenticate the server
	err = verifyChecksum(replyToken, sessionKey)
	if err != nil {
		return err
	}
	// The payload contains flags pertaining to the session config (protection level, etc)
	// TODO: run checks on this to make sure we support what is required.
	// Needs to be sent back to the server, signed with the session key.
	err = sendChallengeResponse(c, replyToken.Payload, sessionKey)
	if err != nil {
		return err
	}
	err = readFinalReply(c)
	if err != nil {
		return err
	}
	return nil
}

// getKerberosAuthMech searches and returns the kerberos auth mechanism from the passed mechanisms.
// nil is returned if the KERBEROS mechanism is not found.
func getKerberosAuthMech(mechanisms []*hadoop.RpcSaslProto_SaslAuth) *hadoop.RpcSaslProto_SaslAuth {
	for _, mech := range mechanisms {
		if *mech.Method == "KERBEROS" {
			return mech
		}
	}
	return nil
}

// verifyChecksum computes the checksum for the passed token and compares it to the checksum contained in the token.
func verifyChecksum(challenge *gssapi.WrapToken, key *types.EncryptionKey) error {
	ok, err := challenge.VerifyCheckSum(*key, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return errors.New("checksum mismatch")
}

func newSASLMessageHeader(clientId []byte) *hadoop.RpcRequestHeaderProto {
	return newRPCRequestHeader(int(saslRpcCallId), clientId)
}

func sendSASLNegotiationRequest(c *NamenodeConnection) error {
	protoState := hadoop.RpcSaslProto_NEGOTIATE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
		})

	if err != nil {
		return err
	}

	c.conn.Write(pkt)
	return nil
}

// readFirstReply reads the server reply and returns the list of supported auth mechanisms
func readFirstReply(c *NamenodeConnection) ([]*hadoop.RpcSaslProto_SaslAuth, error) {
	authMechs, err := readSaslResponse(c)
	if err != nil {
		return nil, err
	}
	return authMechs.GetAuths(), nil
}

// sendInitialToken sends a token to the service to initiate a kerberos/gssapi handshake.
// Returns the session key that will be used to authenticate the handshake tokens.
func sendInitialToken(c *NamenodeConnection, mechanism *hadoop.RpcSaslProto_SaslAuth) (*types.EncryptionKey, error) {

	token, sessionKey, err := getAPR(
		c.kerberosClient,
		c.servicePrincipalName,
		// Get rid of the potential port contained in the address string
		strings.Split(c.host.address, ":")[0])

	if err != nil {
		return nil, err
	}

	protoState := hadoop.RpcSaslProto_INITIATE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
			Token: token.MechToken,
			Auths: []*hadoop.RpcSaslProto_SaslAuth{mechanism},
		})

	if err != nil {
		return nil, err
	}

	c.conn.Write(pkt)

	return sessionKey, nil
}

// readAuthReply reads the second reply from the server (after INITIATE was sent) and
// expects to find a WrapToken in it.
func readAuthReply(c *NamenodeConnection) (*gssapi.WrapToken, error) {

	resp, err := readSaslResponse(c)
	if err != nil {
		return nil, err
	}

	if resp.GetState() != hadoop.RpcSaslProto_CHALLENGE {
		return nil, fmt.Errorf("expected SASL state CHALLENGE. Was: %v", resp.GetState())
	}
	var token gssapi.WrapToken

	if err := token.Unmarshal(resp.GetToken(), true); err != nil {
		return nil, err
	}

	return &token, nil
}

// sendChallengeResponse sends out a reply containing the passed payload.
func sendChallengeResponse(c *NamenodeConnection, payload []byte, sessionKey *types.EncryptionKey) error {

	toSendBack, err := gssapi.NewInitiatorToken(payload, *sessionKey)
	if err != nil {
		return err
	}

	responseBytes, err := toSendBack.Marshal()
	if err != nil {
		return err
	}

	protoState := hadoop.RpcSaslProto_RESPONSE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
			Token: responseBytes,
		})

	if err != nil {
		return err
	}

	c.conn.Write(pkt)
	return nil
}

func readFinalReply(c *NamenodeConnection) error {
	resp, err := readSaslResponse(c)

	if err != nil {
		return err
	}

	if resp.GetState() == hadoop.RpcSaslProto_SUCCESS {
		return nil
	}
	return fmt.Errorf("server returned handshake failure: %+v", resp.GetState())
}

func readSaslResponse(c *NamenodeConnection) (*hadoop.RpcSaslProto, error) {
	var packetLength uint32
	err := binary.Read(c.conn, binary.BigEndian, &packetLength)
	if err != nil {
		return nil, err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(c.conn, packet)
	if err != nil {
		return nil, err
	}

	rrh := &hadoop.RpcResponseHeaderProto{}
	resp := &hadoop.RpcSaslProto{}
	err = readRPCPacket(packet, rrh, resp)

	if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return nil, fmt.Errorf("failed to read response: %s", rrh.GetStatus().String())
	}
	return resp, nil
}

// getAPR returns an initial kerberos negotiation token amd the session key to use for subsequent validation operations
func getAPR(krbClient *client.Client, serviceName string, serviceHost string) (negToken *gssapi.NegTokenInit, sessionKey *types.EncryptionKey, err error) {

	tkt, key, err := krbClient.GetServiceTicket(serviceName + "/" + serviceHost)

	if err != nil {
		return nil, nil, err
	}

	token, err := gssapi.NewNegTokenInitKrb5(*krbClient.Credentials, tkt, key)

	if err != nil {
		return nil, nil, err
	}

	return &token, &key, nil
}
