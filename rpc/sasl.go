package rpc

import (
	"encoding/binary"
	"io"
	"log"
	"strings"

	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	"gopkg.in/jcmturner/gokrb5.v3/client"
	"gopkg.in/jcmturner/gokrb5.v3/gssapi"
	"gopkg.in/jcmturner/gokrb5.v3/iana/keyusage"
	"gopkg.in/jcmturner/gokrb5.v3/types"
)

const saslRpcCallId int32 = -33

// Run a Kerberos handshake with an hadoop Namenode server that is expected to live on the other side
// of the connection.
// Assumes an HRPC header has already been sent.
func doKerberosHandshake(c *NamenodeConnection) {
	// Send the initial request for a SASL negotiation
	sendSASLNegotiationRequest(c)
	// The reply will contain a list of supported mechanisms.
	mechanisms := readFirstReply(c)
	// Check that kerberos is supported and extract the mechanism
	kerbMech := getKerberosAuthMech(mechanisms)
	if kerbMech == nil {
		log.Fatalf("Kerberos is not a supported auth mechanism. Supported mechanisms: %+v", mechanisms)
	}

	sessionKey := sendInitialToken(c, kerbMech)

	replyToken := readAuthReply(c)

	verifyChecksum(replyToken, sessionKey)

	// Contains flags pertaining to the session config (protection level, etc)
	// TODO: run checks on this to make sure we support what is required.
	// Needs to be sent back to the server, signed with the session key.
	toSendBack, err := gssapi.NewInitiatorToken(replyToken.Payload, sessionKey)

	if err != nil {
		log.Panic("could not build an initiator token", err)
	}

	responseBytes, marshalErr := toSendBack.Marshal()

	if marshalErr != nil {
		log.Panic("failed to marshal outbound token.", marshalErr)
	}

	sendChallengeResponse(c, responseBytes)
	readChallengeReply(c)
}

// searches and returns the kerberos auth mechanism from the passed mechanisms.
// nil is returned if the KERBEROS mechanism is not found.
func getKerberosAuthMech(mechanisms []*hadoop.RpcSaslProto_SaslAuth) *hadoop.RpcSaslProto_SaslAuth {
	for _, mech := range mechanisms {
		if *mech.Method == "KERBEROS" {
			return mech
		}
	}
	return nil
}

// Computes the checksum for the passed token and compares it to the checksum contained in the token.
func verifyChecksum(challenge gssapi.WrapToken, key types.EncryptionKey) {
	ok, err := challenge.VerifyCheckSum(key, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		log.Panic(err)
	}
	if !ok {
		log.Panic("checksum mismatch")
	}
}

func newSASLMessageHeader(clientId []byte) *hadoop.RpcRequestHeaderProto {
	return newRPCRequestHeader(int(saslRpcCallId), clientId)
}

func sendSASLNegotiationRequest(c *NamenodeConnection) {
	protoState := hadoop.RpcSaslProto_NEGOTIATE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
		})

	if err != nil {
		log.Fatal(err)
	}

	c.conn.Write(pkt)
}

/**
 * Returns a list of suported auth mechanisms
 */
func readFirstReply(c *NamenodeConnection) []*hadoop.RpcSaslProto_SaslAuth {
	return readSaslResponse(c).GetAuths()
}

/**
 * Sends out a token to the service to initiate a kerberos/gssapi handshake.
 * Returns the session key that will be used to authenticate or encrypt the handshake tokens.
 */
func sendInitialToken(c *NamenodeConnection, mechanism *hadoop.RpcSaslProto_SaslAuth) types.EncryptionKey {

	token, sessionKey := getAPR(
		c.kerberosClient,
		c.servicePrincipalName,
		// Get rid of the potential port contained in the address string
		strings.Split(c.host.address, ":")[0])

	protoState := hadoop.RpcSaslProto_INITIATE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
			Token: token.MechToken,
			Auths: []*hadoop.RpcSaslProto_SaslAuth{mechanism},
		})

	if err != nil {
		log.Panic(err)
	}

	c.conn.Write(pkt)

	return sessionKey
}

/**
 * Returns a list of supported auth mechanisms
 */
func readAuthReply(c *NamenodeConnection) gssapi.WrapToken {

	resp := readSaslResponse(c)

	if resp.GetState() != hadoop.RpcSaslProto_CHALLENGE {
		log.Panicf("expected Sasl state CHALLENGE. Was: %v", resp.GetState())
	}
	var token gssapi.WrapToken

	if err := token.Unmarshal(resp.GetToken(), true); err != nil {
		log.Panic(err)
	}

	return token
}

func sendChallengeResponse(c *NamenodeConnection, challenge []byte) {

	protoState := hadoop.RpcSaslProto_RESPONSE

	pkt, err := makeRPCPacket(
		newSASLMessageHeader(c.clientId),
		&hadoop.RpcSaslProto{
			State: &protoState,
			Token: challenge,
		})

	if err != nil {
		log.Fatal(err)
	}

	c.conn.Write(pkt)
}

func readChallengeReply(c *NamenodeConnection) {

	resp := readSaslResponse(c)

	if resp.GetState() != hadoop.RpcSaslProto_SUCCESS {
		log.Panicf("server returned handshake failure: %+v", resp.GetState())
	}
}

func readSaslResponse(c *NamenodeConnection) *hadoop.RpcSaslProto {
	var packetLength uint32
	err := binary.Read(c.conn, binary.BigEndian, &packetLength)
	if err != nil {
		log.Fatal(err)
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(c.conn, packet)
	if err != nil {
		log.Fatal(err)
	}

	rrh := &hadoop.RpcResponseHeaderProto{}
	resp := &hadoop.RpcSaslProto{}
	err = readRPCPacket(packet, rrh, resp)

	if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		log.Panicf("failed to read response: %s", rrh.GetStatus().String())
	}
	return resp
}

func getAPR(krbClient *client.Client, serviceName string, serviceHost string) (negToken gssapi.NegTokenInit, sessionKey types.EncryptionKey) {

	tkt, key, tktE := krbClient.GetServiceTicket(serviceName + "/" + serviceHost)

	if tktE != nil {
		log.Panic(tktE)
	}

	token, tokenE := gssapi.NewNegTokenInitKrb5(*krbClient.Credentials, tkt, key)

	if tokenE != nil {
		log.Panic(tokenE)
	}

	return token, key
}
