package rpc

import (
	"fmt"
	"regexp"
	"strings"

	hadoop "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_common"
)

const (
	// qopAuthenication is how the namenode refers to authentication mode, which
	// only establishes mutual authentication without encryption (the default).
	qopAuthentication = "auth"
	// qopIntegrity is how the namenode refers to integrity mode, which, in
	// in addition to authentication, verifies the signature of RPC messages.
	qopIntegrity = "auth-int"
	// qopPrivacy is how the namenode refers to privacy mode, which, in addition
	// to authentication and integrity, provides full end-to-end encryption for
	// RPC messages.
	qopPrivacy = "auth-conf"
)

var challengeRegexp = regexp.MustCompile(",?([a-zA-Z0-9]+)=(\"([^\"]+)\"|([^,]+)),?")

type tokenChallenge struct {
	realm     string
	nonce     string
	qop       string
	charset   string
	cipher    []string
	algorithm string
}

func parseChallenge(challenge []byte) (*tokenChallenge, error) {
	tokenChallenge := tokenChallenge{}

	matched := challengeRegexp.FindAllSubmatch(challenge, -1)
	if matched == nil {
		return nil, fmt.Errorf("invalid token challenge: %s", challenge)
	}

	for _, m := range matched {
		key := string(m[1])
		val := string(m[3])
		switch key {
		case "realm":
			tokenChallenge.realm = val
		case "nonce":
			tokenChallenge.nonce = val
		case "qop":
			tokenChallenge.qop = val
		case "charset":
			tokenChallenge.charset = val
		case "cipher":
			tokenChallenge.cipher = strings.Split(val, ",")
		case "algorithm":
			tokenChallenge.algorithm = val
		default:
		}
	}

	return &tokenChallenge, nil
}

// parseChallengeAuth returns a tokenChallenge parsed from a challenge response from
// the namenode.
func parseChallengeAuth(auth *hadoop.RpcSaslProto_SaslAuth) (*tokenChallenge, error) {
	return parseChallenge(auth.Challenge)
}

type cipherType uint8

const (
	cipherUnknown cipherType = 0
	cipherDES     cipherType = 1 << iota
	cipher3DES
	cipherRC4
	cipherRC440
	cipherRC456
	cipherAESCBC
)

func (c cipherType) String() string {
	switch c {
	case cipherDES:
		return "des"
	case cipher3DES:
		return "3des"
	case cipherRC4:
		return "rc4"
	case cipherRC440:
		return "rc4-40"
	case cipherRC456:
		return "rc4-56"
	case cipherAESCBC:
		return "aes-cbc"
	}
	return ""
}

func getCipher(s string) cipherType {
	switch s {
	case "des":
		return cipherDES
	case "3des":
		return cipher3DES
	case "rc4":
		return cipherRC4
	case "rc4-40":
		return cipherRC440
	case "rc4-56":
		return cipherRC456
	case "aes-cbc":
		return cipherAESCBC
	}
	return 0
}

func chooseCipher(cipherOpts []string) cipherType {
	var avail cipherType
	for _, c := range cipherOpts {
		avail |= getCipher(c)
	}

	if avail&cipherRC4 != 0 {
		return cipherRC4
	}
	// if Has(avail, cipher3DES) {
	// 	return cipher3DES
	// }
	if avail&cipherRC456 != 0 {
		return cipherRC456
	}
	if avail&cipherRC440 != 0 {
		return cipherRC440
	}
	// if Has(avail, cipherDES) {
	// 	return cipherDES
	// }
	return 0
}
