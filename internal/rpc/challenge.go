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

// parseChallenge returns a tokenChallenge parsed from a challenge response from
// the namenode.
func parseChallenge(auth *hadoop.RpcSaslProto_SaslAuth) (*tokenChallenge, error) {
	tokenChallenge := tokenChallenge{}

	matched := challengeRegexp.FindAllSubmatch(auth.Challenge, -1)
	if matched == nil {
		return nil, fmt.Errorf("invalid token challenge: %s", auth.Challenge)
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
