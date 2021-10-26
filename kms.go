package hdfs

import (
	"bytes"
	"crypto/aes"
	"io/ioutil"
	"math/rand"

	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	spnego "github.com/jcmturner/gokrb5/v8/spnego"
)

const (
	kmsSchemeHTTP  = "kms://http@"
	kmsSchemeHTTPS = "kms://https@"
)

func (c *Client) kmsAuth(url string) error {
	if c.options.KerberosClient == nil {
		url += ("&user.name=" + c.options.User)
	}

	req, err := http.NewRequest("OPTIONS", url, nil)
	if err != nil {
		return err
	}

	var resp *http.Response
	if c.options.KerberosClient != nil {
		kHttp := spnego.NewClient(c.options.KerberosClient, c.http, "")
		resp, err = kHttp.Do(req)
	} else {
		resp, err = c.http.Do(req)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad kms auth status: %v", resp.StatusCode)
	}
	return nil
}

// parse uri like kms://https@kms01.example.com;kms02.example.com:9600/kms
func kmsParseProviderUri(uri string) ([]string, error) {
	original_uri := uri

	if uri == "" {
		return nil, errors.New("KeyProviderUri empty. not configured on server ?")
	}

	var urls []string
	var proto string
	if strings.HasPrefix(uri, kmsSchemeHTTPS) {
		proto = "https://"
		uri = uri[len(kmsSchemeHTTPS):]
	}
	if proto == "" && strings.HasPrefix(uri, kmsSchemeHTTP) {
		proto = "http://"
		uri = uri[len(kmsSchemeHTTP):]
	}
	if proto == "" {
		return nil, fmt.Errorf("not supported uri %v", original_uri)
	}

	port := ":9600" // default kms port
	path := ""      // default path

	parts := strings.Split(uri, ";")
	for i, s := range parts {
		path_index := strings.Index(s, "/")
		if path_index > -1 {
			path = s[path_index:]
			s = s[:path_index]
		}
		port_index := strings.Index(s, ":")
		if port_index > -1 {
			port = s[port_index:]
			s = s[:port_index]
		}
		if (path_index > -1 || port_index > -1) && i+1 != len(parts) {
			return nil, fmt.Errorf("bad uri: %v", original_uri)
		}
		urls = append(urls, proto+s)
	}

	for i := range urls {
		urls[i] += port
		urls[i] += path
	}

	return urls, nil
}

// kmsUrl parse KeyProviderUri to list of URL's
func (c *Client) kmsUrl(einfo *hdfs.FileEncryptionInfoProto) ([]string, error) {
	defaults, err := c.fetchDefaults()
	if err != nil {
		return nil, err
	}

	urls, err := kmsParseProviderUri(defaults.GetKeyProviderUri())
	if err != nil {
		return nil, err
	}

	// Reorder urls. Simple method to round robin calls across em.
	rand.Shuffle(len(urls), func(i, j int) { urls[i], urls[j] = urls[j], urls[i] })

	for i := range urls {
		urls[i] = urls[i] + "/v1/keyversion/" + url.QueryEscape(*einfo.EzKeyVersionName) + "/_eek?eek_op=decrypt"
	}

	return urls, nil
}

func (c *Client) kmsRequest(url string, requestBody []byte) ([]byte, int, error) {
	resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return responseBody, resp.StatusCode, nil
}

func (c *Client) kmsDecrypt(url string, requestBody []byte) ([]byte, error) {
	responseBody, statusCode, err := c.kmsRequest(url, requestBody)
	if err != nil {
		return nil, err
	}

	if statusCode == 401 {
		err = c.kmsAuth(url)
		if err != nil {
			return nil, err
		}
		// retry with cookie
		responseBody, statusCode, err = c.kmsRequest(url, requestBody)
		if err != nil {
			return nil, err
		}
	}

	if statusCode != 200 {
		// On error, kms respond with error message in JSON object.
		type Exception struct {
			RemoteException struct {
				Message string `json:"message"`
			}
		}
		var kmsException Exception
		if err = json.Unmarshal(responseBody, &kmsException); err == nil && kmsException.RemoteException.Message != "" {
			errorMessage := kmsException.RemoteException.Message
			err = errors.New(errorMessage)
		} else {
			err = fmt.Errorf("unexpected response code from KMS: %v", statusCode)
		}
		return nil, err
	}

	type KmsRespose struct {
		Key string `json:"material"`
	}
	var kmsResponseJson KmsRespose
	if err = json.Unmarshal(responseBody, &kmsResponseJson); err != nil {
		return nil, err
	}

	var key []byte
	key, err = base64.RawURLEncoding.DecodeString(kmsResponseJson.Key)
	if err != nil {
		return nil, err
	}
	if len(key) != aes.BlockSize {
		return nil, fmt.Errorf("unexpected key size from KMS: %v", len(key))
	}

	return key, nil
}

func (c *Client) kmsGetKey(einfo *hdfs.FileEncryptionInfoProto) (*transparentEncryptionInfo, error) {
	if einfo.GetCryptoProtocolVersion() != hdfs.CryptoProtocolVersionProto_ENCRYPTION_ZONES {
		return nil, fmt.Errorf("not supported CryptoProtocolVersion %v", einfo.CryptoProtocolVersion)
	}
	if einfo.GetSuite() != hdfs.CipherSuiteProto_AES_CTR_NOPADDING {
		return nil, fmt.Errorf("not supported CipherSuiteProto %v", einfo.Suite)
	}

	urls, err := c.kmsUrl(einfo)
	if err != nil {
		return nil, errors.Wrap(err, "fail to get KMS address")
	}

	requestBody, err := json.Marshal(map[string]string{
		"material": base64.URLEncoding.EncodeToString(einfo.Key),
		"iv":       base64.URLEncoding.EncodeToString(einfo.Iv),
		"name":     *einfo.KeyName})
	if err != nil {
		return nil, err
	}

	var key []byte
	for _, url := range urls {
		key, err = c.kmsDecrypt(url, requestBody)
		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, errors.Wrap(err, "kms")
	}

	return &transparentEncryptionInfo{
		key: key,
		iv:  einfo.Iv,
	}, nil
}
