package hdfs

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
)

const (
	// in FileWriter we use chunks upto aesChunkSize bytes to encrypt data
	aesChunkSize = 1024 * 1024
)

// calculateIV `shifts` IV to given offset
// based on calculateIV from AesCtrCryptoCodec.java
func calculateIV(offset int64, initIV []byte) ([]byte, error) {
	if len(initIV) != aes.BlockSize {
		return nil, fmt.Errorf("calculateIV: invalid iv size: %v", len(initIV))
	}

	counter := offset / aes.BlockSize
	iv := make([]byte, aes.BlockSize)

	high := binary.BigEndian.Uint64(initIV[:8])
	low := binary.BigEndian.Uint64(initIV[8:])
	origLow := low

	low += uint64(counter)
	if low < origLow { // wrap
		high += 1
	}

	binary.BigEndian.PutUint64(iv, high)
	binary.BigEndian.PutUint64(iv[8:], low)

	return iv, nil
}

// aesCtrStep perform AES-CTR XOR operation on given byte string.
// Once encryption and decryption are exactly the same operation for CTR mode,
// this function can be used to perform both.
func aesCtrStep(offset int64, enc *transparentEncryptionInfo, b []byte) ([]byte, error) {
	iv, err := calculateIV(offset, enc.iv)
	if err != nil {
		return nil, err
	}

	if enc.cipher == nil {
		cipher, err := aes.NewCipher(enc.key)
		if err != nil {
			return nil, err
		}
		enc.cipher = cipher
	}

	stream := cipher.NewCTR(enc.cipher, iv)

	padding := offset % aes.BlockSize
	if padding > 0 {
		tmp := make([]byte, padding)
		stream.XORKeyStream(tmp, tmp)
	}

	text := make([]byte, len(b))
	stream.XORKeyStream(text, b)
	return text, nil
}
