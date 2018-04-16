//
// codec.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"encoding/binary"

	"github.com/yongman/tidis/terror"
)

// encoder and decoder for key of data

// string
// type(1)|key
func SEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TSTRING

	copy(buf[1:], key)
	return buf
}

func SDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]
	if t != TSTRING {
		return nil, terror.ErrTypeNotMatch
	}
	return rawkey[1:], nil
}

// list
// list meta key
func LMetaEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TLISTMETA

	copy(buf[1:], key)
	return buf
}

func LMetaDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]
	if t != TLISTMETA {
		return nil, terror.ErrTypeNotMatch
	}

	return rawkey[1:], nil
}

// list data key
// type(1)|keylen(2)|key|index(8)
func LDataEncoder(key []byte, idx uint64) []byte {
	pos := 0

	buf := make([]byte, len(key)+1+2+8)
	buf[pos] = TLISTDATA
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	binary.BigEndian.PutUint64(buf[pos:], idx)

	return buf
}

func LDataDecoder(rawkey []byte) ([]byte, uint64, error) {
	pos := 0
	t := rawkey[pos]
	if t != TLISTDATA {
		return nil, 0, terror.ErrTypeNotMatch
	}
	pos++

	keyLen := binary.BigEndian.Uint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	idx := binary.BigEndian.Uint64(rawkey[pos:])

	return key, idx, nil
}
