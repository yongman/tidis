//
// codec.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"github.com/yongman/go/util"
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

// for ttl checker
// type(ttl)|type(key_type)|timestamp(8)|key
func TMSEncoder(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+10)
	buf[0], buf[1] = TTTLMETA, TSTRING

	tsRaw, _ := util.Uint64ToBytes(ts)
	copy(buf[2:], tsRaw)

	copy(buf[10:], key)
	return buf
}

func TMSDecoder(rawkey []byte) ([]byte, uint64, error) {
	if len(rawkey) < 10 || rawkey[0] != TTTLMETA || rawkey[1] != TSTRING {
		return nil, 0, terror.ErrTypeNotMatch
	}

	ts, err := util.BytesToUint64(rawkey[2:])
	if err != nil {
		return nil, 0, err
	}

	return rawkey[10:], ts, nil
}

// for ttl get
// type(ttl)|type(key_type)|key, value is unix timestamp
func TDSEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+2)
	buf[0], buf[1] = TTTLDATA, TSTRING

	copy(buf[2:], key)
	return buf
}

func TDSDecoder(rawkey []byte) ([]byte, error) {
	if len(rawkey) < 3 || rawkey[0] != TTTLDATA || rawkey[1] != TSTRING {
		return nil, terror.ErrTypeNotMatch
	}

	return rawkey[2:], nil
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

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	util.Uint64ToBytes1(buf[pos:], idx)

	return buf
}

func LDataDecoder(rawkey []byte) ([]byte, uint64, error) {
	pos := 0
	t := rawkey[pos]
	if t != TLISTDATA {
		return nil, 0, terror.ErrTypeNotMatch
	}
	pos++

	keyLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	idx, _ := util.BytesToUint64(rawkey[pos:])

	return key, idx, nil
}

// for ttl checker
// type(ttl)|type(key_type)|timestamp(8)|key
func TMLEncoder(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+10)
	buf[0], buf[1] = TTTLMETA, TLISTMETA

	tsRaw, _ := util.Uint64ToBytes(ts)
	copy(buf[2:], tsRaw)

	copy(buf[10:], key)
	return buf
}

func TMLDecoder(rawkey []byte) ([]byte, uint64, error) {
	if len(rawkey) < 10 || rawkey[0] != TTTLMETA || rawkey[1] != TLISTMETA {
		return nil, 0, terror.ErrTypeNotMatch
	}

	ts, err := util.BytesToUint64(rawkey[2:])
	if err != nil {
		return nil, 0, err
	}

	return rawkey[10:], ts, nil
}

// hash encoder decoder
// meta key
// type(1)|key
func HMetaEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = THASHMETA

	copy(buf[1:], key)

	return buf
}

func HMetaDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]
	if t != THASHMETA {
		return nil, terror.ErrTypeNotMatch
	}

	return rawkey[1:], nil
}

// data key
// type(1)|keylen(2)|key|field
func HDataEncoder(key, field []byte) []byte {
	pos := 0

	buf := make([]byte, 1+2+len(key)+len(field))
	buf[0] = THASHDATA
	pos++

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	copy(buf[pos:], field)

	return buf
}

func HDataDecoder(rawkey []byte) ([]byte, []byte, error) {
	var pos uint16

	if rawkey[0] != THASHDATA {
		return nil, nil, terror.ErrTypeNotMatch
	}
	pos++

	keyLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+keyLen]
	pos = pos + keyLen

	field := rawkey[pos:]

	return key, field, nil
}

// for ttl checker
// type(ttl)|type(key_type)|timestamp(8)|key
func TMHEncoder(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+10)
	buf[0], buf[1] = TTTLMETA, THASHMETA

	tsRaw, _ := util.Uint64ToBytes(ts)
	copy(buf[2:], tsRaw)

	copy(buf[10:], key)
	return buf
}

func TMHDecoder(rawkey []byte) ([]byte, uint64, error) {
	if len(rawkey) < 10 || rawkey[0] != TTTLMETA || rawkey[1] != THASHMETA {
		return nil, 0, terror.ErrTypeNotMatch
	}

	ts, err := util.BytesToUint64(rawkey[2:])
	if err != nil {
		return nil, 0, err
	}

	return rawkey[10:], ts, nil
}

// set encoder/decoder
// same as hash
func SMetaEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TSETMETA

	copy(buf[1:], key)

	return buf
}

func SMetaDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]
	if t != TSETMETA {
		return nil, terror.ErrTypeNotMatch
	}

	return rawkey[1:], nil
}

func SDataEncoder(key, member []byte) []byte {
	pos := 0

	buf := make([]byte, 1+2+len(key)+len(member))
	buf[0] = TSETDATA
	pos++

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	copy(buf[pos:], member)

	return buf
}

func SDataDecoder(rawkey []byte) ([]byte, []byte, error) {
	var pos uint16

	if rawkey[0] != TSETDATA {
		return nil, nil, terror.ErrTypeNotMatch
	}
	pos++

	keyLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+keyLen]
	pos = pos + keyLen

	field := rawkey[pos:]

	return key, field, nil
}

// for ttl checker
// type(ttl)|type(key_type)|timestamp(8)|key
func TMSetEncoder(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+10)
	buf[0], buf[1] = TTTLMETA, TSETMETA

	tsRaw, _ := util.Uint64ToBytes(ts)
	copy(buf[2:], tsRaw)

	copy(buf[10:], key)
	return buf
}

func TMSetDecoder(rawkey []byte) ([]byte, uint64, error) {
	if len(rawkey) < 10 || rawkey[0] != TTTLMETA || rawkey[1] != TSETMETA {
		return nil, 0, terror.ErrTypeNotMatch
	}

	ts, err := util.BytesToUint64(rawkey[2:])
	if err != nil {
		return nil, 0, err
	}

	return rawkey[10:], ts, nil
}

// sorted set
// type|key
func ZMetaEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TZSETMETA

	copy(buf[1:], key)

	return buf
}

func ZMetaDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]

	if t != TZSETMETA {
		return nil, terror.ErrTypeNotMatch
	}

	return rawkey[1:], nil
}

// type|len(key)|key|len(member)|member
// value: member score
func ZDataEncoder(key, member []byte) []byte {
	pos := 0

	buf := make([]byte, 1+4+len(key)+len(member))
	buf[pos] = TZSETDATA
	pos++

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	util.Uint16ToBytes1(buf[pos:], uint16(len(member)))
	pos = pos + 2

	copy(buf[pos:], member)

	return buf
}

func ZDataEncoderStart(key []byte) []byte {
	return ZDataEncoder(key, []byte{0})
}

func ZDataEncoderEnd(key []byte) []byte {
	pos := 0

	buf := make([]byte, 1+4+len(key))
	buf[pos] = TZSETDATA
	pos++

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	a := -1
	util.Uint16ToBytes1(buf[pos:], uint16(a))

	return buf
}

func ZDataDecoder(rawkey []byte) ([]byte, []byte, error) {
	pos := 0

	if rawkey[pos] != TZSETDATA {
		return nil, nil, terror.ErrTypeNotMatch
	}
	pos++

	keyLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	memLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	mem := rawkey[pos : pos+int(memLen)]

	return key, mem, nil
}

func ZScoreOffset(score int64) uint64 {
	return uint64(score + SCORE_MAX)
}

func ZScoreRestore(rscore uint64) int64 {
	return int64(rscore - uint64(SCORE_MAX))
}

// type|len(key)|key|score|member
func ZScoreEncoder(key, member []byte, score int64) []byte {
	pos := 0

	buf := make([]byte, 1+2+len(key)+8+len(member))
	buf[pos] = TZSETSCORE
	pos++

	util.Uint16ToBytes1(buf[pos:], uint16(len(key)))
	pos = pos + 2

	copy(buf[pos:], key)
	pos = pos + len(key)

	// convert score to uint64 space
	util.Uint64ToBytes1(buf[pos:], ZScoreOffset(score))
	pos = pos + 8

	copy(buf[pos:], member)

	return buf
}

func ZScoreDecoder(rawkey []byte) ([]byte, []byte, int64, error) {
	pos := 0

	if rawkey[pos] != TZSETSCORE {
		return nil, nil, 0, terror.ErrTypeNotMatch
	}
	pos++

	keyLen, _ := util.BytesToUint16(rawkey[pos:])
	pos = pos + 2

	key := rawkey[pos : pos+int(keyLen)]
	pos = pos + int(keyLen)

	score, _ := util.BytesToUint64(rawkey[pos:])
	pos = pos + 8

	mem := rawkey[pos:]

	return key, mem, ZScoreRestore(score), nil
}

// for ttl checker
// type(ttl)|type(key_type)|timestamp(8)|key
func TMZEncoder(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+10)
	buf[0], buf[1] = TTTLMETA, TZSETMETA

	tsRaw, _ := util.Uint64ToBytes(ts)
	copy(buf[2:], tsRaw)

	copy(buf[10:], key)
	return buf
}

func TMZDecoder(rawkey []byte) ([]byte, uint64, error) {
	if len(rawkey) < 10 || rawkey[0] != TTTLMETA || rawkey[1] != TZSETMETA {
		return nil, 0, terror.ErrTypeNotMatch
	}

	ts, err := util.BytesToUint64(rawkey[2:])
	if err != nil {
		return nil, 0, err
	}

	return rawkey[10:], ts, nil
}
