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

const (
	// tenant length should be less than 250, 251-255 can be used by system
	LeaderKey = 251
	GCPointKey = 252
)
// encoder and decoder for key of data

// tenantlen(2)|tenant|dbid(1)|typedata(1)|userkeylen(4)|userkey
func RawKeyPrefix(tenantid string, dbid uint8, key []byte) []byte {
	buf := make([]byte, 2+len(tenantid)+1+1+4+len(key))

	idx := 0
	util.Uint16ToBytes1(buf[idx:], uint16(len(tenantid)))
	idx += 2

	copy(buf[idx:], []byte(tenantid))
	idx += len(tenantid)

	buf[idx], buf[idx+1] = dbid, ObjectData
	idx += 2

	util.Uint32ToBytes1(buf[idx:], uint32(len(key)))
	idx += 4

	copy(buf[idx:], key)
	return buf
}

func RawTenantPrefix(tenantId string) []byte {
	buf := make([]byte, 2+len(tenantId))

	idx := 0
	util.Uint16ToBytes1(buf[idx:], uint16(len(tenantId)))
	idx += 2

	copy(buf[idx:], []byte(tenantId))

	return buf
}

func RawDBPrefix(tenantId string, dbId uint8) []byte {
	buf := RawTenantPrefix(tenantId)
	buf = append(buf, dbId)

	return buf
}

func ZScoreOffset(score int64) uint64 {
	return uint64(score + ScoreMax)
}

func ZScoreRestore(rscore uint64) int64 {
	return int64(rscore - uint64(ScoreMax))
}

func ZScoreDecoder(rawkeyPrefixLen int, rawkey []byte) (int64, []byte, error) {
	pos := rawkeyPrefixLen

	if rawkey[pos] != ScoreTypeKey {
		return 0, nil, terror.ErrTypeNotMatch
	}
	pos++

	score, _ := util.BytesToUint64(rawkey[pos:])
	pos = pos + 8

	mem := rawkey[pos:]

	return ZScoreRestore(score), mem, nil
}

func RawSysLeaderKey() []byte {
	b, _ := util.Uint16ToBytes(LeaderKey)
	return b
}

func RawSysGCPointKey() []byte {
	b, _ := util.Uint16ToBytes(GCPointKey)
	return b
}