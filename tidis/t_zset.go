//
// t_zset.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"math"
	"strconv"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/log"
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
)

var (
	SCORE_MIN int64 = math.MinInt64 + 2
	SCORE_MAX int64 = math.MaxInt64 - 1
)

type MemberPair struct {
	Score  int64
	Member []byte
}

func (tidis *Tidis) Zadd(key []byte, mps ...*MemberPair) (int, error) {
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZaddWithTxn(txn, key, mps...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) ZaddWithTxn(txn interface{}, key []byte, mps ...*MemberPair) (int, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := ZMetaEncoder(key)

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var (
			zsize uint64
			added int
		)

		zsize, ttl, flag, err := tidis.zGetMeta(eMetaKey, nil, txn1)
		if err != nil {
			return nil, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TZSETMETA, key)
			return nil, terror.ErrKeyBusy
		}

		// add data key and score key for each member pair
		for _, mp := range mps {
			eDataKey := ZDataEncoder(key, mp.Member)
			eScoreKey := ZScoreEncoder(key, mp.Member, mp.Score)
			score, err := util.Int64ToBytes(mp.Score)
			if err != nil {
				return nil, err
			}

			v, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return nil, err
			}
			if v == nil {
				// member not exists
				zsize++
				added++
			} else {
				// delete old score item
				oldScore, err := util.BytesToInt64(v)
				if err != nil {
					return nil, err
				}
				oldScoreKey := ZScoreEncoder(key, mp.Member, oldScore)
				err = txn.Delete(oldScoreKey)
				if err != nil {
					return nil, err
				}
			}

			err = txn.Set(eDataKey, score)
			if err != nil {
				return nil, err
			}

			err = txn.Set(eScoreKey, []byte{0})
			if err != nil {
				return nil, err
			}
		}
		// update meta key
		eMetaValue := tidis.zGenMeta(zsize, ttl, FNORMAL)
		err = txn.Set(eMetaKey, eMetaValue)
		if err != nil {
			return nil, err
		}
		return added, nil
	}

	// execute txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) Zcard(txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var zsize uint64

	eMetaKey := ZMetaEncoder(key)

	zsize, _, flag, err := tidis.zGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return 0, nil
	}

	return zsize, nil
}

// zrange key [start stop] => zrange key offset count
func (tidis *Tidis) zRangeParse(key []byte, start, stop int64, ss, txn interface{}, reverse bool) (int64, int64, error) {
	var zsize uint64
	var flag byte
	var err error

	zMetaKey := ZMetaEncoder(key)

	zsize, _, flag, err = tidis.zGetMeta(zMetaKey, ss, txn)
	if err != nil {
		return 0, 0, err
	}
	if zsize == 0 {
		// key not exists
		return 0, 0, nil
	}
	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return 0, 0, nil
	}

	// convert zero based index
	zz := int64(zsize)
	if start < 0 {
		if start < -zz {
			start = 0
		} else {
			start = start + zz
		}
	} else {
		if start >= zz {
			return 0, 0, nil
		}
	}

	if stop < 0 {
		if stop < -zz {
			stop = 0
		} else {
			stop = stop + zz
		}
	} else {
		if stop >= zz {
			stop = zz - 1
		}
	}
	if !reverse {
		return start, stop - start + 1, nil
	}

	start, stop = zz-stop-1, zz-start
	return start, stop - start, nil
}

func (tidis *Tidis) Zrange(txn interface{}, key []byte, start, stop int64, withscores bool, reverse bool) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		s       int64
		ss      interface{}
		err     error
		members [][]byte
	)

	if start > stop && (stop > 0 || start < 0) {
		// empty range
		return EmptyListOrSet, nil
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	startKey := ZScoreEncoder(key, []byte{0}, SCORE_MIN)
	endKey := ZScoreEncoder(key, []byte{0}, SCORE_MAX)

	offset, count, err := tidis.zRangeParse(key, start, stop, ss, txn, reverse)
	if err != nil {
		return nil, err
	}
	// key not exist or marked deleted
	if offset == 0 && count == 0 {
		return EmptyListOrSet, nil
	}

	// get all key range slice
	if txn == nil {
		members, err = tidis.db.GetRangeKeys(startKey, endKey, uint64(offset), uint64(count), ss)
	} else {
		members, err = tidis.db.GetRangeKeysWithTxn(startKey, endKey, uint64(offset), uint64(count), txn)
	}
	if err != nil {
		return nil, err
	}

	respLen := len(members)
	if withscores {
		respLen = respLen * 2
	}
	resp := make([]interface{}, respLen)

	if !withscores {
		if !reverse {
			for i, m := range members {
				_, resp[i], _, _ = ZScoreDecoder(m)
			}
		} else {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _, _ = ZScoreDecoder(members[i])
			}
		}
	} else {
		if !reverse {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				_, resp[i], s, _ = ZScoreDecoder(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		} else {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				_, resp[i], s, _ = ZScoreDecoder(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		}
	}

	return resp, nil

}

func (tidis *Tidis) Zrangebyscore(txn interface{}, key []byte, min, max int64, withscores bool, offset, count int, reverse bool) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	if (!reverse && min > max) || (reverse && min < max) {
		// empty range
		return EmptyListOrSet, nil
	}

	var (
		zsize   uint64
		ss      interface{}
		s       int64
		members [][]byte
		flag    byte
		err     error
	)
	eMetaKey := ZMetaEncoder(key)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	var startKey, endKey []byte

	if !reverse {
		startKey = ZScoreEncoder(key, []byte{0}, min)
		endKey = ZScoreEncoder(key, []byte{0}, max+1)
	} else {
		endKey = ZScoreEncoder(key, []byte{0}, min+1)
		startKey = ZScoreEncoder(key, []byte{0}, max)
	}

	zsize, _, flag, err = tidis.zGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}

	if zsize == 0 {
		return EmptyListOrSet, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return EmptyListOrSet, nil
	}

	if txn == nil {
		members, err = tidis.db.GetRangeKeys(startKey, endKey, 0, zsize, ss)
	} else {
		members, err = tidis.db.GetRangeKeysWithTxn(startKey, endKey, 0, zsize, txn)
	}
	if err != nil {
		return nil, err
	}

	if offset >= 0 {
		if offset < len(members) {
			// get sub slice
			if !reverse {
				end := offset + count
				if end > len(members) {
					end = len(members)
				}
				members = members[offset:end]
			} else {
				offset = len(members) - offset
				end := offset - count
				if end < 0 {
					end = 0
				}
				members = members[end:offset]
			}
		} else {
			return nil, nil
		}
	}

	respLen := len(members)
	if withscores {
		respLen = respLen * 2
	}
	resp := make([]interface{}, respLen)
	if !withscores {
		if !reverse {
			for i, m := range members {
				_, resp[i], _, _ = ZScoreDecoder(m)
			}
		} else {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _, _ = ZScoreDecoder(members[i])
			}
		}
	} else {
		if !reverse {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				_, resp[i], s, _ = ZScoreDecoder(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		} else {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				_, resp[i], s, _ = ZScoreDecoder(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		}
	}

	return resp, nil
}

func checkPrefixValid(a []byte) bool {
	if len(a) == 0 {
		return false
	}
	switch a[0] {
	case '-':
		return true
	case '+':
		return true
	case '(':
		return true
	case '[':
		return true
	default:
		return false
	}
}

func (tidis *Tidis) Zrangebylex(txn interface{}, key []byte, start, stop []byte, offset, count int, reverse bool) ([]interface{}, error) {
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	if reverse {
		// exchange start and stop if reverse range
		start, stop = stop, start
	}

	// start and stop must prefix with -/+/(/[
	if !checkPrefixValid(start) || !checkPrefixValid(stop) {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss                 interface{}
		err                error
		eStartKey, eEndKey []byte
		withStart, withEnd bool
		members            [][]byte
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	eMetaKey := ZMetaEncoder(key)

	zsize, ttl, flag, err := tidis.zGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}

	if zsize == 0 || TTLExpired(int64(ttl)) {
		return EmptyListOrSet, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return EmptyListOrSet, nil
	}

	eStartKey, withStart = tidis.zlexParse(key, start)
	eEndKey, withEnd = tidis.zlexParse(key, start)

	switch stop[0] {
	case '-':
		eEndKey = ZDataEncoderStart(key)
	case '+':
		eEndKey = ZDataEncoderEnd(key)
	case '(':
		eEndKey = ZDataEncoder(key, stop[1:])
		withEnd = false
	case '[':
		eEndKey = ZDataEncoder(key, stop[1:])
		withEnd = true
	}

	if count < 0 {
		count = int(zsize)
	}

	if offset > int(zsize)-1 {
		return EmptyListOrSet, nil
	}

	if reverse {
		offset = int(zsize) - offset - count
		if offset < 0 {
			count = count + offset
			offset = 0
		}
	}

	if txn == nil {
		members, err = tidis.db.GetRangeKeysWithFrontier(eStartKey, withStart, eEndKey, withEnd, uint64(offset), uint64(count), ss)
	} else {
		members, err = tidis.db.GetRangeKeysWithFrontierWithTxn(eStartKey, withStart, eEndKey, withEnd, uint64(offset), uint64(count), txn)
	}
	if err != nil {
		return nil, err
	}

	resp := make([]interface{}, len(members))
	if !reverse {
		for i, member := range members {
			_, resp[i], _ = ZDataDecoder(member)
		}
	} else {
		for i, idx := 0, len(members)-1; idx >= 0; i, idx = i+1, idx-1 {
			_, resp[i], _ = ZDataDecoder(members[idx])
		}
	}

	return resp, nil
}

func (tidis *Tidis) ZremrangebyscoreWithTxn(txn1 interface{}, key []byte, min, max int64, async *bool) (uint64, error) {
	startKey := ZScoreEncoder(key, []byte{0}, min)
	endKey := ZScoreEncoder(key, []byte{0}, max+1)
	eMetaKey := ZMetaEncoder(key)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	var deleted uint64

	zsize, ttl, _, err := tidis.zGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if zsize == 0 {
		return 0, nil
	}

	// async is true only used by delete entire key
	if *async && zsize < 1024 {
		*async = false
	}

	if *async {
		// mark meta key deleted
		v := tidis.zGenMeta(zsize, ttl, FDELETED)
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return 0, err
		}
		deleted = 1
	} else {
		members, err := tidis.db.GetRangeKeysWithTxn(startKey, endKey, 0, zsize, txn)
		if err != nil {
			return 0, err
		}
		log.Debugf("zset clear members:%d", len(members))

		// delete each score key and data key
		for _, member := range members {
			_, mem, _, err := ZScoreDecoder(member)
			if err != nil {
				return 0, err
			}

			// encode data key
			eDataKey := ZDataEncoder(key, mem)

			err = txn.Delete(member)
			if err != nil {
				return 0, err
			}
			err = txn.Delete(eDataKey)
			if err != nil {
				return 0, err
			}
		}
		deleted = uint64(len(members))

		// update zsize
		if zsize < deleted {
			return 0, terror.ErrInvalidMeta
		}
		zsize = zsize - deleted

		if zsize != 0 {
			eMetaValue := tidis.zGenMeta(zsize, ttl, FNORMAL)
			err = txn.Set(eMetaKey, eMetaValue)
			if err != nil {
				return 0, err
			}
		} else {
			// delete meta key
			err = txn.Delete(eMetaKey)
			if err != nil {
				return 0, err
			}
		}
	}

	return deleted, nil
}

func (tidis *Tidis) Zremrangebyscore(key []byte, min, max int64, async bool) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremrangebyscoreWithTxn(txn, key, min, max, &async)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	if async {
		tidis.AsyncDelAdd(TZSETMETA, key)
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Zremrangebylex(key, start, stop []byte) (uint64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremrangebylexWithTxn(txn, key, start, stop)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, nil
	}

	return v.(uint64), nil
}
func (tidis *Tidis) ZremrangebylexWithTxn(txn interface{}, key, start, stop []byte) (uint64, error) {
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := ZMetaEncoder(key)

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			zsize              uint64
			deleted            uint64
			eStartKey, eEndKey []byte
			withStart, withEnd bool
		)

		zsize, ttl, flag, err := tidis.zGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}

		if zsize == 0 {
			return 0, nil
		}
		if flag == FDELETED {
			tidis.AsyncDelAdd(TZSETMETA, key)
			return 0, nil
		}

		eStartKey, withStart = tidis.zlexParse(key, start)
		eEndKey, withEnd = tidis.zlexParse(key, stop)

		members, err := tidis.db.GetRangeKeysWithFrontierWithTxn(eStartKey, withStart, eEndKey, withEnd, 0, zsize, txn)
		if err != nil {
			return nil, err
		}

		deleted = uint64(len(members))
		if zsize < deleted {
			return nil, terror.ErrInvalidMeta
		}

		// delete all members in score and data
		for _, member := range members {
			_, mem, err := ZDataDecoder(member)
			if err != nil {
				return nil, err
			}
			// generate score key
			scoreRaw, err := tidis.db.GetWithTxn(member, txn)
			if err != nil {
				return nil, err
			}
			score, _ := util.BytesToInt64(scoreRaw)
			eScoreKey := ZScoreEncoder(key, mem, score)

			err = txn.Delete(member)
			if err != nil {
				return nil, err
			}

			err = txn.Delete(eScoreKey)
			if err != nil {
				return nil, err
			}
		}

		zsize = zsize - deleted
		// update meta
		if zsize == 0 {
			// delete meta key
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}
		} else {
			// update meta key
			eMetaValue := tidis.zGenMeta(zsize, ttl, FNORMAL)
			err = txn.Set(eMetaKey, eMetaValue)
			if err != nil {
				return nil, err
			}
		}

		return deleted, nil
	}

	// execute txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, nil
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Zcount(txn interface{}, key []byte, min, max int64) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var (
		zsize uint64
		count uint64
		flag  byte
		err   error
		ss    interface{}
	)
	if min > max {
		return 0, nil
	}
	eMetaKey := ZMetaEncoder(key)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}
	}
	zsize, _, flag, err = tidis.zGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return 0, err
	}

	if zsize == 0 {
		return 0, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return 0, nil
	}

	startKey := ZScoreEncoder(key, []byte{0}, min)
	endKey := ZScoreEncoder(key, []byte{0}, max+1)

	if txn == nil {
		count, err = tidis.db.GetRangeKeysCount(startKey, true, endKey, true, zsize, ss)
	} else {
		count, err = tidis.db.GetRangeKeysCountWithTxn(startKey, true, endKey, true, zsize, txn)
	}

	return count, err
}

func (tidis *Tidis) zlexParse(key, lex []byte) ([]byte, bool) {
	if len(lex) == 0 {
		return nil, false
	}
	var lexKey []byte
	var withFrontier bool

	switch lex[0] {
	case '-':
		lexKey = ZDataEncoderStart(key)
	case '+':
		lexKey = ZDataEncoderEnd(key)
	case '(':
		lexKey = ZDataEncoder(key, lex[1:])
		withFrontier = false
	case '[':
		lexKey = ZDataEncoder(key, lex[1:])
		withFrontier = true
	default:
		return nil, false
	}

	return lexKey, withFrontier
}

func (tidis *Tidis) Zlexcount(txn interface{}, key, start, stop []byte) (uint64, error) {
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// start and stop must prefix with -/+/(/[
	if !checkPrefixValid(start) || !checkPrefixValid(stop) {
		return 0, terror.ErrKeyEmpty
	}

	var (
		ss    interface{}
		err   error
		count uint64
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}
	}

	eMetaKey := ZMetaEncoder(key)

	var (
		eStartKey, eEndKey []byte
		withStart, withEnd bool
	)

	zsize, _, flag, err := tidis.zGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return 0, err
	}

	if zsize == 0 {
		return 0, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return 0, nil
	}

	eStartKey, withStart = tidis.zlexParse(key, start)
	eEndKey, withEnd = tidis.zlexParse(key, stop)

	if txn == nil {
		count, err = tidis.db.GetRangeKeysCount(eStartKey, withStart, eEndKey, withEnd, zsize, ss)
	} else {
		count, err = tidis.db.GetRangeKeysCountWithTxn(eStartKey, withStart, eEndKey, withEnd, zsize, txn)
	}
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (tidis *Tidis) Zscore(txn interface{}, key, member []byte) (int64, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eDataKey := ZDataEncoder(key, member)

	var (
		ss       interface{}
		scoreRaw []byte
		err      error
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}

		scoreRaw, err = tidis.db.GetWithSnapshot(eDataKey, ss)
		if err != nil {
			return 0, err
		}
	} else {
		scoreRaw, err = tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return 0, err
		}
	}
	score, _ := util.BytesToInt64(scoreRaw)
	return score, nil
}

func (tidis *Tidis) Zrem(key []byte, members ...[]byte) (uint64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremWithTxn(txn, key, members...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) ZremWithTxn(txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := ZMetaEncoder(key)

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			deleted uint64
		)

		zsize, ttl, flag, err := tidis.zGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return 0, err
		}

		if zsize == 0 {
			return 0, nil
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TZSETMETA, key)
			return 0, nil
		}

		for _, member := range members {
			eDataKey := ZDataEncoder(key, member)

			scoreRaw, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return 0, err
			}
			if scoreRaw == nil {
				// member not exists
				continue
			}

			deleted++

			score, err := util.BytesToInt64(scoreRaw)
			if err != nil {
				return 0, err
			}

			eScoreKey := ZScoreEncoder(key, member, score)

			err = txn.Delete(eDataKey)
			if err != nil {
				return 0, err
			}
			err = txn.Delete(eScoreKey)
			if err != nil {
				return 0, err
			}
		}
		if zsize < deleted {
			return 0, terror.ErrInvalidMeta
		}

		// update meta key
		zsize = zsize - deleted
		eMetaValue := tidis.zGenMeta(zsize, ttl, FNORMAL)
		err = txn.Set(eMetaKey, eMetaValue)
		if err != nil {
			return 0, err
		}

		return deleted, nil
	}

	// execute txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Zincrby(key []byte, delta int64, member []byte) (int64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZincrbyWithTxn(txn, key, delta, member)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int64), nil
}

func (tidis *Tidis) ZincrbyWithTxn(txn interface{}, key []byte, delta int64, member []byte) (int64, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, terror.ErrKeyEmpty
	}
	eMetaKey := ZMetaEncoder(key)

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			zsize     uint64
			newScore  int64
			eScoreKey []byte
		)

		zsize, ttl, flag, err := tidis.zGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return 0, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TZSETMETA, key)
			return 0, terror.ErrKeyBusy
		}

		eDataKey := ZDataEncoder(key, member)
		s, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return 0, err
		}
		if s == nil {
			// member not exists, add it with new score
			zsize++
			newScore = delta
			eScoreKey = ZScoreEncoder(key, member, newScore)

			// add data key and score key, then update meta key
			scoreRaw, _ := util.Int64ToBytes(newScore)
			err = txn.Set(eDataKey, scoreRaw)
			if err != nil {
				return 0, err
			}

			err = txn.Set(eScoreKey, []byte{0})
			if err != nil {
				return 0, err
			}

			eMetaValue := tidis.zGenMeta(zsize, ttl, FNORMAL)
			err = txn.Set(eMetaKey, eMetaValue)
			if err != nil {
				return 0, err
			}
		} else {
			// get the member score
			scoreRaw, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return 0, err
			}
			if scoreRaw == nil {
				return 0, terror.ErrInvalidMeta
			}
			score, _ := util.BytesToInt64(scoreRaw)

			newScore = score + delta

			// update datakey
			scoreRaw, _ = util.Int64ToBytes(newScore)
			err = txn.Set(eDataKey, scoreRaw)
			if err != nil {
				return 0, err
			}

			// delete old score key
			eScoreKey = ZScoreEncoder(key, member, score)
			err = txn.Delete(eScoreKey)
			if err != nil {
				return 0, err
			}

			eScoreKey = ZScoreEncoder(key, member, newScore)
			err = txn.Set(eScoreKey, []byte{0})
			if err != nil {
				return 0, err
			}

		}

		return newScore, nil
	}

	// execute txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(int64), nil
}

// meta data format same as hash type
func (tidis *Tidis) zGetMeta(key []byte, ss, txn interface{}) (uint64, uint64, byte, error) {
	return tidis.hGetMeta(key, ss, txn)
}

func (tidis *Tidis) zGenMeta(size, ttl uint64, flag byte) []byte {
	return tidis.hGenMeta(size, ttl, flag)
}

func (tidis *Tidis) ZPExpireAt(key []byte, ts int64) (int, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZPExpireAtWithTxn(txn, key, ts)
	}

	// execute txn f
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}
func (tidis *Tidis) ZPExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			zMetaKey []byte
			tMetaKey []byte
		)

		zMetaKey = ZMetaEncoder(key)
		zsize, ttl, flag, err := tidis.zGetMeta(zMetaKey, nil, txn)
		if err != nil {
			return 0, err
		}

		if zsize == 0 {
			// key not exists
			return 0, nil
		}
		if flag == FDELETED {
			tidis.AsyncDelAdd(TZSETMETA, key)
			return 0, nil
		}

		// check expire time already set before
		if ttl != 0 {
			tMetaKey = TMZEncoder(key, ttl)
			if err = txn.Delete(tMetaKey); err != nil {
				return 0, err
			}
		}

		// update set meta key and ttl meta key
		zMetaValue := tidis.zGenMeta(zsize, uint64(ts), FNORMAL)
		if err = txn.Set(zMetaKey, zMetaValue); err != nil {
			return 0, err
		}

		tMetaKey = TMZEncoder(key, uint64(ts))
		if err = txn.Set(tMetaKey, []byte{0}); err != nil {
			return 0, err
		}

		return 1, nil
	}

	// execute txn f
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) ZPExpire(key []byte, ms int64) (int, error) {
	return tidis.ZPExpireAt(key, ms+time.Now().UnixNano()/1000/1000)
}

func (tidis *Tidis) ZExpireAt(key []byte, ts int64) (int, error) {
	return tidis.ZPExpireAt(key, ts*1000)
}

func (tidis *Tidis) ZExpire(key []byte, s int64) (int, error) {
	return tidis.ZPExpire(key, s*1000)
}

func (tidis *Tidis) ZPExpireWithTxn(txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.ZPExpireAtWithTxn(txn, key, ms+time.Now().UnixNano()/1000/1000)
}

func (tidis *Tidis) ZExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.ZPExpireAtWithTxn(txn, key, ts*1000)
}

func (tidis *Tidis) ZExpireWithTxn(txn interface{}, key []byte, s int64) (int, error) {
	return tidis.ZPExpireWithTxn(txn, key, s*1000)
}

func (tidis *Tidis) ZPTtl(txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var (
		ss  interface{}
		err error
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}
	}

	eMetaKey := ZMetaEncoder(key)

	ssize, ttl, flag, err := tidis.zGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return 0, err
	}

	if ssize == 0 {
		// key not exists
		return -2, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TZSETMETA, key)
		return -2, nil
	}

	if ttl == 0 {
		// no expire associated
		return -1, nil
	}

	var ts int64
	ts = int64(ttl) - time.Now().UnixNano()/1000/1000
	if ts < 0 {
		ts = 0
		// TODO lazy delete key
	}

	return ts, nil
}

func (tidis *Tidis) ZTtl(txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.ZPTtl(txn, key)
	if ttl < 0 {
		return ttl, err
	}
	return ttl / 1000, err
}
