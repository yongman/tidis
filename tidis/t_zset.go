//
// t_zset.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/log"
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/utils"
	"math"
	"strconv"
)

var (
	ScoreMin int64 = math.MinInt64 + 2
	ScoreMax int64 = math.MaxInt64 - 1
)

type ZSetObj struct {
	Object
	Size uint64
}

func MarshalZSetObj(obj *ZSetObj) []byte {
	totalLen := 1 + 8 + 1 + 8
	raw := make([]byte, totalLen)

	idx := 0
	raw[idx] = obj.Type
	idx++
	_ = util.Uint64ToBytes1(raw[idx:], obj.ExpireAt)
	idx += 8
	raw[idx] = obj.Tomb
	idx++
	_ = util.Uint64ToBytes1(raw[idx:], obj.Size)

	return raw
}

func UnmarshalZSetObj(raw []byte) (*ZSetObj, error) {
	if len(raw) != 18 {
		return nil, nil
	}
	obj := ZSetObj{}
	idx := 0
	obj.Type = raw[idx]
	if obj.Type != TZSETMETA {
		return nil, terror.ErrTypeNotMatch
	}
	idx++
	obj.ExpireAt, _ = util.BytesToUint64(raw[idx:])
	idx += 8
	obj.Tomb = raw[idx]
	idx++
	obj.Size, _ = util.BytesToUint64(raw[idx:])
	return &obj, nil
}

func (tidis *Tidis) ZSetMetaObj(dbId uint8, txn, ss interface{}, key []byte) (*ZSetObj, bool, error) {
	var (
		v   []byte
		err error
	)

	metaKey := tidis.RawKeyPrefix(dbId, key)

	if txn == nil && ss == nil {
		v, err = tidis.db.Get(metaKey)
	} else if txn == nil {
		v, err = tidis.db.GetWithSnapshot(metaKey, ss)
	} else {
		v, err = tidis.db.GetWithTxn(metaKey, txn)
	}
	if err != nil {
		return nil, false, err
	}
	if v == nil {
		return nil, false, nil
	}
	obj, err := UnmarshalZSetObj(v)
	if err != nil {
		return nil, false, err
	}
	if obj.ObjectExpired(utils.Now()) {
		if txn == nil {
			tidis.Zremrangebyscore(dbId, key, ScoreMin, ScoreMax)
		} else {
			tidis.ZremrangebyscoreWithTxn(dbId, txn, key, ScoreMin, ScoreMax)
		}

		return nil, true, nil
	}
	return obj, false, nil
}

func (tidis *Tidis) newZSetMetaObj() *ZSetObj {
	return &ZSetObj{
		Object: Object{
			ExpireAt: 0,
			Type:     TZSETMETA,
			Tomb:     0,
		},
		Size: 0,
	}
}

func (tidis *Tidis) RawZSetDataKey(dbId uint8, key, member []byte) []byte {
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	dataKey := append(keyPrefix, DataTypeKey)
	dataKey = append(dataKey, member...)
	return dataKey
}

func (tidis *Tidis) RawZSetScoreKey(dbId uint8, key, member []byte, score int64) []byte {
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	scoreKey := append(keyPrefix, ScoreTypeKey)
	scoreBytes, _ := util.Uint64ToBytes(ZScoreOffset(score))
	scoreKey = append(scoreKey, scoreBytes...)
	scoreKey = append(scoreKey, member...)
	return scoreKey
}

type MemberPair struct {
	Score  int64
	Member []byte
}

func (tidis *Tidis) Zadd(dbId uint8, key []byte, mps ...*MemberPair) (int, error) {
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZaddWithTxn(dbId, txn, key, mps...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) ZaddWithTxn(dbId uint8, txn interface{}, key []byte, mps ...*MemberPair) (int, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		metaObj = tidis.newZSetMetaObj()
	}

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var (
			added int
		)

		// add data key and score key for each member pair
		for _, mp := range mps {
			eDataKey := tidis.RawZSetDataKey(dbId, key, mp.Member)
			eScoreKey := tidis.RawZSetScoreKey(dbId, key, mp.Member, mp.Score)
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
				metaObj.Size++
				added++
			} else {
				// delete old score item
				oldScore, err := util.BytesToInt64(v)
				if err != nil {
					return nil, err
				}
				oldScoreKey := tidis.RawZSetScoreKey(dbId, key, mp.Member, oldScore)
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
		eMetaKey := tidis.RawKeyPrefix(dbId, key)
		eMetaValue := MarshalZSetObj(metaObj)
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

func (tidis *Tidis) Zcard(dbId uint8, txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return uint64(0), err
	}
	if metaObj == nil {
		return uint64(0), nil
	}

	return metaObj.Size, nil
}

// zrange key [start stop] => zrange key offset count
func (tidis *Tidis) zRangeParse(dbId uint8, key []byte, start, stop int64, ss, txn interface{}, reverse bool) (int64, int64, error) {
	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return 0, 0, err
	}
	if metaObj == nil {
		return 0, 0, nil
	}

	// convert zero based index
	zz := int64(metaObj.Size)
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

func (tidis *Tidis) Zrange(dbId uint8, txn interface{}, key []byte, start, stop int64, withscores bool, reverse bool) ([]interface{}, error) {
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

	startKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, ScoreMin)
	endKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, ScoreMax)

	offset, count, err := tidis.zRangeParse(dbId, key, start, stop, ss, txn, reverse)
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
	keyPrefixLen := len(tidis.RawKeyPrefix(dbId, key))

	if !withscores {
		if !reverse {
			for i, m := range members {
				_, resp[i], _ = ZScoreDecoder(keyPrefixLen, m)
			}
		} else {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _ = ZScoreDecoder(keyPrefixLen, members[i])
			}
		}
	} else {
		if !reverse {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				s, resp[i], _ = ZScoreDecoder(keyPrefixLen, members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		} else {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				s, resp[i], _ = ZScoreDecoder(keyPrefixLen, members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		}
	}

	return resp, nil

}

func (tidis *Tidis) Zrangebyscore(dbId uint8, txn interface{}, key []byte, min, max int64, withscores bool, offset, count int, reverse bool) ([]interface{}, error) {
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
		err     error
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return EmptyListOrSet, nil
	}

	var startKey, endKey []byte

	if !reverse {
		startKey = tidis.RawZSetScoreKey(dbId, key, []byte{0}, min)
		endKey = tidis.RawZSetScoreKey(dbId, key, []byte{0}, max+1)
	} else {
		endKey = tidis.RawZSetScoreKey(dbId, key, []byte{0}, min+1)
		startKey = tidis.RawZSetScoreKey(dbId, key, []byte{0}, max)
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
	keyPrefixLen := len(tidis.RawKeyPrefix(dbId, key))
	if !withscores {
		if !reverse {
			for i, m := range members {
				_, resp[i], _ = ZScoreDecoder(keyPrefixLen, m)
			}
		} else {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _ = ZScoreDecoder(keyPrefixLen, members[i])
			}
		}
	} else {
		if !reverse {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				s, resp[i], _ = ZScoreDecoder(keyPrefixLen, members[idx])
				resp[i+1] = []byte(strconv.FormatInt(s, 10))
			}
		} else {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				s, resp[i], _ = ZScoreDecoder(keyPrefixLen, members[idx])
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

func (tidis *Tidis) Zrangebylex(dbId uint8, txn interface{}, key []byte, start, stop []byte, offset, count int, reverse bool) ([]interface{}, error) {
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

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return EmptyListOrSet, nil
	}

	eStartKey, withStart = tidis.zlexParse(dbId, key, start)
	eEndKey, withEnd = tidis.zlexParse(dbId, key, stop)

	if offset > int(metaObj.Size)-1 {
		return EmptyListOrSet, nil
	}

	if reverse {
		offset = int(metaObj.Size) - offset - count
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
	keyPreFixLen := len(tidis.RawKeyPrefix(dbId, key))
	if !reverse {
		for i, member := range members {
			resp[i] = member[keyPreFixLen+1:]
		}
	} else {
		for i, idx := 0, len(members)-1; idx >= 0; i, idx = i+1, idx-1 {
			resp[i] = members[idx][keyPreFixLen+1:]
		}
	}

	return resp, nil
}

func (tidis *Tidis) ZremrangebyscoreWithTxn(dbId uint8, txn1 interface{}, key []byte, min, max int64) (uint64, error) {
	startKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, min)
	endKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, max+1)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	var deleted uint64

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	members, err := tidis.db.GetRangeKeysWithTxn(startKey, endKey, 0, metaObj.Size, txn)
	if err != nil {
		return 0, err
	}
	log.Debugf("zset clear members:%d", len(members))

	prefixKeyLen := len(tidis.RawKeyPrefix(dbId, key))

	// delete each score key and data key
	for _, member := range members {
		_, mem, err := ZScoreDecoder(prefixKeyLen, member)
		if err != nil {
			return 0, err
		}

		// encode data key
		eDataKey := tidis.RawZSetDataKey(dbId, key, mem)

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
	metaObj.Size = metaObj.Size - deleted

	eMetaKey := tidis.RawKeyPrefix(dbId, key)
	if metaObj.Size != 0 {
		eMetaValue := MarshalZSetObj(metaObj)
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

	return deleted, nil
}

func (tidis *Tidis) Zremrangebyscore(dbId uint8, key []byte, min, max int64) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremrangebyscoreWithTxn(dbId, txn, key, min, max)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Zremrangebylex(dbId uint8, key, start, stop []byte) (uint64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremrangebylexWithTxn(dbId, txn, key, start, stop)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, nil
	}

	return v.(uint64), nil
}
func (tidis *Tidis) ZremrangebylexWithTxn(dbId uint8, txn interface{}, key, start, stop []byte) (uint64, error) {
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

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

		eStartKey, withStart = tidis.zlexParse(dbId, key, start)
		eEndKey, withEnd = tidis.zlexParse(dbId, key, stop)

		members, err := tidis.db.GetRangeKeysWithFrontierWithTxn(eStartKey, withStart, eEndKey, withEnd, 0, zsize, txn)
		if err != nil {
			return nil, err
		}

		deleted = uint64(len(members))
		if zsize < deleted {
			return nil, terror.ErrInvalidMeta
		}

		eMetaKey := tidis.RawKeyPrefix(dbId, key)

		keyPrefixLen := len(eMetaKey)
		// delete all members in score and data
		for _, member := range members {
			mem := member[keyPrefixLen+1:]
			// generate score key
			scoreRaw, err := tidis.db.GetWithTxn(member, txn)
			if err != nil {
				return nil, err
			}
			score, _ := util.BytesToInt64(scoreRaw)
			eScoreKey := tidis.RawZSetScoreKey(dbId, key, mem, score)

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
			eMetaValue := MarshalZSetObj(metaObj)
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

func (tidis *Tidis) Zcount(dbId uint8, txn interface{}, key []byte, min, max int64) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var (
		count uint64
		err   error
		ss    interface{}
	)
	if min > max {
		return 0, nil
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	startKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, min)
	endKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, max+1)

	if txn == nil {
		count, err = tidis.db.GetRangeKeysCount(startKey, true, endKey, true, metaObj.Size, ss)
	} else {
		count, err = tidis.db.GetRangeKeysCountWithTxn(startKey, true, endKey, true, metaObj.Size, txn)
	}

	return count, err
}

func (tidis *Tidis) zlexParse(dbId uint8, key, lex []byte) ([]byte, bool) {
	if len(lex) == 0 {
		return nil, false
	}
	var lexKey []byte
	var withFrontier bool
	var m []byte

	switch lex[0] {
	case '-':
		m = []byte{0}
	case '+':
		m = append(lex[1:], byte(0))
	case '(':
		m = lex[1:]
		withFrontier = false
	case '[':
		m = lex[1:]
		withFrontier = true
	default:
		return nil, false
	}
	lexKey = tidis.RawZSetDataKey(dbId, key, m)

	return lexKey, withFrontier
}

func (tidis *Tidis) Zlexcount(dbId uint8, txn interface{}, key, start, stop []byte) (uint64, error) {
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

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	var (
		eStartKey, eEndKey []byte
		withStart, withEnd bool
	)

	eStartKey, withStart = tidis.zlexParse(dbId, key, start)
	eEndKey, withEnd = tidis.zlexParse(dbId, key, stop)

	if txn == nil {
		count, err = tidis.db.GetRangeKeysCount(eStartKey, withStart, eEndKey, withEnd, metaObj.Size, ss)
	} else {
		count, err = tidis.db.GetRangeKeysCountWithTxn(eStartKey, withStart, eEndKey, withEnd, metaObj.Size, txn)
	}
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (tidis *Tidis) Zscore(dbId uint8, txn interface{}, key, member []byte) (int64, bool, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, false, terror.ErrKeyEmpty
	}

	var (
		ss       interface{}
		scoreRaw []byte
		err      error
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, false, err
		}
		metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
		if err != nil {
			return 0, false, err
		}
		if metaObj == nil {
			return 0, false, nil
		}
		eDataKey := tidis.RawZSetDataKey(dbId, key, member)

		scoreRaw, err = tidis.db.GetWithSnapshot(eDataKey, ss)
		if err != nil {
			return 0, false, err
		}
	} else {
		metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, ss, key)
		if err != nil {
			return 0, false, err
		}
		if metaObj == nil {
			return 0, false, nil
		}
		eDataKey := tidis.RawZSetDataKey(dbId, key, member)
		scoreRaw, err = tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return 0, false, err
		}
	}

	score, _ := util.BytesToInt64(scoreRaw)
	return score, true, nil
}

func (tidis *Tidis) Zrem(dbId uint8, key []byte, members ...[]byte) (uint64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZremWithTxn(dbId, txn, key, members...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) ZremWithTxn(dbId uint8, txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			deleted uint64
		)

		for _, member := range members {
			eDataKey := tidis.RawZSetDataKey(dbId, key, member)

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

			eScoreKey := tidis.RawZSetScoreKey(dbId, key, member, score)

			err = txn.Delete(eDataKey)
			if err != nil {
				return 0, err
			}
			err = txn.Delete(eScoreKey)
			if err != nil {
				return 0, err
			}
		}
		if metaObj.Size < deleted {
			return 0, terror.ErrInvalidMeta
		}

		// update meta key
		metaObj.Size = metaObj.Size - deleted

		eMetaKey := tidis.RawKeyPrefix(dbId, key)
		eMetaValue := MarshalZSetObj(metaObj)
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

func (tidis *Tidis) Zincrby(dbId uint8, key []byte, delta int64, member []byte) (int64, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.ZincrbyWithTxn(dbId, txn, key, delta, member)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int64), nil
}

func (tidis *Tidis) ZincrbyWithTxn(dbId uint8, txn interface{}, key []byte, delta int64, member []byte) (int64, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	metaObj, _, err := tidis.ZSetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			newScore  int64
			eScoreKey []byte
		)

		eDataKey := tidis.RawZSetDataKey(dbId, key, member)
		s, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return 0, err
		}
		if s == nil {
			// member not exists, add it with new score
			metaObj.Size++
			newScore = delta
			eScoreKey = tidis.RawZSetScoreKey(dbId, key, member, newScore)

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

			eMetaValue := MarshalZSetObj(metaObj)
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
			eScoreKey = tidis.RawZSetScoreKey(dbId, key, member, score)
			err = txn.Delete(eScoreKey)
			if err != nil {
				return 0, err
			}

			eScoreKey = tidis.RawZSetScoreKey(dbId, key, member, newScore)
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

func (tidis *Tidis) Zrank(dbId uint8, txn interface{}, key, member []byte, score int64) (int64, bool, error) {
	if len(key) == 0 {
		return -1, false, terror.ErrKeyEmpty
	}

	var (
		err   error
		v     int64
		exist bool
		ss    interface{}
	)

	startKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, ScoreMin)
	endKey := tidis.RawZSetScoreKey(dbId, key, []byte{0}, ScoreMax)
	objKey := tidis.RawZSetScoreKey(dbId, key, member, score)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return -1, false, err
		}
		v, exist, err = tidis.db.GetRank(startKey, endKey, objKey, ss)
	} else {
		v, exist, err = tidis.db.GetRankWithTxn(startKey, endKey, objKey, txn)
	}

	return v, exist, err
}
