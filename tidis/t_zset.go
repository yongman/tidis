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

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
)

var (
	SCORE_MIN int64 = math.MinInt64 + 1
	SCORE_MAX int64 = math.MaxInt64
)

type MemberPair struct {
	Score  int64
	Member []byte
}

func (tidis *Tidis) Zadd(key []byte, mps ...*MemberPair) (int32, error) {
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
			zsize uint64 = 0
			added int32  = 0
		)

		ss := txn.GetSnapshot()

		zsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if zsizeRaw == nil {
			// not exists
			zsize = 0
		} else {
			zsize, _ = util.BytesToUint64(zsizeRaw)
		}

		// add data key and score key for each member pair
		for _, mp := range mps {
			eDataKey := ZDataEncoder(key, mp.Member)
			eScoreKey := ZScoreEncoder(key, mp.Member, mp.Score)
			score, err := util.Int64ToBytes(mp.Score)
			if err != nil {
				return nil, err
			}

			_, err = txn.Get(eDataKey)
			if err != nil && !kv.IsErrNotFound(err) {
				return nil, err
			}
			if kv.IsErrNotFound(err) {
				// member not exists
				zsize++
				added++
			} else {
				// delete old score item
				err = txn.Delete(eScoreKey)
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
		zsizeRaw, _ = util.Uint64ToBytes(zsize)
		err = txn.Set(eMetaKey, zsizeRaw)
		if err != nil {
			return nil, err
		}
		return added, nil
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int32), nil
}

func (tidis *Tidis) Zcard(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var zsize uint64 = 0

	eMetaKey := ZMetaEncoder(key)

	zsizeRaw, err := tidis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if zsizeRaw == nil {
		zsize = 0
	} else {
		zsize, err = util.BytesToUint64(zsizeRaw)
		if err != nil {
			return 0, err
		}
	}

	return zsize, nil
}

func (tidis *Tidis) Zrange(key []byte, start, stop int64, withscore bool) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	if start > stop && (stop > 0 || start < 0) {
		// empty range
		return nil, nil
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}

	eMetaKey := ZMetaEncoder(key)
	_, err = tidis.db.GetWithSnapshot(eMetaKey, ss)
	if err != nil {
		return nil, err
	}
	//TODO
	return nil, nil

}

func (tidis *Tidis) Zrangebyscore(key []byte, min, max int64, withscores bool, offset, count int, reverse bool) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	if (!reverse && min > max) || (reverse && min < max) {
		// empty range
		return nil, nil
	}

	var zsize uint64 = 0
	var s int64

	eMetaKey := ZMetaEncoder(key)

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}

	var startKey, endKey []byte

	if !reverse {
		startKey = ZScoreEncoder(key, []byte{0}, min)
		endKey = ZScoreEncoder(key, []byte{0}, max+1)
	} else {
		endKey = ZScoreEncoder(key, []byte{0}, min-1)
		startKey = ZScoreEncoder(key, []byte{0}, max)
	}

	zsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
	if err != nil {
		return nil, err
	}
	if zsizeRaw != nil {
		zsize, err = util.BytesToUint64(zsizeRaw)
		if err != nil {
			return nil, err
		}
	}

	members, err := tidis.db.GetRangeKeys(startKey, endKey, zsize, ss)
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
