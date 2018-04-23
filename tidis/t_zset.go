//
// t_zset.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
)

type MemberPair struct {
	Score  uint64
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
			score, err := util.Uint64ToBytes(mp.Score)
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
