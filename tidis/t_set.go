//
// t_set.go
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

func (tidis *Tidis) Sadd(key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		ss := txn.GetSnapshot()

		var (
			ssize uint64
			err   error
			added uint64 = 0
		)

		ssizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		if ssizeRaw == nil {
			ssize = 0
		} else {
			ssize, err = util.BytesToUint64(ssizeRaw)
			if err != nil {
				return nil, err
			}
		}

		for _, member := range members {
			eDataKey := SDataEncoder(key, member)
			// check member exists
			v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
			if err != nil {
				return nil, err
			}
			if v != nil {
				// already exists
			} else {
				added++
				err = txn.Set(eDataKey, []byte{0})
				if err != nil {
					return nil, err
				}
			}
		}
		// update meta
		ssizeRaw, err = util.Uint64ToBytes(ssize + added)
		if err != nil {
			return nil, err
		}
		err = txn.Set(eMetaKey, ssizeRaw)
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

	return v.(uint64), nil
}

func (tidis *Tidis) Scard(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)

	v, err := tidis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	} else {
		ssize, err := util.BytesToUint64(v)
		if err != nil {
			return 0, err
		}
		return ssize, nil
	}
}

func (tidis *Tidis) Sismember(key, member []byte) (uint8, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eDataKey := SDataEncoder(key, member)

	v, err := tidis.db.Get(eDataKey)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	} else {
		return 1, nil
	}
}

func (tidis *Tidis) Smembers(key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)

	// get newest snapshot
	iss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}
	ss, ok := iss.(kv.Snapshot)
	if !ok {
		return nil, terror.ErrBackendType
	}

	// get meta size
	ssizeRaw, err := ss.Get(eMetaKey)
	if err != nil {
		return nil, err
	}

	ssize, err := util.BytesToUint64(ssizeRaw)
	if err != nil {
		return nil, err
	}

	// get key range from startkey
	startKey := SDataEncoder(key, []byte(nil))

	members, err := tidis.db.GetRangeKeys(startKey, nil, ssize, ss)
	if err != nil {
		return nil, err
	}

	imembers := make([]interface{}, len(members))

	for i, member := range members {
		_, imembers[i], err = SDataDecoder(member)
		if err != nil {
			return nil, err
		}
	}

	return imembers, nil
}
