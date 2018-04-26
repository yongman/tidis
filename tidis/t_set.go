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

	"github.com/deckarep/golang-set"
)

const (
	opDiff = iota
	opInter
	opUnion
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
	ssizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
	if err != nil {
		return nil, err
	}

	if ssizeRaw == nil {
		return nil, nil
	}

	ssize, err := util.BytesToUint64(ssizeRaw)
	if err != nil {
		return nil, err
	}

	// get key range from startkey
	startKey := SDataEncoder(key, []byte(nil))

	members, err := tidis.db.GetRangeKeys(startKey, nil, 0, ssize, ss)
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

func (tidis *Tidis) Srem(key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)

	// check key exists before 2pc
	v, err := tidis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}

	var removed uint64 = 0

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}
		ss := txn.GetSnapshot()

		for _, member := range members {
			// check exists
			eDataKey := SDataEncoder(key, member)
			v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
			if err != nil {
				return nil, err
			}
			if v != nil {
				removed++
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		}

		if removed > 0 {
			// update meta
			ssizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
			if err != nil {
				return nil, err
			}
			ssize, err := util.BytesToUint64(ssizeRaw)
			if err != nil {
				return nil, err
			}
			if ssize < removed {
				return nil, terror.ErrInvalidMeta
			}
			ssize = ssize - removed
			// update meta
			if ssize > 0 {
				ssizeRaw, _ := util.Uint64ToBytes(ssize)
				err = txn.Set(eMetaKey, ssizeRaw)
				if err != nil {
					return nil, err
				}
			} else {
				// ssize == 0, delete meta
				err = txn.Delete(eMetaKey)
				if err != nil {
					return nil, err
				}
			}
		}

		return removed, nil
	}

	// execute txn
	v1, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v1.(uint64), nil
}

func (tidis *Tidis) newSetsFromKeys(ss kv.Snapshot, keys ...[]byte) ([]mapset.Set, error) {
	mss := make([]mapset.Set, len(keys))

	var (
		eMetaKey []byte
	)

	for i, k := range keys {
		eMetaKey = SMetaEncoder(k)

		ssizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		if ssizeRaw == nil {
			// key not exists
			mss[i] = nil
			continue
		}

		ssize, err := util.BytesToUint64(ssizeRaw)
		if err != nil {
			return nil, err
		}

		startKey := SDataEncoder(k, []byte(nil))

		members, err := tidis.db.GetRangeKeys(startKey, nil, 0, ssize, ss)
		if err != nil {
			return nil, err
		}

		// create new set
		strMembers := make([]interface{}, len(members))
		for i, member := range members {
			_, s, _ := SDataDecoder(member)
			strMembers[i] = string(s)
		}
		mss[i] = mapset.NewSet(strMembers...)
	}
	return mss, nil
}

func (tidis *Tidis) Sops(opType int, keys ...[]byte) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	iss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}
	ss, ok := iss.(kv.Snapshot)
	if !ok {
		return nil, terror.ErrBackendType
	}

	mss, err := tidis.newSetsFromKeys(ss, keys...)
	if err != nil {
		return nil, err
	}

	var opSet mapset.Set

	for i, ms1 := range mss {
		ms := ms1.(mapset.Set)
		if i == 0 {
			opSet = ms
		} else {
			switch opType {
			case opDiff:
				opSet = opSet.Difference(ms)
				break
			case opInter:
				opSet = opSet.Intersect(ms)
				break
			case opUnion:
				opSet = opSet.Union(ms)
				break
			}
		}
	}

	return opSet.ToSlice(), nil
}

func (tidis *Tidis) Sdiff(keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(opDiff, keys...)
}

func (tidis *Tidis) Sinter(keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(opInter, keys...)
}

func (tidis *Tidis) Sunion(keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(opUnion, keys...)
}

func (tidis *Tidis) Sclear(keys ...[]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// clear all keys in one txn
	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var deleted uint64 = 0

		ss := txn.GetSnapshot()

		// clear each key
		for _, key := range keys {
			eMetaKey := SMetaEncoder(key)

			// check key exists
			v, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
			if err != nil {
				return nil, err
			}
			if v == nil {
				// not exists, just continue
				continue
			}

			// delete meta key and all members
			ssize, err := util.BytesToUint64(v)
			if err != nil {
				return nil, err
			}

			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}

			startKey := SDataEncoder(key, []byte(nil))
			_, err = tidis.db.DeleteRangeWithTxn(startKey, nil, ssize, txn)
			if err != nil {
				return nil, err
			}

			deleted++
		}

		return deleted, nil
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SopsStore(opType int, dest []byte, keys ...[]byte) (uint64, error) {
	if len(dest) == 0 || len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eDestMetaKey := SMetaEncoder(dest)

	// write in txn
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}
		ss := txn.GetSnapshot()

		// get result set from keys ops
		mss, err := tidis.newSetsFromKeys(ss, keys...)
		if err != nil {
			return nil, err
		}

		var opSet mapset.Set

		for i, ms1 := range mss {
			ms := ms1.(mapset.Set)
			if i == 0 {
				opSet = ms
			} else {
				switch opType {
				case opDiff:
					opSet = opSet.Difference(ms)
					break
				case opInter:
					opSet = opSet.Intersect(ms)
					break
				case opUnion:
					opSet = opSet.Union(ms)
					break
				}
			}
		}
		// result is opSet
		v, err := tidis.db.GetWithSnapshot(eDestMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if v != nil {
			// dest key exists, delete it first
			ssize, err := util.BytesToUint64(v)
			if err != nil {
				return nil, err
			}

			// startkey
			startKey := SDataEncoder(dest, []byte(nil))
			_, err = tidis.db.DeleteRangeWithTxn(startKey, nil, ssize, txn)
			if err != nil {
				return nil, err
			}
		}

		// save opset to new dest key
		for _, member := range opSet.ToSlice() {
			eDataKey := SDataEncoder(dest, []byte(member.(string)))
			err = txn.Set(eDataKey, []byte{0})
			if err != nil {
				return nil, err
			}
		}
		// save dest meta key
		ssizeRaw, _ := util.Uint64ToBytes(uint64(opSet.Cardinality()))
		err = txn.Set(eDestMetaKey, ssizeRaw)
		if err != nil {
			return nil, err
		}

		return uint64(opSet.Cardinality()), nil
	}

	// execute in txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Sdiffstore(dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(opDiff, dest, keys...)
}

func (tidis *Tidis) Sinterstore(dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(opInter, dest, keys...)
}

func (tidis *Tidis) Sunionstore(dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(opUnion, dest, keys...)
}
