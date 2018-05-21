//
// t_set.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"time"

	"github.com/pingcap/tidb/kv"
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
			ttl   uint64
			err   error
			added uint64 = 0
		)

		ssize, ttl, err = tidis.sGetMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
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
		eMetaValue := tidis.sGenMeta(ssize+added, ttl)
		err = txn.Set(eMetaKey, eMetaValue)
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

	ssize, _, err := tidis.sGetMeta(eMetaKey, nil)
	if err != nil {
		return 0, err
	}
	return ssize, nil
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
	ssize, _, err := tidis.sGetMeta(eMetaKey, iss)
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
			ssize, ttl, err := tidis.sGetMeta(eMetaKey, ss)
			if err != nil {
				return nil, err
			}
			if ssize < removed {
				return nil, terror.ErrInvalidMeta
			}
			ssize = ssize - removed
			// update meta
			if ssize > 0 {
				eMetaValue := tidis.sGenMeta(ssize, ttl)
				err = txn.Set(eMetaKey, eMetaValue)
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

		ssize, _, err := tidis.sGetMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		if ssize == 0 {
			// key not exists
			mss[i] = nil
			continue
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

func (tidis *Tidis) SclearWithTxn(key []byte, txn1 interface{}) (int, error) {
	eMetaKey := SMetaEncoder(key)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	ss := txn.GetSnapshot()

	// check key exists
	ssize, _, err := tidis.sGetMeta(eMetaKey, ss)
	if err != nil {
		return 0, err
	}
	if ssize == 0 {
		// not exists, just continue
		return 0, nil
	}

	// delete meta key and all members
	err = txn.Delete(eMetaKey)
	if err != nil {
		return 0, err
	}

	startKey := SDataEncoder(key, []byte(nil))
	_, err = tidis.db.DeleteRangeWithTxn(startKey, nil, ssize, txn)
	if err != nil {
		return 0, err
	}

	return 1, nil
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

		// clear each key
		for _, key := range keys {
			_, err := tidis.SclearWithTxn(key, txn)
			if err != nil {
				return 0, err
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
		ssize, _, err := tidis.sGetMeta(eDestMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if ssize != 0 {
			// dest key exists, delete it first
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
		eDestMetaValue := tidis.sGenMeta(uint64(opSet.Cardinality()), 0)
		err = txn.Set(eDestMetaKey, eDestMetaValue)
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

// Meta data format same as hash type
func (tidis *Tidis) sGetMeta(key []byte, ss1 interface{}) (uint64, uint64, error) {
	return tidis.hGetMeta(key, ss1)
}

func (tidis *Tidis) sGenMeta(size, ttl uint64) []byte {
	return tidis.hGenMeta(size, ttl)
}

func (tidis *Tidis) SPExpireAt(key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			sMetaKey []byte
			tMetaKey []byte
		)

		ss := txn.GetSnapshot()
		sMetaKey = SMetaEncoder(key)
		ssize, ttl, err := tidis.sGetMeta(sMetaKey, ss)
		if err != nil {
			return 0, err
		}

		if ssize == 0 {
			// key not exists
			return 0, nil
		}

		// check expire time already set before
		if ttl != 0 {
			tMetaKey = TMSetEncoder(key, ttl)
			if err = txn.Delete(tMetaKey); err != nil {
				return 0, err
			}
		}

		// update set meta key and ttl meta key
		sMetaValue := tidis.sGenMeta(ssize, uint64(ts))
		if err = txn.Set(sMetaKey, sMetaValue); err != nil {
			return 0, err
		}

		tMetaKey = TMSetEncoder(key, uint64(ts))
		if err = txn.Set(tMetaKey, []byte{0}); err != nil {
			return 0, err
		}

		return 1, nil
	}

	// execute txn f
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) SPExpire(key []byte, ms int64) (int, error) {
	return tidis.SPExpireAt(key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) SExpireAt(key []byte, ts int64) (int, error) {
	return tidis.SPExpireAt(key, ts*1000)
}

func (tidis *Tidis) SExpire(key []byte, s int64) (int, error) {
	return tidis.SPExpire(key, s*1000)
}

func (tidis *Tidis) SPTtl(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return 0, err
	}

	eMetaKey := SMetaEncoder(key)

	ssize, ttl, err := tidis.sGetMeta(eMetaKey, ss)
	if err != nil {
		return 0, err
	}

	if ssize == 0 {
		// key not exists
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

func (tidis *Tidis) STtl(key []byte) (int64, error) {
	ttl, err := tidis.SPTtl(key)
	if ttl < 0 {
		return ttl, err
	} else {
		return ttl / 1000, err
	}
}
