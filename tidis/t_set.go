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
	"github.com/yongman/go/log"
	"github.com/yongman/tidis/terror"

	"github.com/deckarep/golang-set"
)

const (
	opDiff = iota
	opInter
	opUnion
)

func (tidis *Tidis) Sadd(key []byte, members ...[]byte) (uint64, error) {
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SaddWithTxn(txn, key, members...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SaddWithTxn(txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		if err := tidis.SdeleteIfExpired(txn, key); err != nil {
			return 0, err
		}
	}

	eMetaKey := SMetaEncoder(key)

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var (
			ssize uint64
			ttl   uint64
			flag  byte
			err   error
			added uint64
		)

		ssize, ttl, flag, err = tidis.sGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TSETMETA, key)
			return nil, terror.ErrKeyBusy
		}

		for _, member := range members {
			eDataKey := SDataEncoder(key, member)
			// check member exists
			v, err := tidis.db.GetWithTxn(eDataKey, txn)
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
		eMetaValue := tidis.sGenMeta(ssize+added, ttl, FNORMAL)
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

	return v.(uint64), nil
}

func (tidis *Tidis) Scard(txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		if err := tidis.SdeleteIfExpired(nil, key); err != nil {
			return 0, err
		}
	}

	eMetaKey := SMetaEncoder(key)

	ssize, _, flag, err := tidis.sGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TSETMETA, key)
		return 0, nil
	}

	return ssize, nil
}

func (tidis *Tidis) Sismember(txn interface{}, key, member []byte) (uint8, error) {
	if len(key) == 0 || len(member) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		if err := tidis.SdeleteIfExpired(nil, key); err != nil {
			return 0, err
		}
	}

	var (
		v    []byte
		flag byte
		err  error
	)
	eMetaKey := SMetaEncoder(key)
	_, _, flag, err = tidis.sGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}
	if flag == FDELETED {
		tidis.AsyncDelAdd(TSETMETA, key)
		return 0, nil
	}

	eDataKey := SDataEncoder(key, member)

	if txn == nil {
		v, err = tidis.db.Get(eDataKey)
	} else {
		v, err = tidis.db.GetWithTxn(eDataKey, txn)
	}
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return 1, nil
}

func (tidis *Tidis) Smembers(txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		if err := tidis.SdeleteIfExpired(nil, key); err != nil {
			return nil, err
		}
	}

	eMetaKey := SMetaEncoder(key)

	var (
		iss     interface{}
		err     error
		members [][]byte
	)

	if txn == nil {
		// get newest snapshot
		iss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	// get meta size
	ssize, _, flag, err := tidis.sGetMeta(eMetaKey, iss, txn)
	if err != nil {
		return nil, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TSETMETA, key)
		return EmptyListOrSet, nil
	}

	// get key range from startkey
	startKey := SDataEncoder(key, []byte(nil))

	if txn == nil {
		members, err = tidis.db.GetRangeKeys(startKey, nil, 0, ssize, iss)
	} else {
		members, err = tidis.db.GetRangeKeysWithTxn(startKey, nil, 0, ssize, txn)
	}
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

func (tidis *Tidis) skeyExists(metaKey []byte, ss, txn interface{}) (bool, error) {
	size, _, flag, err := tidis.sGetMeta(metaKey, ss, txn)
	if err != nil {
		return false, err
	}
	if size == 0 || flag == FDELETED {
		return false, nil
	}
	// TODO ttl
	return true, nil
}

func (tidis *Tidis) skeyExistsIgnoreFlag(metaKey []byte, ss, txn interface{}) (bool, uint64, error) {
	size, _, _, err := tidis.sGetMeta(metaKey, ss, txn)
	if err != nil {
		return false, 0, err
	}
	if size == 0 {
		return false, 0, nil
	}
	// TODO ttl
	return true, size, nil
}

func (tidis *Tidis) Srem(key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)
	// check key exists
	exists, err := tidis.skeyExists(eMetaKey, nil, nil)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}

	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SremWithTxn(txn, key, members...)
	}

	// execute txn
	v1, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v1.(uint64), nil
}

func (tidis *Tidis) SremWithTxn(txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}
	if tidis.LazyCheck() {
		if err := tidis.SdeleteIfExpired(nil, key); err != nil {
			return 0, err
		}
	}

	var removed uint64

	eMetaKey := SMetaEncoder(key)
	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		for _, member := range members {
			// check exists
			eDataKey := SDataEncoder(key, member)
			v, err := tidis.db.GetWithTxn(eDataKey, txn)
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
			ssize, ttl, flag, err := tidis.sGetMeta(eMetaKey, nil, txn)
			if err != nil {
				return nil, err
			}
			if flag == FDELETED {
				tidis.AsyncDelAdd(TSETMETA, key)
				return 0, nil
			}
			if ssize < removed {
				return nil, terror.ErrInvalidMeta
			}
			ssize = ssize - removed
			// update meta
			if ssize > 0 {
				eMetaValue := tidis.sGenMeta(ssize, ttl, FNORMAL)
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
	v1, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v1.(uint64), nil
}

func (tidis *Tidis) newSetsFromKeys(ss, txn interface{}, keys ...[]byte) ([]mapset.Set, error) {
	mss := make([]mapset.Set, len(keys))

	var (
		eMetaKey []byte
		members  [][]byte
	)

	for i, k := range keys {
		eMetaKey = SMetaEncoder(k)

		ssize, _, flag, err := tidis.sGetMeta(eMetaKey, ss, txn)
		if err != nil {
			return nil, err
		}

		if ssize == 0 {
			// key not exists
			mss[i] = nil
			continue
		}
		if flag == FDELETED {
			mss[i] = nil
			continue
		}

		startKey := SDataEncoder(k, []byte(nil))

		if txn == nil {
			members, err = tidis.db.GetRangeKeys(startKey, nil, 0, ssize, ss)
		} else {
			members, err = tidis.db.GetRangeKeysWithTxn(startKey, nil, 0, ssize, txn)
		}
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

func (tidis *Tidis) Sops(txn interface{}, opType int, keys ...[]byte) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss  interface{}
		err error
	)
	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	for _, key := range keys {
		if tidis.LazyCheck() {
			if err = tidis.SdeleteIfExpired(nil, key); err != nil {
				return nil, err
			}
		}
	}

	mss, err := tidis.newSetsFromKeys(ss, txn, keys...)
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

func (tidis *Tidis) Sdiff(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(txn, opDiff, keys...)
}

func (tidis *Tidis) Sinter(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(txn, opInter, keys...)
}

func (tidis *Tidis) Sunion(txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(txn, opUnion, keys...)
}

func (tidis *Tidis) SclearKeyWithTxn(txn1 interface{}, key []byte, async *bool, lazyCheckConstraint bool) (int, error) {
	eMetaKey := SMetaEncoder(key)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	if tidis.LazyCheck() && lazyCheckConstraint {
		if err := tidis.SdeleteIfExpired(nil, key); err != nil {
			return 0, err
		}
	}

	// check key exists
	ssize, ttl, _, err := tidis.sGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}
	if ssize == 0 {
		// not exists, just continue
		return 0, nil
	}

	if *async {
		// mark meta key deleted
		v := tidis.sGenMeta(ssize, ttl, FDELETED)
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return 0, err
		}
	} else {
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
	}

	return 1, nil
}

func (tidis *Tidis) Sclear(async bool, keys ...[]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// clear all keys in one txn
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SclearWithTxn(&async, true, txn, keys...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	if async {
		// notify async task
		for _, key := range keys {
			tidis.AsyncDelAdd(TSETMETA, key)
		}
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SclearWithTxn(async *bool, lazyCheckConstraint bool, txn interface{}, keys ...[]byte) (uint64, error) {
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

		var deleted uint64

		// clear each key
		for _, key := range keys {
			eMetaKey := SMetaEncoder(key)
			exists, ssize, err := tidis.skeyExistsIgnoreFlag(eMetaKey, nil, txn)
			if err != nil {
				return 0, err
			}

			if !exists {
				continue
			}

			if len(keys) == 1 && ssize < 1024 {
				// convert to sync deletion for small set key
				*async = false
			}

			_, err = tidis.SclearKeyWithTxn(txn, key, async, lazyCheckConstraint)
			if err != nil {
				return 0, err
			}

			deleted++
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

func (tidis *Tidis) SopsStore(opType int, dest []byte, keys ...[]byte) (uint64, error) {
	// write in txn
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SopsStoreWithTxn(txn, opType, dest, keys...)
	}

	// execute in txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SopsStoreWithTxn(txn interface{}, opType int, dest []byte, keys ...[]byte) (uint64, error) {
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

		for _, key := range keys {
			if tidis.LazyCheck() {
				if err := tidis.SdeleteIfExpired(nil, key); err != nil {
					return 0, nil
				}
			}
		}

		// result is opSet
		ssize, _, flag, err := tidis.sGetMeta(eDestMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if flag == FDELETED {
			tidis.AsyncDelAdd(TSETMETA, dest)
			return nil, terror.ErrKeyBusy
		}

		// get result set from keys ops
		mss, err := tidis.newSetsFromKeys(nil, txn, keys...)
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
		eDestMetaValue := tidis.sGenMeta(uint64(opSet.Cardinality()), 0, FNORMAL)
		err = txn.Set(eDestMetaKey, eDestMetaValue)
		if err != nil {
			return nil, err
		}

		return uint64(opSet.Cardinality()), nil
	}

	// execute in txn
	v, err := tidis.db.BatchWithTxn(f, txn)
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

func (tidis *Tidis) SdiffstoreWithTxn(txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(txn, opDiff, dest, keys...)
}

func (tidis *Tidis) SinterstoreWithTxn(txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(txn, opInter, dest, keys...)
}

func (tidis *Tidis) SunionstoreWithTxn(txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(txn, opUnion, dest, keys...)
}

// Meta data format same as hash type
func (tidis *Tidis) sGetMeta(key []byte, ss interface{}, txn interface{}) (uint64, uint64, byte, error) {
	return tidis.hGetMeta(key, ss, txn)
}

func (tidis *Tidis) sGenMeta(size, ttl uint64, flag byte) []byte {
	return tidis.hGenMeta(size, ttl, flag)
}

func (tidis *Tidis) SPExpireAt(key []byte, ts int64) (int, error) {
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SPExpireAtWithTxn(txn, key, ts)
	}

	// execute txn f
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) SPExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
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

		sMetaKey = SMetaEncoder(key)
		ssize, ttl, flag, err := tidis.sGetMeta(sMetaKey, nil, txn)
		if err != nil {
			return 0, err
		}

		if ssize == 0 {
			// key not exists
			return 0, nil
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TSETMETA, key)
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
		sMetaValue := tidis.sGenMeta(ssize, uint64(ts), FNORMAL)
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
	v, err := tidis.db.BatchWithTxn(f, txn)
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

func (tidis *Tidis) SPExpireWithTxn(txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.SPExpireAtWithTxn(txn, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) SExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.SPExpireAtWithTxn(txn, key, ts*1000)
}

func (tidis *Tidis) SExpireWithTxn(txn interface{}, key []byte, s int64) (int, error) {
	return tidis.SPExpireWithTxn(txn, key, s*1000)
}

func (tidis *Tidis) SPTtl(txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := SMetaEncoder(key)

	ssize, ttl, flag, err := tidis.sGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if ssize == 0 {
		// key not exists
		return -2, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TSETMETA, key)
		return -2, nil
	}

	if ttl == 0 {
		// no expire associated
		return -1, nil
	}

	var ts int64
	ts = int64(ttl) - time.Now().UnixNano()/1000/1000
	if ts < 0 {
		err = tidis.sdeleteIfNeeded(txn, key, false)
		if err != nil {
			return 0, err
		}
		return -2, nil
	}

	return ts, nil
}

func (tidis *Tidis) STtl(txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.SPTtl(txn, key)
	if ttl < 0 {
		return ttl, err
	}
	return ttl / 1000, err
}

func (tidis *Tidis) SdeleteIfExpired(txn interface{}, key []byte) error {
	ttl, err := tidis.STtl(txn, key)
	if err != nil {
		return err
	}
	if ttl != 0 {
		return nil
	}

	log.Debugf("Lazy deletion set key %v", key)

	return tidis.sdeleteIfNeeded(txn, key, false)

}

func (tidis *Tidis) SclearExpire(txn interface{}, key []byte) error {
	ttl, err := tidis.STtl(txn, key)
	if err != nil {
		return err
	}

	if ttl < 0 {
		return nil
	}

	// clear ttl field
	if _, err := tidis.SExpireAtWithTxn(txn, key, 0); err != nil {
		return err
	}

	return tidis.sdeleteIfNeeded(txn, key, true)
}

func (tidis *Tidis) sdeleteIfNeeded(txn interface{}, key []byte, expireOnly bool) error {
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get ts of the key
		sMetaKey := SMetaEncoder(key)

		size, ttl, _, err := tidis.sGetMeta(sMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			// already deleted
			return nil, nil
		}

		tMetaKey := TMSEncoder(key, uint64(ttl))

		// delete tMetaKey/entire hashkey
		if err = txn.Delete(tMetaKey); err != nil {
			return nil, err
		}

		if !expireOnly {
			False := false
			_, err = tidis.SclearWithTxn(&False, false, txn, key)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	}

	var err error
	if txn == nil {
		_, err = tidis.db.BatchInTxn(f)
	} else {
		_, err = tidis.db.BatchWithTxn(f, txn)
	}

	return err
}
