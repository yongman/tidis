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
	"github.com/yongman/tidis/utils"

	"github.com/deckarep/golang-set"
)

const (
	opDiff = iota
	opInter
	opUnion
)

type SetObj struct {
	Object
	Size uint64
}

func MarshalSetObj(obj *SetObj) []byte {
	totalLen := 1 + 8 + 1 + 8
	raw := make([]byte, totalLen)

	idx := 0
	raw[idx] = obj.Type
	idx++
	util.Uint64ToBytes1(raw[idx:], obj.ExpireAt)
	idx += 8
	raw[idx] = obj.Tomb
	idx++
	util.Uint64ToBytes1(raw[idx:], obj.Size)

	return raw
}

func UnmarshalSetObj(raw []byte) (*SetObj, error) {
	if len(raw) != 18 {
		return nil, nil
	}
	obj := SetObj{}
	idx := 0
	obj.Type = raw[idx]
	if obj.Type != TSETMETA {
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

func (tidis *Tidis) RawSetDataKey(dbId uint8, key, member []byte) []byte {
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	dataKey := append(keyPrefix, DataTypeKey)
	dataKey = append(dataKey, member...)
	return dataKey
}

func (tidis *Tidis) SetMetaObj(dbId uint8, txn, ss interface{}, key []byte) (*SetObj, bool, error) {
	return tidis.SetMetaObjWithExpire(dbId, txn, ss, key, true)
}
func (tidis *Tidis) SetMetaObjWithExpire(dbId uint8, txn, ss interface{}, key []byte, checkExpire bool) (*SetObj, bool, error) {
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
	obj, err := UnmarshalSetObj(v)
	if err != nil {
		return nil, false, err
	}
	if checkExpire && obj.ObjectExpired(utils.Now()) {
		if txn == nil {
			tidis.Sclear(dbId, key)
		} else {
			tidis.SclearWithTxn(dbId, txn, key)
		}
		return nil, true, nil
	}
	return obj, false, nil
}

func (tidis *Tidis) newSetMetaObj() *SetObj {
	return &SetObj{
		Object: Object{
			ExpireAt: 0,
			Type:     TSETMETA,
			Tomb:     0,
		},
		Size: 0,
	}
}

func (tidis *Tidis) Sadd(dbId uint8, key []byte, members ...[]byte) (uint64, error) {
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SaddWithTxn(dbId, txn, key, members...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SaddWithTxn(dbId uint8, txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.SetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		metaObj = tidis.newSetMetaObj()
	}

	var added uint64

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		for _, member := range members {
			eDataKey := tidis.RawSetDataKey(dbId, key, member)
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
		metaObj.Size += added
		// update meta
		eMetaKey := tidis.RawKeyPrefix(dbId, key)
		eMetaValue := MarshalSetObj(metaObj)
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

func (tidis *Tidis) Scard(dbId uint8, txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.SetMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	return metaObj.Size, nil
}

func (tidis *Tidis) Sismember(dbId uint8, txn interface{}, key, member []byte) (uint8, error) {
	if len(key) == 0 || len(member) == 0 {
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

	metaObj, _, err := tidis.SetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	eDataKey := tidis.RawSetDataKey(dbId, key, member)

	var v []byte
	if txn == nil {
		v, err = tidis.db.GetWithSnapshot(eDataKey, ss)
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

func (tidis *Tidis) Smembers(dbId uint8, txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss      interface{}
		err     error
		members [][]byte
	)
	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	metaObj, _, err := tidis.SetMetaObj(dbId, txn, ss, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	}

	// get key range from startkey
	startKey := tidis.RawSetDataKey(dbId, key, []byte(nil))

	if txn == nil {
		members, err = tidis.db.GetRangeKeys(startKey, nil, 0, metaObj.Size, ss)
	} else {
		members, err = tidis.db.GetRangeKeysWithTxn(startKey, nil, 0, metaObj.Size, txn)
	}
	if err != nil {
		return nil, err
	}

	imembers := make([]interface{}, len(members))

	metaKeyLen := len(tidis.RawKeyPrefix(dbId, key))

	for i, member := range members {
		imembers[i] = member[metaKeyLen+1:]
	}

	return imembers, nil
}

func (tidis *Tidis) skeyExists(dbId uint8, metaKey []byte, ss, txn interface{}) (bool, error) {
	metaObj, _, err := tidis.SetMetaObj(dbId, txn, ss, metaKey)
	if err != nil {
		return false, err
	}
	if metaObj == nil {
		return false, nil
	}
	return true, nil
}

func (tidis *Tidis) Srem(dbId uint8, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SremWithTxn(dbId, txn, key, members...)
	}

	// execute txn
	v1, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v1.(uint64), nil
}

func (tidis *Tidis) SremWithTxn(dbId uint8, txn interface{}, key []byte, members ...[]byte) (uint64, error) {
	if len(key) == 0 || len(members) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var removed uint64

	metaObj, _, err := tidis.SetMetaObj(dbId, txn, nil, key)

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		for _, member := range members {
			// check exists
			eDataKey := tidis.RawSetDataKey(dbId, key, member)
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
			eMetaKey := tidis.RawKeyPrefix(dbId, key)
			// update meta
			metaObj.Size -= removed
			// update meta
			if metaObj.Size > 0 {
				eMetaValue := MarshalSetObj(metaObj)
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

func (tidis *Tidis) newSetsFromKeys(dbId uint8, ss, txn interface{}, keys ...[]byte) ([]mapset.Set, error) {
	mss := make([]mapset.Set, len(keys))

	var (
		members [][]byte
	)

	for i, k := range keys {
		metaObj, _, err := tidis.SetMetaObj(dbId, txn, ss, k)
		if err != nil {
			return nil, err
		}

		if metaObj == nil {
			// key not exists
			mss[i] = nil
			continue
		}

		startKey := tidis.RawSetDataKey(dbId, k, nil)

		if txn == nil {
			members, err = tidis.db.GetRangeKeys(startKey, nil, 0, metaObj.Size, ss)
		} else {
			members, err = tidis.db.GetRangeKeysWithTxn(startKey, nil, 0, metaObj.Size, txn)
		}
		if err != nil {
			return nil, err
		}

		keyPrefixLen := len(tidis.RawKeyPrefix(dbId, k))
		// create new set
		strMembers := make([]interface{}, len(members))
		for i, member := range members {
			if keyPrefixLen + 1 > len(member) {
				continue
			}
			s := member[keyPrefixLen+1:]
			strMembers[i] = string(s)
		}
		mss[i] = mapset.NewSet(strMembers...)
	}
	return mss, nil
}

func (tidis *Tidis) Sops(dbId uint8, txn interface{}, opType int, keys ...[]byte) ([]interface{}, error) {
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

	mss, err := tidis.newSetsFromKeys(dbId, ss, txn, keys...)
	if err != nil {
		return nil, err
	}

	var (
		opSet mapset.Set = nil
		i     int
	)

	for j, ms1 := range mss {
		if j == 0 && ms1 == nil && opType == opDiff {
			return make([]interface{}, 0), nil
		}
		if ms1 == nil {
			continue
		}
		if i == 0 {
			opSet = ms1
		} else {
			ms := ms1.(mapset.Set)
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
		i++
	}
	if opSet == nil {
		return make([]interface{}, 0), nil
	}

	return opSet.ToSlice(), nil
}

func (tidis *Tidis) Sdiff(dbId uint8, txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(dbId, txn, opDiff, keys...)
}

func (tidis *Tidis) Sinter(dbId uint8, txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(dbId, txn, opInter, keys...)
}

func (tidis *Tidis) Sunion(dbId uint8, txn interface{}, keys ...[]byte) ([]interface{}, error) {
	return tidis.Sops(dbId, txn, opUnion, keys...)
}

func (tidis *Tidis) SclearKeyWithTxn(dbId uint8, txn1 interface{}, key []byte) (int, error) {
	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	metaObj, _, err := tidis.SetMetaObjWithExpire(dbId, txn, nil, key, false)
	// check key exists
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		// not exists
		return 0, nil
	}

	// delete meta key and all members
	err = txn.Delete(eMetaKey)
	if err != nil {
		return 0, err
	}

	startKey := tidis.RawSetDataKey(dbId, key, nil)
	_, err = tidis.db.DeleteRangeWithTxn(startKey, nil, metaObj.Size, txn)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (tidis *Tidis) Sclear(dbId uint8, keys ...[]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// clear all keys in one txn
	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SclearWithTxn(dbId, txn, keys...)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SclearWithTxn(dbId uint8, txn interface{}, keys ...[]byte) (uint64, error) {
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
			_, err := tidis.SclearKeyWithTxn(dbId, txn, key)
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

func (tidis *Tidis) SopsStore(dbId uint8, opType int, dest []byte, keys ...[]byte) (uint64, error) {
	// write in txn
	f := func(txn interface{}) (interface{}, error) {
		return tidis.SopsStoreWithTxn(dbId, txn, opType, dest, keys...)
	}

	// execute in txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) SopsStoreWithTxn(dbId uint8, txn interface{}, opType int, dest []byte, keys ...[]byte) (uint64, error) {
	if len(dest) == 0 || len(keys) == 0 {
		return uint64(0), terror.ErrKeyEmpty
	}

	destMetaObj, _, err := tidis.SetMetaObj(dbId, txn, nil, dest)
	if err != nil {
		return uint64(0), err
	}

	// write in txn
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return uint64(0), terror.ErrBackendType
		}

		// get result set from keys ops
		mss, err := tidis.newSetsFromKeys(dbId, nil, txn, keys...)
		if err != nil {
			return uint64(0), err
		}

		var opSet mapset.Set
		var i int

		for j, ms1 := range mss {
			if j == 0 && ms1 == nil && opType == opDiff {
				return uint64(0), nil
			}
			if ms1 == nil {
				continue
			}
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
			i++
		}

		eDestMetaKey := tidis.RawKeyPrefix(dbId, dest)

		if destMetaObj != nil {
			// startkey
			startKey := tidis.RawSetDataKey(dbId, dest, nil)
			_, err = tidis.db.DeleteRangeWithTxn(startKey, nil, destMetaObj.Size, txn)
			if err != nil {
				return uint64(0), err
			}
			err = txn.Delete(eDestMetaKey)
			if err != nil {
				return uint64(0), err
			}
		} else {
			destMetaObj = tidis.newSetMetaObj()
		}
		if opSet == nil || opSet.Cardinality() == 0 {
			return uint64(0), nil
		}

		// save opset to new dest key
		for _, member := range opSet.ToSlice() {
			eDataKey := tidis.RawSetDataKey(dbId, dest, []byte(member.(string)))
			err = txn.Set(eDataKey, []byte{0})
			if err != nil {
				return uint64(0), err
			}
		}
		// save dest meta key
		destMetaObj.Size = uint64(opSet.Cardinality())
		eDestMetaValue := MarshalSetObj(destMetaObj)
		err = txn.Set(eDestMetaKey, eDestMetaValue)
		if err != nil {
			return uint64(0), err
		}

		return destMetaObj.Size, nil
	}

	// execute in txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(uint64), nil
}

func (tidis *Tidis) Sdiffstore(dbId uint8, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(dbId, opDiff, dest, keys...)
}

func (tidis *Tidis) Sinterstore(dbId uint8, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(dbId, opInter, dest, keys...)
}

func (tidis *Tidis) Sunionstore(dbId uint8, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStore(dbId, opUnion, dest, keys...)
}

func (tidis *Tidis) SdiffstoreWithTxn(dbId uint8, txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(dbId, txn, opDiff, dest, keys...)
}

func (tidis *Tidis) SinterstoreWithTxn(dbId uint8, txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(dbId, txn, opInter, dest, keys...)
}

func (tidis *Tidis) SunionstoreWithTxn(dbId uint8, txn interface{}, dest []byte, keys ...[]byte) (uint64, error) {
	return tidis.SopsStoreWithTxn(dbId, txn, opUnion, dest, keys...)
}
