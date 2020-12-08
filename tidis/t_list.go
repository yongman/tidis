//
// t_list.go
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
)

const (
	LHeadDirection uint8 = 0
	LTailDirection uint8 = 1

	LItemMinIndex uint64 = 1024
	LItemMaxIndex uint64 = 1<<64 - 1024

	LItemInitIndex uint64 = 1<<32 - 512
)

type ListObj struct {
	Object
	Head uint64
	Tail uint64
	Size uint64
}

func MarshalListObj(obj *ListObj) []byte {
	totalLen := 1 + 8 + 1 + 8 + 8 + 8
	raw := make([]byte, totalLen)

	idx := 0
	raw[idx] = obj.Type
	idx++
	_ = util.Uint64ToBytes1(raw[idx:], obj.ExpireAt)
	idx += 8
	raw[idx] = obj.Tomb
	idx++
	_ = util.Uint64ToBytes1(raw[idx:], obj.Head)

	idx += 8
	_ = util.Uint64ToBytes1(raw[idx:], obj.Tail)

	idx += 8
	_ = util.Uint64ToBytes1(raw[idx:], obj.Size)

	return raw
}

func UnmarshalListObj(raw []byte) (*ListObj, error) {
	if len(raw) != 34 {
		return nil, nil
	}
	obj := ListObj{}
	idx := 0
	obj.Type = raw[idx]
	if obj.Type != TLISTMETA {
		return nil, terror.ErrTypeNotMatch
	}
	idx++
	obj.ExpireAt, _ = util.BytesToUint64(raw[idx:])
	idx += 8
	obj.Tomb = raw[idx]
	idx++
	obj.Head, _ = util.BytesToUint64(raw[idx:])
	idx += 8
	obj.Tail, _ = util.BytesToUint64(raw[idx:])
	idx += 8
	obj.Size, _ = util.BytesToUint64(raw[idx:])
	return &obj, nil
}

func (tidis *Tidis) ListMetaObj(dbId uint8, txn, ss interface{}, key []byte) (*ListObj, bool, error) {
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
	obj, err := UnmarshalListObj(v)
	if err != nil {
		return nil, false, err
	}
	if obj.ObjectExpired(utils.Now()) {
		if txn == nil {
			tidis.Ldelete(dbId, key)
		} else {
			tidis.LdelWithTxn(dbId, txn, key)
		}

		return nil, true, nil
	}
	return obj, false, nil
}

func (tidis *Tidis) newListMetaObj() *ListObj {
	return &ListObj{
		Object: Object{
			Tomb:     0,
			Type:     TLISTMETA,
			ExpireAt: 0,
		},
		Head: LItemInitIndex,
		Tail: LItemInitIndex,
		Size: 0,
	}
}

func (tidis *Tidis) RawListKey(dbId uint8, key []byte, idx uint64) []byte {
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	listKey := append(keyPrefix, DataTypeKey)
	idxBytes, _ := util.Uint64ToBytes(idx)
	listKey = append(listKey, idxBytes...)

	return listKey
}

func (tidis *Tidis) Lpop(dbId uint8, txn interface{}, key []byte) ([]byte, error) {
	if txn == nil {
		return tidis.lPop(dbId, key, LHeadDirection)
	}

	return tidis.lPopWithTxn(dbId, txn, key, LHeadDirection)
}

func (tidis *Tidis) Lpush(dbId uint8, txn interface{}, key []byte, items ...[]byte) (uint64, error) {
	if txn == nil {
		return tidis.lPush(dbId, key, LHeadDirection, items...)
	}

	return tidis.lPushWithTxn(dbId, txn, key, LHeadDirection, items...)
}

func (tidis *Tidis) Rpop(dbId uint8, txn interface{}, key []byte) ([]byte, error) {
	if txn == nil {
		return tidis.lPop(dbId, key, LTailDirection)
	}

	return tidis.lPopWithTxn(dbId, txn, key, LTailDirection)
}

func (tidis *Tidis) Rpush(dbId uint8, txn interface{}, key []byte, items ...[]byte) (uint64, error) {
	if txn == nil {
		return tidis.lPush(dbId, key, LTailDirection, items...)
	}

	return tidis.lPushWithTxn(dbId, txn, key, LTailDirection, items...)
}

func (tidis *Tidis) Llen(dbId uint8, txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	return metaObj.Size, nil
}

func (tidis *Tidis) Lindex(dbId uint8, txn interface{}, key []byte, index int64) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	}

	if index >= 0 {
		if index >= int64(metaObj.Size) {
			// not exist
			return nil, nil
		}
	} else {
		if -index > int64(metaObj.Size) {
			// not exist
			return nil, nil
		}
		index = index + int64(metaObj.Size)
	}

	eDataKey := tidis.RawListKey(dbId, key, uint64(index)+metaObj.Head)

	return tidis.db.Get(eDataKey)
}

// return map[string][]byte key is encoded key, not user key
func (tidis *Tidis) Lrange(dbId uint8, txn interface{}, key []byte, start, stop int64) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	if start > stop && (stop > 0 || start < 0) {
		// empty range result
		return nil, nil
	}

	var (
		retMap map[string][]byte
		err    error
		ss     interface{}
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}
	// get meta first
	metaObj, _, err := tidis.ListMetaObj(dbId, txn, ss, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return EmptyListOrSet, nil
	}

	if start < 0 {
		if start < -int64(metaObj.Size) {
			// set start be first item index
			start = 0
		} else {
			start = start + int64(metaObj.Size)
		}
	} else {
		if start >= int64(metaObj.Size) {
			// empty result
			return nil, nil
		}
	}

	if stop < 0 {
		if stop < -int64(metaObj.Size) {
			// set stop be first item index
			stop = 0
		} else {
			// item index
			stop = stop + int64(metaObj.Size)
		}
	} else {
		if stop >= int64(metaObj.Size) {
			// set stop be last item index
			stop = int64(metaObj.Size) - 1
		}
	}

	// here start and stop both be positive
	if start > stop {
		return nil, nil
	}

	// generate batch request keys
	keys := make([][]byte, stop-start+1)

	for i := range keys {
		keys[i] = tidis.RawListKey(dbId, key, metaObj.Head+uint64(start)+uint64(i))
	}

	// batchget
	if txn == nil {
		retMap, err = tidis.db.MGetWithSnapshot(keys, ss)
	} else {
		retMap, err = tidis.db.MGetWithTxn(keys, txn)
	}
	if err != nil {
		return nil, err
	}

	// convert map to array by keys sort
	retSlice := make([]interface{}, len(keys))
	for i, k := range keys {
		v, ok := retMap[string(k)]
		if !ok {
			retSlice[i] = []byte(nil)
		} else {
			retSlice[i] = v
		}
	}

	return retSlice, nil
}

func (tidis *Tidis) Lset(dbId uint8, key []byte, index int64, value []byte) error {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return nil, tidis.LsetWithTxn(dbId, txn, key, index, value)
	}

	// execute txn func
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LsetWithTxn(dbId uint8, txn interface{}, key []byte, index int64, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return err
	}
	if metaObj == nil {
		metaObj = tidis.newListMetaObj()
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		if index >= 0 {
			if index >= int64(metaObj.Size) {
				// not exist
				return nil, terror.ErrOutOfIndex
			}
		} else {
			if -index > int64(metaObj.Size) {
				// not exist
				return nil, terror.ErrOutOfIndex
			}
			index = index + int64(metaObj.Size)
		}
		if index >= int64(metaObj.Size) {
			return nil, terror.ErrOutOfIndex
		}

		eDataKey := tidis.RawListKey(dbId, key, uint64(index)+metaObj.Head)

		// set item data
		err = txn.Set(eDataKey, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// execute txn func
	_, err = tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Ltrim(dbId uint8, key []byte, start, stop int64) error {
	//txn function
	f := func(txn interface{}) (interface{}, error) {
		return nil, tidis.LtrimWithTxn(dbId, txn, key, start, stop)
	}

	// execute func in txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LtrimWithTxn(dbId uint8, txn interface{}, key []byte, start, stop int64) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return err
	}
	if metaObj == nil {
		return nil
	}

	//txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var delKey bool

		if start < 0 {
			if start < -int64(metaObj.Size) {
				// set start be first item index
				start = 0
			} else {
				start = start + int64(metaObj.Size)
			}
		} else {
			if start >= int64(metaObj.Size) {
				// all keys will be delete
				delKey = true
			}
		}

		if stop < 0 {
			if stop < -int64(metaObj.Size) {
				// set stop be first item index
				stop = 0
			} else {
				// item index
				stop = stop + int64(metaObj.Size)
			}
		} else {
			if stop >= int64(metaObj.Size) {
				// set stop be last item index
				stop = int64(metaObj.Size) - 1
			}
		}

		if start > stop {
			delKey = true
		}

		if delKey {
			// delete meta key and all items
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}

			for i := start; i < stop; i++ {
				eDataKey := tidis.RawListKey(dbId, key, metaObj.Head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		} else {
			// update meta and delete other items
			head := metaObj.Head
			size := metaObj.Size

			metaObj.Head = metaObj.Head + uint64(start)
			metaObj.Tail = metaObj.Head + uint64(stop) + 1
			metaObj.Size = metaObj.Tail - metaObj.Head

			metaValue := MarshalListObj(metaObj)

			// update meta
			err = txn.Set(eMetaKey, metaValue)
			if err != nil {
				return nil, err
			}

			var i int64
			// delete front items
			for i = 0; i < start; i++ {
				eDataKey := tidis.RawListKey(dbId, key, head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}

			// delete backend items
			for i = stop; i < int64(size)-1; i++ {
				eDataKey := tidis.RawListKey(dbId, key, head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}

	// execute func in txn
	_, err = tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LdelWithTxn(dbId uint8, txn1 interface{}, key []byte) (int, error) {
	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	metaObj, _, err := tidis.ListMetaObj(dbId, txn1, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	// del meta key
	err = txn.Delete(eMetaKey)
	if err != nil {
		return 0, err
	}

	// del items
	for i := metaObj.Head; i < metaObj.Tail; i++ {
		eDataKey := tidis.RawListKey(dbId, key, i)

		err = txn.Delete(eDataKey)
		if err != nil {
			return 0, err
		}
	}
	return 1, nil
}

func (tidis *Tidis) Ldelete(dbId uint8, key []byte) (int, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.LdelWithTxn(dbId, txn, key)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

// head <----------------> tail
//
func (tidis *Tidis) lPop(dbId uint8, key []byte, direc uint8) ([]byte, error) {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.lPopWithTxn(dbId, txn, key, direc)
	}

	// execute txn func
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return nil, err
	}

	if ret == nil {
		return nil, nil
	}

	return ret.([]byte), nil
}

func (tidis *Tidis) lPopWithTxn(dbId uint8, txn interface{}, key []byte, direc uint8) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var eDataKey []byte

		// update meta
		if direc == LHeadDirection {
			eDataKey = tidis.RawListKey(dbId, key, metaObj.Head)
			metaObj.Head++
		} else {
			metaObj.Tail--
			eDataKey = tidis.RawListKey(dbId, key, metaObj.Tail)
		}
		metaObj.Size--

		if metaObj.Size == 0 {
			// only one item left, delete meta
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}
		} else {
			// update meta key
			// update meta, put item
			v := MarshalListObj(metaObj)
			err = txn.Set(eMetaKey, v)
			if err != nil {
				return nil, err
			}
		}

		// get item value
		item, err := txn.Get(eDataKey)
		if err != nil {
			if !kv.IsErrNotFound(err) {
				return nil, err
			}
			return nil, nil
		}

		// delete item
		err = txn.Delete(eDataKey)
		if err != nil {
			return nil, err
		}

		return item, nil
	}

	// execute txn func
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return nil, err
	}

	if ret == nil {
		return nil, nil
	}

	retByte, ok := ret.([]byte)
	if !ok {
		return nil, terror.ErrTypeAssertion
	}

	return retByte, nil
}

// head <--------------> tail
// meta [head, tail)
func (tidis *Tidis) lPush(dbId uint8, key []byte, direc uint8, items ...[]byte) (uint64, error) {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.lPushWithTxn(dbId, txn, key, direc, items...)
	}

	// run txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	if ret == nil {
		return 0, nil
	}

	return ret.(uint64), nil
}

func (tidis *Tidis) lPushWithTxn(dbId uint8, txn interface{}, key []byte, direc uint8, items ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)

	metaObj, _, err := tidis.ListMetaObj(dbId, txn, nil, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		metaObj = tidis.newListMetaObj()
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var index uint64

		// update key meta
		itemCnt := uint64(len(items))
		if direc == LHeadDirection {
			index = metaObj.Head
			metaObj.Head = metaObj.Head - itemCnt
		} else {
			index = metaObj.Tail
			metaObj.Tail = metaObj.Tail + itemCnt
		}
		metaObj.Size = metaObj.Size + itemCnt

		// encode meta value to bytes
		v := MarshalListObj(metaObj)

		// update meta, put item
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return nil, err
		}

		var eDataKey []byte

		for _, item := range items {
			// generate item key
			if direc == LHeadDirection {
				index--
				eDataKey = tidis.RawListKey(dbId, key, index)
			} else {
				eDataKey = tidis.RawListKey(dbId, key, index)
				index++
			}
			err = txn.Set(eDataKey, item)
			if err != nil {
				return nil, err
			}
		}
		return metaObj.Size, nil
	}

	// run txn
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	if ret == nil {
		return 0, nil
	}

	return ret.(uint64), nil
}
