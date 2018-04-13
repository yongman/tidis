//
// t_list.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

import (
	"github.com/YongMan/go/util"
	"github.com/YongMan/tedis/terror"
	"github.com/pingcap/tidb/kv"
)

const (
	LHeadDirection uint8 = 0
	LTailDirection uint8 = 1

	LItemMinIndex uint64 = 1024
	LItemMaxIndex uint64 = 1<<64 - 1024

	LItemInitIndex uint64 = 1<<32 - 512
)

func (tedis *Tedis) Lpop(key []byte) ([]byte, error) {
	return tedis.lPop(key, LHeadDirection)
}

func (tedis *Tedis) Lpush(key []byte, items ...[]byte) (uint64, error) {
	return tedis.lPush(key, LHeadDirection, items...)
}

func (tedis *Tedis) Rpop(key []byte) ([]byte, error) {
	return tedis.lPop(key, LTailDirection)
}

func (tedis *Tedis) Rpush(key []byte, items ...[]byte) (uint64, error) {
	return tedis.lPush(key, LTailDirection, items...)
}

func (tedis *Tedis) Llen(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	_, _, size, err := tedis.lGetKeyMeta(eMetaKey, nil)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (tedis *Tedis) Lindex(key []byte, index int64) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	ss, err := tedis.db.GetNewestSnapshot()

	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, err := tedis.lGetKeyMeta(eMetaKey, ss)
	if err != nil {
		return nil, err
	}

	if index >= 0 {
		if index >= int64(size) {
			// not exist
			return nil, nil
		}
	} else {
		if -index > int64(size) {
			// not exist
			return nil, nil
		}
		index = index + int64(size)
	}

	eDataKey := LDataEncoder(key, uint64(index)+head)

	return tedis.db.Get(eDataKey)
}

// return map[string][]byte key is encoded key, not user key
func (tedis *Tedis) Lrange(key []byte, start, stop int64) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	if start > stop {
		// empty range result
		return nil, nil
	}

	ss, err := tedis.db.GetNewestSnapshot()

	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, err := tedis.lGetKeyMeta(eMetaKey, ss)
	if err != nil {
		return nil, err
	}

	if start < 0 {
		if start < -int64(size) {
			// set start be first item index
			start = 0
		} else {
			start = start + int64(size)
		}
	} else {
		if start >= int64(size) {
			// empty result
			return nil, nil
		}
	}

	if stop < 0 {
		if stop < -int64(size) {
			// set stop be first item index
			stop = 0
		} else {
			// item index
			stop = stop + int64(size)
		}
	} else {
		if stop >= int64(size) {
			// set stop be last item index
			stop = int64(size) - 1
		}
	}

	// here start and stop both be positive
	if start > stop {
		return nil, nil
	}

	// generate batch request keys
	keys := make([][]byte, stop-start+1)

	for i, _ := range keys {
		keys[i] = LDataEncoder(key, head+uint64(start)+uint64(i))
	}

	// batchget
	retMap, err := tedis.db.MGetWithSnapshot(keys, ss)
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

// head <----------------> tail
//
func (tedis *Tedis) lPop(key []byte, direc uint8) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get meta value from txn
		head, tail, size, err := tedis.lGetKeyMeta(eMetaKey, txn.GetSnapshot())
		if err != nil {
			return nil, err
		}

		// empty list, return nil
		if size == 0 {
			return nil, nil
		}

		var eDataKey []byte

		// update meta
		if direc == LHeadDirection {
			eDataKey = LDataEncoder(key, head)
			head++
		} else {
			eDataKey = LDataEncoder(key, tail)
			tail--
		}
		size--

		if size == 0 {
			// only one item left, delete meta
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}
		} else {
			// update meta key
			// encode meta value to bytes
			v, err := tedis.lGenarateKeyMeta(head, tail, size)
			if err != nil {
				return nil, err
			}

			// update meta, put item
			err = txn.Set(eMetaKey, v)
			if err != nil {
				return nil, err
			}
		}

		// get item value
		item, err := txn.GetSnapshot().Get(eDataKey)
		if err != nil {
			if !kv.IsErrNotFound(err) {
				return nil, err
			} else {
				return nil, nil
			}
		}

		// delete item
		err = txn.Delete(eDataKey)
		if err != nil {
			return nil, err
		}

		return item, nil
	}

	// execute txn func
	ret, err := tedis.db.BatchInTxn(f)
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
//
func (tedis *Tedis) lPush(key []byte, direc uint8, items ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var index uint64

		// get key meta from txn snapshot and decode if needed
		head, tail, size, err := tedis.lGetKeyMeta(eMetaKey, txn.GetSnapshot())
		if err != nil {
			return nil, err
		}

		// update key meta
		itemCnt := uint64(len(items))
		if direc == LHeadDirection {
			index = head
			head = head - itemCnt
		} else {
			index = tail
			tail = tail + itemCnt
		}
		size = size + itemCnt

		// encode meta value to bytes
		v, err := tedis.lGenarateKeyMeta(head, tail, size)
		if err != nil {
			return nil, err
		}

		// update meta, put item
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return nil, err
		}

		for _, item := range items {
			// generate item key
			if direc == LHeadDirection {
				index--
			} else {
				index++
			}
			eDataKey := LDataEncoder(key, index)
			err = txn.Set(eDataKey, item)
			if err != nil {
				return nil, err
			}
		}
		return size, nil
	}

	// run txn
	ret, err := tedis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	if ret == nil {
		return 0, nil
	}

	retInt, ok := ret.(uint64)
	if !ok {
		return 0, terror.ErrTypeAssertion
	}

	return retInt, nil
}

// get meta for a list key
// return initial meta if not exist
// ss is used by write transaction, nil for read
func (tedis *Tedis) lGetKeyMeta(ekey []byte, ss interface{}) (uint64, uint64, uint64, error) {
	if len(ekey) == 0 {
		return 0, 0, 0, terror.ErrKeyEmpty
	}

	var (
		head uint64
		tail uint64
		size uint64
		err  error
		v    []byte
	)

	// value format head(8)|tail(8)|size(8)
	if ss == nil {
		v, err = tedis.db.Get(ekey)
	} else {
		ss1, ok := ss.(kv.Snapshot)
		if !ok {
			return 0, 0, 0, terror.ErrBackendType
		}
		v, err = ss1.Get(ekey)
	}
	if err != nil && !kv.IsErrNotFound(err) {
		return 0, 0, 0, err
	}
	if v == nil {
		// not exist
		head = LItemInitIndex
		tail = LItemInitIndex
		size = 0
	} else {
		head, err = util.BytesToUint64(v[0:])
		if err != nil {
			return 0, 0, 0, err
		}
		tail, err = util.BytesToUint64(v[8:])
		if err != nil {
			return 0, 0, 0, err
		}
		size, err = util.BytesToUint64(v[16:])
		if err != nil {
			return 0, 0, 0, err
		}
	}
	return head, tail, size, nil
}

// return  meta value bytes for a list key
// meta key and item key must be execute in one txn funcion
func (tedis *Tedis) lGenarateKeyMeta(head, tail, size uint64) ([]byte, error) {
	buf := make([]byte, 24)

	err := util.Uint64ToBytes1(buf[0:], head)
	if err != nil {
		return nil, err
	}

	err = util.Uint64ToBytes1(buf[8:], tail)
	if err != nil {
		return nil, err
	}

	err = util.Uint64ToBytes1(buf[16:], size)

	if err != nil {
		return nil, err
	}

	return buf, nil
}
