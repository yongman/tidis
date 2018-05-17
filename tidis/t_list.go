//
// t_list.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
)

const (
	LHeadDirection uint8 = 0
	LTailDirection uint8 = 1

	LItemMinIndex uint64 = 1024
	LItemMaxIndex uint64 = 1<<64 - 1024

	LItemInitIndex uint64 = 1<<32 - 512
)

func (tidis *Tidis) Lpop(key []byte) ([]byte, error) {
	return tidis.lPop(key, LHeadDirection)
}

func (tidis *Tidis) Lpush(key []byte, items ...[]byte) (uint64, error) {
	return tidis.lPush(key, LHeadDirection, items...)
}

func (tidis *Tidis) Rpop(key []byte) ([]byte, error) {
	return tidis.lPop(key, LTailDirection)
}

func (tidis *Tidis) Rpush(key []byte, items ...[]byte) (uint64, error) {
	return tidis.lPush(key, LTailDirection, items...)
}

func (tidis *Tidis) Llen(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	_, _, size, _, err := tidis.lGetKeyMeta(eMetaKey, nil)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (tidis *Tidis) Lindex(key []byte, index int64) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()

	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, _, err := tidis.lGetKeyMeta(eMetaKey, ss)
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

	return tidis.db.Get(eDataKey)
}

// return map[string][]byte key is encoded key, not user key
func (tidis *Tidis) Lrange(key []byte, start, stop int64) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}
	if start > stop && (stop > 0 || start < 0) {
		// empty range result
		return nil, nil
	}

	ss, err := tidis.db.GetNewestSnapshot()

	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, _, err := tidis.lGetKeyMeta(eMetaKey, ss)
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
	retMap, err := tidis.db.MGetWithSnapshot(keys, ss)
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

func (tidis *Tidis) Lset(key []byte, index int64, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		ss := txn.GetSnapshot()

		// get meta first
		head, _, size, _, err := tidis.lGetKeyMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		if index >= 0 {
			if index >= int64(size) {
				// not exist
				return nil, terror.ErrOutOfIndex
			}
		} else {
			if -index > int64(size) {
				// not exist
				return nil, terror.ErrOutOfIndex
			}
			index = index + int64(size)
		}
		if index >= int64(size) {
			return nil, terror.ErrOutOfIndex
		}

		eDataKey := LDataEncoder(key, uint64(index)+head)

		// set item data
		err = txn.Set(eDataKey, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// execute txn func
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Ltrim(key []byte, start, stop int64) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	//txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var delKey bool = false

		ss := txn.GetSnapshot()

		head, _, size, ttl, err := tidis.lGetKeyMeta(eMetaKey, ss)
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
				// all keys will be delete
				delKey = true
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
				eDataKey := LDataEncoder(key, head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		} else {
			// update meta and delete other items
			nhead := head + uint64(start)
			ntail := head + uint64(stop) + 1
			size := ntail - nhead

			v, err := tidis.lGenKeyMeta(nhead, ntail, size, ttl)
			if err != nil {
				return nil, err
			}

			// update meta
			err = txn.Set(eMetaKey, v)
			if err != nil {
				return nil, err
			}

			var i int64
			// delete front items
			for i = 0; i < start; i++ {
				eDataKey := LDataEncoder(key, head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}

			// delete backend items
			for i = stop; i < int64(size)-1; i++ {
				eDataKey := LDataEncoder(key, head+uint64(i))
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}

	// execute func in txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Ldelete(key []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get meta info
		head, tail, _, _, err := tidis.lGetKeyMeta(eMetaKey, txn.GetSnapshot())
		if err != nil {
			return nil, err
		}

		// del meta key
		err = txn.Delete(eMetaKey)
		if err != nil {
			return nil, err
		}

		// del items
		for i := head; i < tail; i++ {
			eDataKey := LDataEncoder(key, i)

			err = txn.Delete(eDataKey)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	// execute txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return nil
	}

	return nil
}

// head <----------------> tail
//
func (tidis *Tidis) lPop(key []byte, direc uint8) ([]byte, error) {
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
		head, tail, size, ttl, err := tidis.lGetKeyMeta(eMetaKey, txn.GetSnapshot())
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
			v, err := tidis.lGenKeyMeta(head, tail, size, ttl)
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
	ret, err := tidis.db.BatchInTxn(f)
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
func (tidis *Tidis) lPush(key []byte, direc uint8, items ...[]byte) (uint64, error) {
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
		head, tail, size, ttl, err := tidis.lGetKeyMeta(eMetaKey, txn.GetSnapshot())
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
		v, err := tidis.lGenKeyMeta(head, tail, size, ttl)
		if err != nil {
			return nil, err
		}

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
				eDataKey = LDataEncoder(key, index)
			} else {
				eDataKey = LDataEncoder(key, index)
				index++
			}
			err = txn.Set(eDataKey, item)
			if err != nil {
				return nil, err
			}
		}
		return size, nil
	}

	// run txn
	ret, err := tidis.db.BatchInTxn(f)
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
func (tidis *Tidis) lGetKeyMeta(ekey []byte, ss interface{}) (uint64, uint64, uint64, uint64, error) {
	if len(ekey) == 0 {
		return 0, 0, 0, 0, terror.ErrKeyEmpty
	}

	var (
		head uint64
		tail uint64
		size uint64
		ttl  uint64
		err  error
		v    []byte
	)

	// value format head(8)|tail(8)|size(8)|ttl(8)
	if ss == nil {
		v, err = tidis.db.Get(ekey)
	} else {
		ss1, ok := ss.(kv.Snapshot)
		if !ok {
			return 0, 0, 0, 0, terror.ErrBackendType
		}
		v, err = tidis.db.GetWithSnapshot(ekey, ss1)
	}
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if v == nil {
		// not exist
		head = LItemInitIndex
		tail = LItemInitIndex
		size = 0
		ttl = 0
	} else {
		head, err = util.BytesToUint64(v[0:])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		tail, err = util.BytesToUint64(v[8:])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		size, err = util.BytesToUint64(v[16:])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		ttl, err = util.BytesToUint64(v[24:])
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}
	return head, tail, size, ttl, nil
}

// return  meta value bytes for a list key
// meta key and item key must be execute in one txn funcion
func (tidis *Tidis) lGenKeyMeta(head, tail, size, ttl uint64) ([]byte, error) {
	buf := make([]byte, 32)

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

	err = util.Uint64ToBytes1(buf[24:], ttl)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (tidis *Tidis) LPExpireAt(key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			lMetaKey []byte
			tMetaKey []byte
		)

		ss := txn.GetSnapshot()
		lMetaKey = LMetaEncoder(key)
		head, tail, lsize, ttl, err := tidis.lGetKeyMeta(lMetaKey, ss)
		if err != nil {
			return 0, err
		}

		if lsize == 0 {
			// key not exists
			return 0, nil
		}

		// check expire time already set before
		if ttl != 0 {
			// delete ttl meta key first
			tMetaKey = TMLEncoder(key, ttl)
			if err = txn.Delete(tMetaKey); err != nil {
				return 0, err
			}
		}

		// update list meta key and set ttl meta key
		lMetaValue, _ := tidis.lGenKeyMeta(head, tail, lsize, uint64(ts))
		if err = txn.Set(lMetaKey, lMetaValue); err != nil {
			return 0, err
		}

		tMetaKey = TMLEncoder(key, uint64(ts))
		if err = txn.Set(lMetaKey, []byte{0}); err != nil {
			return 0, err
		}

		return 1, nil
	}

	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) LPExpire(key []byte, ms int64) (int, error) {
	return tidis.LPExpireAt(key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) LExpireAt(key []byte, ts int64) (int, error) {
	return tidis.LPExpireAt(key, ts*1000)
}

func (tidis *Tidis) LExpire(key []byte, s int64) (int, error) {
	return tidis.LPExpire(key, s*1000)
}

func (tidis *Tidis) LPTtl(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return 0, err
	}

	eMetaKey := LMetaEncoder(key)

	_, _, lsize, ttl, err := tidis.lGetKeyMeta(eMetaKey, ss)
	if err != nil {
		return 0, err
	}
	if lsize == 0 {
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
	}

	return ts, nil
}

func (tidis *Tidis) LTtl(key []byte) (int64, error) {
	ttl, err := tidis.LPTtl(key)
	if ttl < 0 {
		return ttl, err
	} else {
		return ttl / 1000, err
	}
}
