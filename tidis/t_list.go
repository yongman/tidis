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
	"github.com/yongman/go/log"
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

func (tidis *Tidis) Lpop(txn interface{}, key []byte) ([]byte, error) {
	if txn == nil {
		return tidis.lPop(key, LHeadDirection)
	}

	return tidis.lPopWithTxn(txn, key, LHeadDirection)
}

func (tidis *Tidis) Lpush(txn interface{}, key []byte, items ...[]byte) (uint64, error) {
	if txn == nil {
		return tidis.lPush(key, LHeadDirection, items...)
	}

	return tidis.lPushWithTxn(txn, key, LHeadDirection, items...)
}

func (tidis *Tidis) Rpop(txn interface{}, key []byte) ([]byte, error) {
	if txn == nil {
		return tidis.lPop(key, LTailDirection)
	}

	return tidis.lPopWithTxn(txn, key, LTailDirection)
}

func (tidis *Tidis) Rpush(txn interface{}, key []byte, items ...[]byte) (uint64, error) {
	if txn == nil {
		return tidis.lPush(key, LTailDirection, items...)
	}

	return tidis.lPushWithTxn(txn, key, LTailDirection, items...)
}

func (tidis *Tidis) Llen(txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return 0, err
		}
	}

	eMetaKey := LMetaEncoder(key)

	_, _, size, _, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TLISTMETA, key)
		return 0, nil
	}

	return size, nil
}

func (tidis *Tidis) Lindex(txn interface{}, key []byte, index int64) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return nil, err
		}
	}

	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, _, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
	if err != nil {
		return nil, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TLISTMETA, key)
		return nil, nil
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
func (tidis *Tidis) Lrange(txn interface{}, key []byte, start, stop int64) ([]interface{}, error) {
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

	if tidis.LazyCheck() {
		err = tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return nil, err
		}
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}
	// get meta first
	eMetaKey := LMetaEncoder(key)
	head, _, size, _, flag, err := tidis.lGetKeyMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		return EmptyListOrSet, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TLISTMETA, key)
		return EmptyListOrSet, nil
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

	for i := range keys {
		keys[i] = LDataEncoder(key, head+uint64(start)+uint64(i))
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

func (tidis *Tidis) Lset(key []byte, index int64, value []byte) error {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return nil, tidis.LsetWithTxn(txn, key, index, value)
	}

	// execute txn func
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LsetWithTxn(txn interface{}, key []byte, index int64, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return err
		}
	}

	eMetaKey := LMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get meta first
		head, _, size, _, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn1)
		if err != nil {
			return nil, err
		}
		if flag == FDELETED {
			return nil, terror.ErrKeyBusy
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
	_, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Ltrim(key []byte, start, stop int64) error {
	//txn function
	f := func(txn interface{}) (interface{}, error) {
		return nil, tidis.LtrimWithTxn(txn, key, start, stop)
	}

	// execute func in txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LtrimWithTxn(txn interface{}, key []byte, start, stop int64) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return err
		}
	}
	eMetaKey := LMetaEncoder(key)

	//txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var delKey bool

		head, _, size, ttl, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn1)
		if err != nil {
			return nil, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TLISTMETA, key)
			return nil, terror.ErrKeyBusy
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

			v, err := tidis.lGenKeyMeta(nhead, ntail, size, ttl, FNORMAL)
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
	_, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) LdelWithTxn(txn1 interface{}, key []byte, async *bool) (int, error) {
	// check lazy data in case of return incorrect count
	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn1, key)
		if err != nil {
			return 0, err
		}
	}

	eMetaKey := LMetaEncoder(key)

	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return 0, terror.ErrBackendType
	}

	// get meta info
	head, tail, size, ttl, _, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}

	if *async && size < 1024 {
		// convert async deletion to sync for small list
		*async = false
	}

	if *async {
		// mark meta key as deleted
		v, _ := tidis.lGenKeyMeta(head, tail, size, ttl, FDELETED)
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return 0, err
		}
	} else {
		// del meta key
		err = txn.Delete(eMetaKey)
		if err != nil {
			return 0, err
		}

		// del items
		for i := head; i < tail; i++ {
			eDataKey := LDataEncoder(key, i)

			err = txn.Delete(eDataKey)
			if err != nil {
				return 0, err
			}
		}
	}
	return 1, nil
}

func (tidis *Tidis) Ldelete(key []byte, async bool) (int, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.LdelWithTxn(txn, key, &async)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	if async {
		// send key to async task after txn commit
		tidis.AsyncDelAdd(TLISTMETA, key)
	}

	return v.(int), nil
}

// head <----------------> tail
//
func (tidis *Tidis) lPop(key []byte, direc uint8) ([]byte, error) {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.lPopWithTxn(txn, key, direc)
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

func (tidis *Tidis) lPopWithTxn(txn interface{}, key []byte, direc uint8) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return nil, err
		}
	}

	eMetaKey := LMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get meta value from txn
		head, tail, size, ttl, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}

		// empty list, return nil
		if size == 0 {
			return nil, nil
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(TLISTMETA, key)
			return nil, nil
		}

		var eDataKey []byte

		// update meta
		if direc == LHeadDirection {
			eDataKey = LDataEncoder(key, head)
			head++
		} else {
			tail--
			eDataKey = LDataEncoder(key, tail)
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
			v, err := tidis.lGenKeyMeta(head, tail, size, ttl, FNORMAL)
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
func (tidis *Tidis) lPush(key []byte, direc uint8, items ...[]byte) (uint64, error) {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.lPushWithTxn(txn, key, direc, items...)
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

func (tidis *Tidis) lPushWithTxn(txn interface{}, key []byte, direc uint8, items ...[]byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	if tidis.LazyCheck() {
		err := tidis.LdeleteIfExpired(txn, key)
		if err != nil {
			return 0, err
		}
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
		head, tail, size, ttl, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if flag == FDELETED {
			tidis.AsyncDelAdd(TLISTMETA, key)
			return nil, terror.ErrKeyBusy
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
		v, err := tidis.lGenKeyMeta(head, tail, size, ttl, FNORMAL)
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
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	if ret == nil {
		return 0, nil
	}

	return ret.(uint64), nil
}

// get meta for a list key
// return initial meta if not exist
// ss is used by write transaction, nil for read
func (tidis *Tidis) lGetKeyMeta(ekey []byte, ss, txn interface{}) (uint64, uint64, uint64, uint64, byte, error) {
	if len(ekey) == 0 {
		return 0, 0, 0, 0, FNORMAL, terror.ErrKeyEmpty
	}

	var (
		head uint64
		tail uint64
		size uint64
		ttl  uint64
		flag byte
		err  error
		v    []byte
	)

	// value format head(8)|tail(8)|size(8)|ttl(8)|flag(1)
	if ss == nil && txn == nil {
		v, err = tidis.db.Get(ekey)
	} else if ss != nil {
		v, err = tidis.db.GetWithSnapshot(ekey, ss)
	} else {
		v, err = tidis.db.GetWithTxn(ekey, txn)
	}
	if err != nil {
		return 0, 0, 0, 0, FNORMAL, err
	}
	if v == nil {
		// not exist
		head = LItemInitIndex
		tail = LItemInitIndex
		size = 0
		flag = FNORMAL
		ttl = 0
	} else {
		head, err = util.BytesToUint64(v[0:])
		if err != nil {
			return 0, 0, 0, 0, FNORMAL, err
		}
		tail, err = util.BytesToUint64(v[8:])
		if err != nil {
			return 0, 0, 0, 0, FNORMAL, err
		}
		size, err = util.BytesToUint64(v[16:])
		if err != nil {
			return 0, 0, 0, 0, FNORMAL, err
		}
		ttl, err = util.BytesToUint64(v[24:])
		if err != nil {
			return 0, 0, 0, 0, FNORMAL, err
		}
		if len(v) > 32 {
			flag = v[32]
		}
	}
	return head, tail, size, ttl, flag, nil
}

// return  meta value bytes for a list key
// meta key and item key must be execute in one txn funcion
func (tidis *Tidis) lGenKeyMeta(head, tail, size, ttl uint64, flag byte) ([]byte, error) {
	buf := make([]byte, 32+1)

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

	buf[32] = flag

	return buf, nil
}

func (tidis *Tidis) LPExpireAt(key []byte, ts int64) (int, error) {
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.LPExpireAtWithTxn(txn1, key, ts)
	}

	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) LPExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
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

		lMetaKey = LMetaEncoder(key)
		head, tail, lsize, ttl, flag, err := tidis.lGetKeyMeta(lMetaKey, nil, txn)
		log.Debugf("head: %d tail:%d lsize:%d ttl:%d", head, tail, lsize, ttl)
		if err != nil {
			return 0, err
		}

		if lsize == 0 {
			// key not exists
			return 0, nil
		}
		if flag == FDELETED {
			tidis.AsyncDelAdd(TLISTMETA, key)
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
		lMetaValue, _ := tidis.lGenKeyMeta(head, tail, lsize, uint64(ts), FNORMAL)
		log.Debugf("metavalue %v", lMetaValue)
		if err = txn.Set(lMetaKey, lMetaValue); err != nil {
			return 0, err
		}

		tMetaKey = TMLEncoder(key, uint64(ts))
		if err = txn.Set(tMetaKey, []byte{0}); err != nil {
			return 0, err
		}

		return 1, nil
	}

	v, err := tidis.db.BatchWithTxn(f, txn)
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

func (tidis *Tidis) LPExpireWithTxn(txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.LPExpireAtWithTxn(txn, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) LExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.LPExpireAtWithTxn(txn, key, ts*1000)
}

func (tidis *Tidis) LExpireWithTxn(txn interface{}, key []byte, s int64) (int, error) {
	return tidis.LPExpireWithTxn(txn, key, s*1000)
}

func (tidis *Tidis) LPTtl(txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := LMetaEncoder(key)

	_, _, lsize, ttl, flag, err := tidis.lGetKeyMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}
	if lsize == 0 {
		// key not exists
		return -2, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(TLISTMETA, key)
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

func (tidis *Tidis) LTtl(txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.LPTtl(txn, key)
	if ttl < 0 {
		return ttl, err
	}
	return ttl / 1000, err
}

func (tidis *Tidis) LdeleteIfExpired(txn interface{}, key []byte) error {
	ttl, err := tidis.LTtl(txn, key)
	if err != nil {
		return err
	}
	if ttl != 0 {
		return nil
	}

	log.Debugf("Lazy deletion list key:%v", key)

	return tidis.ldeleteIfNeeded(txn, key, false)
}

// clear expire flag in meta and delete ttl key
func (tidis *Tidis) LclearExpire(txn interface{}, key []byte) error {
	ttl, err := tidis.LTtl(txn, key)
	if err != nil {
		return err
	}

	if ttl < 0 {
		return nil
	}

	log.Debugf("Clear expire list key: %v", key)

	if _, err = tidis.LExpireAtWithTxn(txn, key, 0); err != nil {
		return err
	}

	return tidis.ldeleteIfNeeded(txn, key, true)
}

// expireOnly == true, clear expire timestamp of this key
// expireOnly == false, delete entire expire and key data
func (tidis *Tidis) ldeleteIfNeeded(txn interface{}, key []byte, expireOnly bool) error {
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		lMetaKey := LMetaEncoder(key)

		head, tail, size, ttl, _, err := tidis.lGetKeyMeta(lMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			// already deleted
			return nil, nil
		}

		tMetaKey := TMLEncoder(key, ttl)

		// delete tMetaKey/entire hashkey
		if err = txn.Delete(tMetaKey); err != nil {
			return nil, err
		}

		if !expireOnly {
			if err = txn.Delete(lMetaKey); err != nil {
				return nil, err
			}

			for i := head; i < tail; i++ {
				eDataKey := LDataEncoder(key, i)
				if err = txn.Delete(eDataKey); err != nil {
					return nil, err
				}
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
