//
// t_hash.go
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

func (tidis *Tidis) Hget(txn interface{}, key, field []byte) ([]byte, error) {
	if len(key) == 0 || len(field) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		v   []byte
		err error
	)

	if tidis.LazyCheck() {
		err = tidis.HdeleteIfExpired(txn, key)
		if err != nil {
			return nil, err
		}
	}

	eDataKey := HDataEncoder(key, field)
	if txn == nil {
		v, err = tidis.db.Get(eDataKey)
	} else {
		v, err = tidis.db.GetWithTxn(eDataKey, txn)
	}
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (tidis *Tidis) Hstrlen(txn interface{}, key, field []byte) (int, error) {
	v, err := tidis.Hget(txn, key, field)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (tidis *Tidis) Hexists(txn interface{}, key, field []byte) (bool, error) {
	v, err := tidis.Hget(txn, key, field)
	if err != nil {
		return false, err
	}

	if v == nil || len(v) == 0 {
		return false, nil
	}

	return true, nil
}

func (tidis *Tidis) Hlen(txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	var (
		err error
	)

	if tidis.LazyCheck() {
		err = tidis.HdeleteIfExpired(txn, key)
		if err != nil {
			return 0, err
		}
	}

	eMetaKey := HMetaEncoder(key)
	hsize, _, flag, err := tidis.hGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return 0, err
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(THASHMETA, key)
		return 0, nil
	}

	return hsize, nil
}

func (tidis *Tidis) Hmget(txn interface{}, key []byte, fields ...[]byte) ([]interface{}, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, terror.ErrKeyOrFieldEmpty
	}
	var (
		retMap map[string][]byte
		err    error
	)

	if tidis.LazyCheck() {
		err = tidis.HdeleteIfExpired(txn, key)
		if err != nil {
			return nil, err
		}
	}

	batchKeys := make([][]byte, len(fields))
	for i, field := range fields {
		batchKeys[i] = HDataEncoder(key, field)
	}
	if txn == nil {
		retMap, err = tidis.db.MGet(batchKeys)
	} else {
		retMap, err = tidis.db.MGetWithTxn(batchKeys, txn)
	}
	if err != nil {
		return nil, err
	}

	// convert map to slice
	ret := make([]interface{}, len(fields))
	for i, ek := range batchKeys {
		v, ok := retMap[string(ek)]
		if !ok {
			ret[i] = nil
		} else {
			ret[i] = v
		}
	}
	return ret, nil
}

func (tidis *Tidis) Hdel(key []byte, fields ...[]byte) (uint64, error) {
	if tidis.LazyCheck() {
		err := tidis.HdeleteIfExpired(nil, key)
		if err != nil {
			return 0, nil
		}
	}

	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.HdelWithTxn(txn, key, fields...)
	}

	// execute txn
	deleted, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return deleted.(uint64), nil
}

func (tidis *Tidis) HdelWithTxn(txn interface{}, key []byte, fields ...[]byte) (uint64, error) {
	if len(key) == 0 || len(fields) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var delCnt uint64

		hsize, ttl, flag, err := tidis.hGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if hsize == 0 {
			return delCnt, nil
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(THASHMETA, key)
			return nil, terror.ErrKeyBusy
		}

		for _, field := range fields {
			eDataKey := HDataEncoder(key, field)
			v, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return nil, err
			}
			if v != nil {
				delCnt++
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		}

		hsize = hsize - delCnt
		if hsize > 0 {
			// update meta size
			eMetaValue := tidis.hGenMeta(hsize, ttl, FNORMAL)
			err = txn.Set(eMetaKey, eMetaValue)
			if err != nil {
				return nil, err
			}
		} else {
			// delete entire user hash key
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}
			// clear ttl meta key if needed
			if ttl > 0 {
				err = tidis.HclearExpire(txn, key)
				if err != nil {
					return nil, err
				}
			}
		}

		return delCnt, nil
	}

	// execute txn
	deleted, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return deleted.(uint64), nil
}

func (tidis *Tidis) Hset(key, field, value []byte) (uint8, error) {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.HsetWithTxn(txn1, key, field, value)
	}

	// execute txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) HsetWithTxn(txn interface{}, key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var ret uint8
		var hsize uint64

		if tidis.LazyCheck() {
			if err := tidis.HdeleteIfExpired(txn, key); err != nil {
				return 0, err
			}
		}

		hsize, ttl, flag, err := tidis.hGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(THASHMETA, key)
			return nil, terror.ErrKeyBusy
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return nil, err
		}

		if v != nil {
			ret = 0
		} else {
			// new insert field, add hsize
			ret = 1
			hsize++

			// update meta key
			eMetaValue := tidis.hGenMeta(hsize, ttl, FNORMAL)
			err = txn.Set(eMetaKey, eMetaValue)
			if err != nil {
				return nil, err
			}
		}

		// set or update field
		err = txn.Set(eDataKey, value)
		if err != nil {
			return 0, err
		}

		return ret, nil
	}

	// execute txn
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) Hsetnx(key, field, value []byte) (uint8, error) {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.HsetnxWithTxn(txn1, key, field, value)
	}

	// execute txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) HsetnxWithTxn(txn interface{}, key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var hsize uint64

		if tidis.LazyCheck() {
			if err := tidis.HdeleteIfExpired(nil, key); err != nil {
				return 0, err
			}
		}

		hsize, ttl, flag, err := tidis.hGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return uint8(0), err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(THASHMETA, key)
			return uint8(0), terror.ErrKeyBusy
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return uint8(0), err
		}

		if v != nil {
			// field already exists, no perform update
			return uint8(0), nil
		}

		// new insert field, add hsize
		hsize++

		// update meta key
		eMetaData := tidis.hGenMeta(hsize, ttl, FNORMAL)
		err = txn.Set(eMetaKey, eMetaData)
		if err != nil {
			return uint8(0), err
		}

		// set or update field
		err = txn.Set(eDataKey, value)
		if err != nil {
			return uint8(0), err
		}

		return uint8(1), nil
	}

	// execute txn
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) Hmset(key []byte, fieldsvalues ...[]byte) error {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return nil, tidis.HmsetWithTxn(txn1, key, fieldsvalues...)
	}

	// execute txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) HmsetWithTxn(txn interface{}, key []byte, fieldsvalues ...[]byte) error {
	if len(key) == 0 || len(fieldsvalues)%2 != 0 {
		return terror.ErrCmdParams
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var hsize uint64

		if tidis.LazyCheck() {
			if err := tidis.HdeleteIfExpired(txn, key); err != nil {
				return nil, err
			}
		}

		hsize, ttl, flag, err := tidis.hGetMeta(eMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(THASHMETA, key)
			return nil, terror.ErrKeyBusy
		}

		// multi get set
		for i := 0; i < len(fieldsvalues)-1; i = i + 2 {
			field, value := fieldsvalues[i], fieldsvalues[i+1]

			// check field already exists, update hsize
			eDataKey := HDataEncoder(key, field)
			v, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return nil, err
			}

			if v == nil {
				// field not exists, hsize should incr
				hsize++
			}

			// update field
			err = txn.Set(eDataKey, value)
			if err != nil {
				return nil, err
			}
		}

		// update meta
		eMetaData := tidis.hGenMeta(hsize, ttl, FNORMAL)
		err = txn.Set(eMetaKey, eMetaData)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	// execute txn
	_, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Hkeys(txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss   interface{}
		err  error
		keys [][]byte
	)

	if tidis.LazyCheck() {
		if err = tidis.HdeleteIfExpired(txn, key); err != nil {
			return nil, err
		}
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, flag, err := tidis.hGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return EmptyListOrSet, nil
	}
	if flag == FDELETED {
		tidis.AsyncDelAdd(THASHMETA, key)
		return EmptyListOrSet, nil
	}

	if txn == nil {
		keys, err = tidis.db.GetRangeKeys(eDataKey, nil, 0, hsize, ss)
	} else {
		keys, err = tidis.db.GetRangeKeysWithTxn(eDataKey, nil, 0, hsize, txn)
	}
	if err != nil {
		return nil, err
	}

	retkeys := make([]interface{}, len(keys))
	for i, key := range keys {
		_, retkeys[i], _ = HDataDecoder(key)
	}

	return retkeys, nil
}

func (tidis *Tidis) Hvals(txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss   interface{}
		err  error
		vals [][]byte
	)

	if tidis.LazyCheck() {
		if err = tidis.HdeleteIfExpired(txn, key); err != nil {
			return nil, err
		}
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, flag, err := tidis.hGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return EmptyListOrSet, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(THASHMETA, key)
		return EmptyListOrSet, nil
	}

	if txn == nil {
		vals, err = tidis.db.GetRangeVals(eDataKey, nil, hsize, ss)
	} else {
		vals, err = tidis.db.GetRangeValsWithTxn(eDataKey, nil, hsize, txn)
	}
	if err != nil {
		return nil, err
	}

	retvals := make([]interface{}, len(vals))
	for i, val := range vals {
		retvals[i] = val
	}

	return retvals, nil
}

func (tidis *Tidis) Hgetall(txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss  interface{}
		kvs [][]byte
		err error
	)

	if tidis.LazyCheck() {
		if err = tidis.HdeleteIfExpired(txn, key); err != nil {
			return nil, err
		}
	}

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, flag, err := tidis.hGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return EmptyListOrSet, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(THASHMETA, key)
		return EmptyListOrSet, nil
	}

	if txn == nil {
		kvs, err = tidis.db.GetRangeKeysVals(eDataKey, nil, hsize, ss)
	} else {
		kvs, err = tidis.db.GetRangeKeysValsWithTxn(eDataKey, nil, hsize, txn)
	}
	if err != nil {
		return nil, err
	}

	// decode fields
	retkvs := make([]interface{}, len(kvs))
	for i := 0; i < len(kvs); i = i + 1 {
		if i%2 == 0 {
			_, retkvs[i], _ = HDataDecoder(kvs[i])
		} else {
			retkvs[i] = kvs[i]
		}
	}

	return retkvs, nil
}

func (tidis *Tidis) Hclear(key []byte, async bool) (uint8, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// check exists before start a transaction
	ret, err := tidis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if ret == nil {
		// key not exists, just return
		return 0, nil
	}

	// txn func
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.HclearWithTxn(txn1, key, &async)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	if async {
		// notify async task
		tidis.AsyncDelAdd(THASHMETA, key)
	}

	return v.(uint8), nil
}

func (tidis *Tidis) HclearWithTxn(txn1 interface{}, key []byte, async *bool) (uint8, error) {
	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return uint8(0), terror.ErrBackendType
	}

	eMetaKey := HMetaEncoder(key)
	hsize, ttl, _, err := tidis.hGetMeta(eMetaKey, nil, txn)
	if err != nil {
		return uint8(0), err
	}
	if hsize == 0 {
		return uint8(0), nil
	}
	//if flag == FDELETED {
	// mark async to add key to async task if needed
	//	*async = true
	//	return uint8(0), nil
	//}

	if *async && hsize < 1024 {
		// convert async to sync operation for small hash key
		*async = false
	}

	if *async {
		// mark meta key as deleted
		v := tidis.hGenMeta(hsize, ttl, FDELETED)
		err = txn.Set(eMetaKey, v)
		if err != nil {
			return uint8(0), err
		}
	} else {

		// delete meta key
		err = txn.Delete(eMetaKey)
		if err != nil {
			return uint8(0), err
		}

		// delete all fields
		eDataKeyStart := HDataEncoder(key, nil)
		keys, err := tidis.db.GetRangeKeysWithTxn(eDataKeyStart, nil, 0, hsize, txn)
		if err != nil {
			return uint8(0), err
		}
		for _, key := range keys {
			txn.Delete(key)
		}

		// delete ttl meta key
		err = tidis.HclearExpire(txn, key)
		if err != nil {
			return uint8(0), err
		}
	}
	return uint8(1), nil
}

// snapshot has higher priority than transaction, if both not nil
func (tidis *Tidis) hGetMeta(key []byte, ss, txn interface{}) (uint64, uint64, byte, error) {
	if len(key) == 0 {
		return 0, 0, FNORMAL, terror.ErrKeyEmpty
	}

	var (
		size uint64
		ttl  uint64
		flag byte
		err  error
		v    []byte
	)
	if ss == nil && txn == nil {
		v, err = tidis.db.Get(key)
	} else if ss != nil {
		v, err = tidis.db.GetWithSnapshot(key, ss)
	} else {
		v, err = tidis.db.GetWithTxn(key, txn)
	}
	if err != nil {
		return 0, 0, FNORMAL, err
	}
	if v == nil {
		return 0, 0, FNORMAL, nil
	}
	if len(v) < 16 {
		return 0, 0, FNORMAL, terror.ErrInvalidMeta
	}
	if size, err = util.BytesToUint64(v[0:]); err != nil {
		return 0, 0, FNORMAL, terror.ErrInvalidMeta
	}
	if ttl, err = util.BytesToUint64(v[8:]); err != nil {
		return 0, 0, FNORMAL, terror.ErrInvalidMeta
	}

	if len(v) == 17 {
		flag = v[16]
	}

	return size, ttl, flag, nil
}

func (tidis *Tidis) hGenMeta(size, ttl uint64, flag byte) []byte {
	buf := make([]byte, 17)

	util.Uint64ToBytes1(buf[0:], size)
	util.Uint64ToBytes1(buf[8:], ttl)
	buf[16] = flag

	return buf
}

func (tidis *Tidis) HPExpireAt(key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn interface{}) (interface{}, error) {
		return tidis.HPExpireAtWithTxn(txn, key, ts)
	}

	// execute txn func
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}

func (tidis *Tidis) HPExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		var (
			hMetaKey []byte
			tMetaKey []byte
		)

		hMetaKey = HMetaEncoder(key)
		hsize, ttl, flag, err := tidis.hGetMeta(hMetaKey, nil, txn)
		if err != nil {
			return 0, err
		}
		if hsize == 0 {
			// key not exists
			return 0, nil
		}

		if flag == FDELETED {
			tidis.AsyncDelAdd(THASHMETA, key)
			return 0, nil
		}

		// check expire time already set before
		if ttl != 0 {
			// delete ttl meta key first
			tMetaKey = TMHEncoder(key, ttl)
			if err = txn.Delete(tMetaKey); err != nil {
				return 0, err
			}
		}

		// update hash meta key and set ttl meta key
		hMetaValue := tidis.hGenMeta(hsize, uint64(ts), FNORMAL)
		if err = txn.Set(hMetaKey, hMetaValue); err != nil {
			return 0, err
		}

		tMetaKey = TMHEncoder(key, uint64(ts))
		if err = txn.Set(tMetaKey, []byte{0}); err != nil {
			return 0, err
		}

		return 1, nil
	}

	// execute txn func
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}

func (tidis *Tidis) HPExpire(key []byte, ms int64) (int, error) {
	return tidis.HPExpireAt(key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) HExpireAt(key []byte, ts int64) (int, error) {
	return tidis.HPExpireAt(key, ts*1000)
}

func (tidis *Tidis) HExpire(key []byte, s int64) (int, error) {
	return tidis.HPExpire(key, s*1000)
}

func (tidis *Tidis) HPExpireWithTxn(txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.HPExpireAtWithTxn(txn, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) HExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.HPExpireAtWithTxn(txn, key, ts*1000)
}

func (tidis *Tidis) HExpireWithTxn(txn interface{}, key []byte, s int64) (int, error) {
	return tidis.HPExpireWithTxn(txn, key, s*1000)
}
func (tidis *Tidis) HPTtl(txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
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

	eMetaKey := HMetaEncoder(key)

	hsize, ttl, flag, err := tidis.hGetMeta(eMetaKey, ss, txn)
	if err != nil {
		return 0, err
	}
	if hsize == 0 {
		// key not exists
		return -2, nil
	}

	if flag == FDELETED {
		tidis.AsyncDelAdd(THASHMETA, key)
		return -2, nil
	}

	if ttl == 0 {
		// no expire associated
		return -1, nil
	}

	var ts int64
	ts = int64(ttl) - time.Now().UnixNano()/1000/1000
	if ts < 0 {
		err = tidis.hdeleteIfNeeded(txn, key, false)
		if err != nil {
			return 0, err
		}
		return -2, nil
	}

	return ts, nil
}

func (tidis *Tidis) HTtl(txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.HPTtl(txn, key)
	if ttl < 0 {
		return ttl, err
	}

	return ttl / 1000, err
}

func (tidis *Tidis) HdeleteIfExpired(txn interface{}, key []byte) error {
	// check ttl
	ttl, err := tidis.HTtl(txn, key)
	if err != nil {
		return err
	}
	if ttl != 0 {
		return nil
	}

	log.Debugf("Lazy deletion key %v", key)

	return tidis.hdeleteIfNeeded(txn, key, false)
}

func (tidis *Tidis) HclearExpire(txn interface{}, key []byte) error {
	ttl, err := tidis.HTtl(txn, key)
	if err != nil {
		return err
	}

	if ttl < 0 {
		return nil
	}

	log.Debugf("Clear expire key: %v", key)

	// clear key ttl field in key meta
	if _, err = tidis.HExpireAtWithTxn(txn, key, 0); err != nil {
		return err
	}

	return tidis.hdeleteIfNeeded(txn, key, true)
}

// expireOnly == true : remove expire key only
// expireOnly == false: delete expire key and data
func (tidis *Tidis) hdeleteIfNeeded(txn interface{}, key []byte, expireOnly bool) error {
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get ts of the key
		hMetaKey := HMetaEncoder(key)

		size, ttl, _, err := tidis.hGetMeta(hMetaKey, nil, txn)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			// already deleted
			return nil, nil
		}

		tMetaKey := TMHEncoder(key, uint64(ttl))

		// delete tMetaKey/entire hashkey
		if err = txn.Delete(tMetaKey); err != nil {
			return nil, err
		}

		if !expireOnly {
			False := false
			_, err = tidis.HclearWithTxn(txn, key, &False)
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
