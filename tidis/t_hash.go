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
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
)

func (tidis *Tidis) Hget(key, field []byte) ([]byte, error) {
	if len(key) == 0 || len(field) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eDataKey := HDataEncoder(key, field)
	v, err := tidis.db.Get(eDataKey)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (tidis *Tidis) Hstrlen(key, field []byte) (int, error) {
	v, err := tidis.Hget(key, field)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (tidis *Tidis) Hexists(key, field []byte) (bool, error) {
	v, err := tidis.Hget(key, field)
	if err != nil {
		return false, err
	}

	if v == nil || len(v) == 0 {
		return false, nil
	}

	return true, nil
}

func (tidis *Tidis) Hlen(key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	eMetaKey := HMetaEncoder(key)
	v, err := tidis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	hsize, _, err := tidis.hGetMeta(eMetaKey, nil)
	if err != nil {
		return 0, err
	}
	return hsize, nil
}

func (tidis *Tidis) Hmget(key []byte, fields ...[]byte) ([]interface{}, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, terror.ErrKeyOrFieldEmpty
	}

	batchKeys := make([][]byte, len(fields))
	for i, field := range fields {
		batchKeys[i] = HDataEncoder(key, field)
	}
	retMap, err := tidis.db.MGet(batchKeys)
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

		var delCnt uint64 = 0

		ss := txn.GetSnapshot()
		hsize, ttl, err := tidis.hGetMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsize == 0 {
			return nil, nil
		}

		for _, field := range fields {
			eDataKey := HDataEncoder(key, field)
			v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
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
			eMetaValue := tidis.hGenMeta(hsize, ttl)
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
		}

		return delCnt, nil
	}

	// execute txn
	deleted, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return deleted.(uint64), nil
}

func (tidis *Tidis) Hset(key, field, value []byte) (uint8, error) {
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

		ss := txn.GetSnapshot()

		hsize, ttl, err := tidis.hGetMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
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
			eMetaValue := tidis.hGenMeta(hsize, ttl)
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
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) Hsetnx(key, field, value []byte) (uint8, error) {
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

		ss := txn.GetSnapshot()

		hsize, ttl, err := tidis.hGetMeta(eMetaKey, ss)
		if err != nil {
			return nil, err
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
		if err != nil {
			return nil, err
		}

		if v != nil {
			// field already exists, no perform update
			return uint8(0), nil
		}

		// new insert field, add hsize
		hsize++

		// update meta key
		eMetaData := tidis.hGenMeta(hsize, ttl)
		err = txn.Set(eMetaKey, eMetaData)
		if err != nil {
			return nil, err
		}

		// set or update field
		err = txn.Set(eDataKey, value)
		if err != nil {
			return uint8(0), err
		}

		return uint8(1), nil
	}

	// execute txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) Hmset(key []byte, fieldsvalues ...[]byte) error {
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

		ss := txn.GetSnapshot()

		hsize, ttl, err := tidis.hGetMeta(eMetaKey, ss)

		// multi get set
		for i := 0; i < len(fieldsvalues)-1; i = i + 2 {
			field, value := fieldsvalues[i], fieldsvalues[i+1]

			// check field already exists, update hsize
			eDataKey := HDataEncoder(key, field)
			v, err := tidis.db.GetWithSnapshot(eDataKey, ss)
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
		eMetaData := tidis.hGenMeta(hsize, ttl)
		err = txn.Set(eMetaKey, eMetaData)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	// execute txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Hkeys(key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}

	ss1, ok := ss.(kv.Snapshot)
	if !ok {
		return nil, terror.ErrBackendType
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, err := tidis.hGetMeta(eMetaKey, ss1)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return nil, nil
	}

	keys, err := tidis.db.GetRangeKeys(eDataKey, nil, 0, hsize, ss)
	if err != nil {
		return nil, err
	}

	retkeys := make([]interface{}, len(keys))
	for i, key := range keys {
		_, retkeys[i], _ = HDataDecoder(key)
	}

	return retkeys, nil
}

func (tidis *Tidis) Hvals(key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}

	ss1, ok := ss.(kv.Snapshot)
	if !ok {
		return nil, terror.ErrBackendType
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, err := tidis.hGetMeta(eMetaKey, ss1)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return nil, nil
	}

	vals, err := tidis.db.GetRangeVals(eDataKey, nil, hsize, ss)
	if err != nil {
		return nil, err
	}

	retvals := make([]interface{}, len(vals))
	for i, val := range vals {
		retvals[i] = val
	}

	return retvals, nil
}

func (tidis *Tidis) Hgetall(key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return nil, err
	}

	ss1, ok := ss.(kv.Snapshot)
	if !ok {
		return nil, terror.ErrBackendType
	}

	eMetaKey := HMetaEncoder(key)
	eDataKey := HDataEncoder(key, nil)

	hsize, _, err := tidis.hGetMeta(eMetaKey, ss1)
	if err != nil {
		return nil, err
	}
	if hsize == 0 {
		return nil, nil
	}

	kvs, err := tidis.db.GetRangeKeysVals(eDataKey, nil, hsize, ss)
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

func (tidis *Tidis) Hclear(key []byte) (uint8, error) {
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
		return tidis.HclearWithTxn(key, txn1)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint8), nil
}

func (tidis *Tidis) HclearWithTxn(key []byte, txn1 interface{}) (uint8, error) {
	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return uint8(0), terror.ErrBackendType
	}

	ss := txn.GetSnapshot()
	eMetaKey := HMetaEncoder(key)
	hsize, _, err := tidis.hGetMeta(eMetaKey, ss)
	if err != nil {
		return uint8(0), err
	}
	if hsize == 0 {
		return uint8(0), nil
	}

	// delete meta key
	err = txn.Delete(eMetaKey)
	if err != nil {
		return uint8(0), err
	}

	// delete all fields
	eDataKeyStart := HDataEncoder(key, nil)
	keys, err := tidis.db.GetRangeKeys(eDataKeyStart, nil, 0, hsize, ss)
	if err != nil {
		return uint8(0), err
	}
	for _, key := range keys {
		txn.Delete(key)
	}
	return uint8(1), nil
}

func (tidis *Tidis) hGetMeta(key []byte, ss1 interface{}) (uint64, uint64, error) {
	if len(key) == 0 {
		return 0, 0, terror.ErrKeyEmpty
	}

	var (
		size uint64
		ttl  uint64
		err  error
		v    []byte
	)
	if ss1 == nil {
		v, err = tidis.db.Get(key)
	} else {
		ss, ok := ss1.(kv.Snapshot)
		if !ok {
			return 0, 0, terror.ErrBackendType
		}
		v, err = tidis.db.GetWithSnapshot(key, ss)
		if err != nil {
			return 0, 0, err
		}
	}
	if v == nil {
		return 0, 0, nil
	}
	if len(v) != 16 {
		return 0, 0, terror.ErrInvalidMeta
	}
	if size, err = util.BytesToUint64(v[0:]); err != nil {
		return 0, 0, terror.ErrInvalidMeta
	}
	if ttl, err = util.BytesToUint64(v[8:]); err != nil {
		return 0, 0, terror.ErrInvalidMeta
	}

	return size, ttl, nil
}

func (tidis *Tidis) hGenMeta(size, ttl uint64) []byte {
	buf := make([]byte, 16)

	util.Uint64ToBytes1(buf[0:], size)
	util.Uint64ToBytes1(buf[8:], ttl)

	return buf
}

func (tidis *Tidis) HPExpireAt(key []byte, ts int64) (int, error) {
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

		ss := txn.GetSnapshot()
		hMetaKey = HMetaEncoder(key)
		hsize, ttl, err := tidis.hGetMeta(hMetaKey, ss)
		if err != nil {
			return 0, err
		}
		if hsize == 0 {
			// key not exists
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
		hMetaValue := tidis.hGenMeta(hsize, uint64(ts))
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
	v, err := tidis.db.BatchInTxn(f)
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

func (tidis *Tidis) HPTtl(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return 0, err
	}

	eMetaKey := HMetaEncoder(key)

	hsize, ttl, err := tidis.hGetMeta(eMetaKey, ss)
	if err != nil {
		return 0, err
	}
	if hsize == 0 {
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

func (tidis *Tidis) HTtl(key []byte) (int64, error) {
	ttl, err := tidis.HPTtl(key)
	if ttl < 0 {
		return ttl, err
	} else {
		return ttl / 1000, err
	}
}
