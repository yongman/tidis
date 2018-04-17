//
// t_hash.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
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
	hsize, err := util.BytesToUint64(v)
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
		hsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
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

		hsize, err := util.BytesToUint64(hsizeRaw)
		if err != nil {
			return nil, err
		}

		hsize = hsize - delCnt
		if hsize > 0 {
			// update meta size
			eMetaValue, _ := util.Uint64ToBytes(hsize)
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

		hsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			// create a new meta key
			hsize = 0
		} else {
			hsize, err = util.BytesToUint64(hsizeRaw)
			if err != nil {
				return nil, err
			}
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
			eMetaValue, _ := util.Uint64ToBytes(hsize)
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

		hsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			// create a new meta key
			hsize = 0
		} else {
			hsize, err = util.BytesToUint64(hsizeRaw)
			if err != nil {
				return nil, err
			}
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
		eMetaData, _ := util.Uint64ToBytes(hsize)
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

		hsizeRaw, err := tidis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			hsize = 0
		} else {
			hsize, err = util.BytesToUint64(hsizeRaw)
			if err != nil {
				return nil, err
			}
		}

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
		eMetaData, _ := util.Uint64ToBytes(hsize)
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

	hsizeRaw, err := ss1.Get(eMetaKey)
	if err != nil {
		return nil, err
	}

	hsize, err := util.BytesToUint64(hsizeRaw)
	if err != nil {
		return nil, err
	}

	keys, err := tidis.db.GetRangeKeys(eDataKey, nil, hsize, ss)
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

	hsizeRaw, err := ss1.Get(eMetaKey)
	if err != nil {
		return nil, err
	}

	hsize, err := util.BytesToUint64(hsizeRaw)
	if err != nil {
		return nil, err
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

	hsizeRaw, err := ss1.Get(eMetaKey)
	if err != nil {
		return nil, err
	}

	hsize, err := util.BytesToUint64(hsizeRaw)
	if err != nil {
		return nil, err
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
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		ss := txn.GetSnapshot()
		hsizeRaw, err := ss.Get(eMetaKey)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			return uint8(0), nil
		}

		hsize, err := util.BytesToUint64(hsizeRaw)
		if err != nil {
			return nil, err
		}

		// delete meta key
		err = txn.Delete(eMetaKey)
		if err != nil {
			return nil, err
		}

		// delete all fields
		eDataKeyStart := HDataEncoder(key, nil)
		keys, err := tidis.db.GetRangeKeys(eDataKeyStart, nil, hsize, ss)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			txn.Delete(key)
		}
		return uint8(1), nil
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint8), nil
}
