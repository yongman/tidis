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
	"github.com/yongman/tidis/utils"
)

type HashObj struct {
	Object
	Size uint64
}

func MarshalHashObj(obj *HashObj) []byte {
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

func UnmarshalHashObj(raw []byte) (*HashObj, error) {
	if len(raw) != 18 {
		return nil, nil
	}
	obj := HashObj{}
	idx := 0
	obj.Type = raw[idx]
	if obj.Type != THASHMETA {
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

func (tidis *Tidis) RawHashDataKey(dbId uint8, key, field []byte) []byte {
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	dataKey := append(keyPrefix, DataTypeKey)
	dataKey = append(dataKey, field...)
	return dataKey
}

func (tidis *Tidis) HashMetaObj(dbId uint8, txn interface{}, key []byte) (*HashObj, error) {
	return tidis.HashMetaObjWithExpire(dbId, txn, key, true)
}
func (tidis *Tidis) HashMetaObjWithExpire(dbId uint8, txn interface{}, key []byte, checkExpire bool) (*HashObj, error) {
	var (
		v   []byte
		err error
	)

	metaKey := tidis.RawKeyPrefix(dbId, key)

	if txn == nil {
		v, err = tidis.db.Get(metaKey)
	} else {
		v, err = tidis.db.GetWithTxn(metaKey, txn)
	}
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	obj, err := UnmarshalHashObj(v)
	if err != nil {
		return nil, err
	}
	if checkExpire && obj.ObjectExpired(utils.Now()) {
		if txn == nil {
			tidis.Hclear(dbId, key)
		} else {
			tidis.HclearWithTxn(dbId, txn, key)
		}
		return nil, nil
	}
	return obj, nil
}

func (tidis *Tidis) newHashObj() *HashObj {
	return &HashObj{
		Object: Object{
			Type:     THASHMETA,
			Tomb:     0,
			ExpireAt: 0,
		},
		Size: 0,
	}
}

func (tidis *Tidis) Hget(dbId uint8, txn interface{}, key, field []byte) ([]byte, error) {
	if len(key) == 0 || len(field) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		v   []byte
		err error
	)

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	}

	if metaObj.ObjectExpired(utils.Now()) {
		// TODO
		// hash key size > 100 use range delete otherwise use generic delete

		return nil, nil
	}

	eDataKey := tidis.RawHashDataKey(dbId, key, field)
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

func (tidis *Tidis) Hstrlen(dbId uint8, txn interface{}, key, field []byte) (int, error) {
	v, err := tidis.Hget(dbId, txn, key, field)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (tidis *Tidis) Hexists(dbId uint8, txn interface{}, key, field []byte) (bool, error) {
	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return false, err
	}
	if metaObj == nil {
		return false, nil
	}

	if metaObj.ObjectExpired(utils.Now()) {
		// TODO
		// hash key size > 100 use range delete otherwise use generic delete

		return false, nil
	}

	return true, nil
}

func (tidis *Tidis) Hlen(dbId uint8, txn interface{}, key []byte) (uint64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}
	if metaObj.ObjectExpired(utils.Now()) {
		// TODO
		return 0, nil
	}

	return metaObj.Size, nil
}

func (tidis *Tidis) Hmget(dbId uint8, txn interface{}, key []byte, fields ...[]byte) ([]interface{}, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, terror.ErrKeyOrFieldEmpty
	}
	var (
		retMap map[string][]byte
		err    error
	)

	ret := make([]interface{}, len(fields))

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return ret, nil
	}
	if metaObj.ObjectExpired(utils.Now()) {
		// TODO

		return ret, nil
	}

	batchKeys := make([][]byte, len(fields))
	for i, field := range fields {
		batchKeys[i] = tidis.RawHashDataKey(dbId, key, field)
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

func (tidis *Tidis) Hdel(dbId uint8, key []byte, fields ...[]byte) (uint64, error) {
	// txn function
	f := func(txn interface{}) (interface{}, error) {
		return tidis.HdelWithTxn(dbId, txn, key, fields...)
	}

	// execute txn
	deleted, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return deleted.(uint64), nil
}

func (tidis *Tidis) HdelWithTxn(dbId uint8, txn interface{}, key []byte, fields ...[]byte) (uint64, error) {
	if len(key) == 0 || len(fields) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		return 0, nil
	}
	if metaObj.ObjectExpired(utils.Now()) {
		// TODO

		return 0, nil
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var delCnt uint64

		if metaObj.Size == 0 {
			return delCnt, nil
		}

		for _, field := range fields {
			eDataKey := tidis.RawHashDataKey(dbId, key, field)
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

		metaObj.Size = metaObj.Size - delCnt
		eMetaKey := tidis.RawKeyPrefix(dbId, key)
		if metaObj.Size > 0 {
			// update meta size
			eMetaValue := MarshalHashObj(metaObj)
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
	deleted, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return deleted.(uint64), nil
}

func (tidis *Tidis) Hset(dbId uint8, key, field, value []byte) (uint8, error) {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.HsetWithTxn(dbId, txn1, key, field, value)
	}

	// execute txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) HsetWithTxn(dbId uint8, txn interface{}, key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		metaObj = tidis.newHashObj()
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var ret uint8

		eDataKey := tidis.RawHashDataKey(dbId, key, field)
		v, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return nil, err
		}

		if v != nil {
			ret = 0
		} else {
			// new insert field, add hsize
			ret = 1
			metaObj.Size++

			// update meta key
			eMetaValue := MarshalHashObj(metaObj)
			eMetaKey := tidis.RawKeyPrefix(dbId, key)
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

func (tidis *Tidis) Hsetnx(dbId uint8, key, field, value []byte) (uint8, error) {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return tidis.HsetnxWithTxn(dbId, txn1, key, field, value)
	}

	// execute txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}
	return ret.(uint8), nil
}

func (tidis *Tidis) HsetnxWithTxn(dbId uint8, txn interface{}, key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		metaObj = tidis.newHashObj()
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		eDataKey := tidis.RawHashDataKey(dbId, key, field)
		v, err := tidis.db.GetWithTxn(eDataKey, txn)
		if err != nil {
			return uint8(0), err
		}

		if v != nil {
			// field already exists, no perform update
			return uint8(0), nil
		}

		// new insert field, add hsize
		metaObj.Size++

		// update meta key
		eMetaData := MarshalHashObj(metaObj)
		eMetaKey := tidis.RawKeyPrefix(dbId, key)
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

// Deprecated
func (tidis *Tidis) Hmset(dbId uint8, key []byte, fieldsvalues ...[]byte) error {
	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		return nil, tidis.HmsetWithTxn(dbId, txn1, key, fieldsvalues...)
	}

	// execute txn
	_, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}

// Deprecated
func (tidis *Tidis) HmsetWithTxn(dbId uint8, txn interface{}, key []byte, fieldsvalues ...[]byte) error {
	if len(key) == 0 || len(fieldsvalues)%2 != 0 {
		return terror.ErrCmdParams
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return err
	}
	if metaObj == nil {
		metaObj = tidis.newHashObj()
	}

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// multi get set
		for i := 0; i < len(fieldsvalues)-1; i = i + 2 {
			field, value := fieldsvalues[i], fieldsvalues[i+1]

			// check field already exists, update hsize
			eDataKey := tidis.RawHashDataKey(dbId, key, field)
			v, err := tidis.db.GetWithTxn(eDataKey, txn)
			if err != nil {
				return nil, err
			}

			if v == nil {
				// field not exists, size should incr by one
				metaObj.Size++
			}

			// update field
			err = txn.Set(eDataKey, value)
			if err != nil {
				return nil, err
			}
		}

		// update meta
		eMetaKey := tidis.RawKeyPrefix(dbId, key)
		eMetaData := MarshalHashObj(metaObj)
		err = txn.Set(eMetaKey, eMetaData)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	// execute txn
	_, err = tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return err
	}

	return nil
}

func (tidis *Tidis) Hkeys(dbId uint8, txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss   interface{}
		err  error
		keys [][]byte
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	eDataKey := tidis.RawHashDataKey(dbId, key, nil)

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	} else if metaObj.ObjectExpired(utils.Now()) {
		// TODO delete all hash key field

		return nil, nil
	}

	if txn == nil {
		keys, err = tidis.db.GetRangeKeys(eDataKey, nil, 0, metaObj.Size, ss)
	} else {
		keys, err = tidis.db.GetRangeKeysWithTxn(eDataKey, nil, 0, metaObj.Size, txn)
	}
	if err != nil {
		return nil, err
	}

	retkeys := make([]interface{}, len(keys))
	keyPrefixLen := len(tidis.RawKeyPrefix(dbId, key))
	for i, key := range keys {
		retkeys[i] = key[keyPrefixLen+1:]
	}

	return retkeys, nil
}

func (tidis *Tidis) Hvals(dbId uint8, txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss   interface{}
		err  error
		vals [][]byte
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	} else if metaObj.ObjectExpired(utils.Now()) {
		// TODO delete all hash key field

		return nil, nil
	}

	eDataKey := tidis.RawHashDataKey(dbId, key, nil)

	if txn == nil {
		vals, err = tidis.db.GetRangeVals(eDataKey, nil, metaObj.Size, ss)
	} else {
		vals, err = tidis.db.GetRangeValsWithTxn(eDataKey, nil, metaObj.Size, txn)
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

func (tidis *Tidis) Hgetall(dbId uint8, txn interface{}, key []byte) ([]interface{}, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		ss  interface{}
		kvs [][]byte
		err error
	)

	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return nil, err
		}
	}

	metaObj, err := tidis.HashMetaObj(dbId, txn, key)
	if err != nil {
		return nil, err
	}
	if metaObj == nil {
		return nil, nil
	} else if metaObj.ObjectExpired(utils.Now()) {
		// TODO delete all hash key field

		return nil, nil
	}
	eDataKey := tidis.RawHashDataKey(dbId, key, nil)

	if txn == nil {
		kvs, err = tidis.db.GetRangeKeysVals(eDataKey, nil, metaObj.Size, ss)
	} else {
		kvs, err = tidis.db.GetRangeKeysValsWithTxn(eDataKey, nil, metaObj.Size, txn)
	}
	if err != nil {
		return nil, err
	}

	// decode fields
	keyPrefix := tidis.RawKeyPrefix(dbId, key)
	retkvs := make([]interface{}, len(kvs))
	for i := 0; i < len(kvs); i = i + 1 {
		if i%2 == 0 {
			retkvs[i] = kvs[i][len(keyPrefix)+1:] // get field from data key
		} else {
			retkvs[i] = kvs[i]
		}
	}

	return retkvs, nil
}

func (tidis *Tidis) Hclear(dbId uint8, key []byte) (uint8, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// txn func
	f := func(txn interface{}) (interface{}, error) {
		return tidis.HclearWithTxn(dbId, txn, key)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(uint8), nil
}

func (tidis *Tidis) HclearWithTxn(dbId uint8, txn1 interface{}, key []byte) (uint8, error) {
	txn, ok := txn1.(kv.Transaction)
	if !ok {
		return uint8(0), terror.ErrBackendType
	}

	metaObj, err := tidis.HashMetaObjWithExpire(dbId, txn, key, false)
	if err != nil {
		return 0, err
	}
	if metaObj == nil {
		// not exist
		return 0, nil
	}

	eMetaKey := tidis.RawKeyPrefix(dbId, key)
	// delete meta key
	err = txn.Delete(eMetaKey)
	if err != nil {
		return uint8(0), err
	}

	// delete all fields
	eDataKeyStart := tidis.RawHashDataKey(dbId, key, nil)
	keys, err := tidis.db.GetRangeKeysWithTxn(eDataKeyStart, nil, 0, metaObj.Size, txn)
	if err != nil {
		return uint8(0), err
	}
	for _, key := range keys {
		txn.Delete(key)
	}

	return uint8(1), nil
}
