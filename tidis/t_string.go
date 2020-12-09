//
// t_string.go
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
	"time"
)

type StringObj struct {
	Object
	Value []byte
}

func MarshalStringObj(obj *StringObj) []byte {
	totalLen := 1 + 8 + 1 + len(obj.Value)
	raw := make([]byte, totalLen)

	idx := 0
	raw[idx] = obj.Type
	idx++
	util.Uint64ToBytes1(raw[idx:], obj.ExpireAt)
	idx += 8
	raw[idx] = obj.Tomb
	idx++
	copy(raw[idx:], obj.Value)

	return raw
}

func UnmarshalStringObj(raw []byte) (*StringObj, error) {
	if len(raw) < 10 {
		return nil, nil
	}
	obj := StringObj{}
	idx := 0
	obj.Type = raw[idx]
	if obj.Type != TSTRING {
		return nil, terror.ErrTypeNotMatch
	}
	idx++
	obj.ExpireAt, _ = util.BytesToUint64(raw[idx:])
	idx += 8
	obj.Tomb = raw[idx]
	idx++
	obj.Value = raw[idx:]
	return &obj, nil
}

func (tidis *Tidis) Get(dbId uint8, txn interface{}, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	key = tidis.RawKeyPrefix(dbId, key)

	var (
		v   []byte
		err error
	)

	if txn == nil {
		v, err = tidis.db.Get(key)
	} else {
		v, err = tidis.db.GetWithTxn(key, txn)
	}
	if err != nil {
		return nil, err
	}
	// key not exist, return asap
	if v == nil {
		return nil, nil
	}

	obj, err := UnmarshalStringObj(v)
	if err != nil {
		return nil, err
	}

	if obj.ObjectExpired(utils.Now()) {
		if txn == nil {
			tidis.db.Delete([][]byte{key})
		} else {
			tidis.db.DeleteWithTxn([][]byte{key}, txn)
		}
		return nil, nil
	}

	return obj.Value, nil
}

func (tidis *Tidis) MGet(dbId uint8, txn interface{}, keys [][]byte) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		m   map[string][]byte
		err error
	)
	for i := 0; i < len(keys); i++ {
		keys[i] = tidis.RawKeyPrefix(dbId, keys[i])
	}

	if txn == nil {
		m, err = tidis.db.MGet(keys)
	} else {
		m, err = tidis.db.MGetWithTxn(keys, txn)
	}
	if err != nil {
		return nil, err
	}

	resp := make([]interface{}, len(keys))
	for i, key := range keys {
		if v, ok := m[string(key)]; ok {
			obj, err := UnmarshalStringObj(v)
			if err != nil {
				resp[i] = nil
				continue
			} else if obj.ObjectExpired(utils.Now()) {
				resp[i] = nil
				if txn == nil {
					tidis.db.Delete([][]byte{key})
				} else {
					tidis.db.DeleteWithTxn([][]byte{key}, txn)
				}
			} else {
				resp[i] = obj.Value
			}
		} else {
			resp[i] = nil
		}
	}
	return resp, nil
}

func (tidis *Tidis) Set(dbId uint8, txn interface{}, key, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	var err error

	key = tidis.RawKeyPrefix(dbId, key)

	obj := StringObj{
		Object: Object{
			Type:     TSTRING,
			Tomb:     0,
			ExpireAt: 0,
		},
		Value: value,
	}
	v := MarshalStringObj(&obj)

	if txn == nil {
		err = tidis.db.Set(key, v)
	} else {
		err = tidis.db.SetWithTxn(key, v, txn)
	}
	if err != nil {
		return err
	}
	return nil
}

func (tidis *Tidis) SetWithParam(dbId uint8, txn interface{}, key, value []byte, msTtl uint64, nxFlag bool, xxFlag bool) (bool, error) {
	if len(key) == 0 {
		return false, terror.ErrKeyEmpty
	}

	if nxFlag == true && xxFlag == true {
		return false, terror.ErrCmdParams
	}

	obj := StringObj{
		Object: Object{
			Type:     TSTRING,
			Tomb:     0,
			ExpireAt: 0,
		},
		Value: value,
	}

	var err error

	f := func(txn interface{}) (interface{}, error) {
		tValue, err := tidis.Get(dbId, txn, key)
		if err != nil {
			return false, err
		}

		if nxFlag == true && tValue != nil {
			return false, nil
		}

		if xxFlag == true && tValue == nil {
			return false, nil
		}

		if msTtl > 0 {
			obj.ExpireAt = utils.Now() + msTtl
		}

		value = MarshalStringObj(&obj)
		metaKey := tidis.RawKeyPrefix(dbId, key)
		err = tidis.db.SetWithTxn(metaKey, value, txn)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	var result interface{}
	if txn == nil {
		result, err = tidis.db.BatchInTxn(f)
	} else {
		result, err = tidis.db.BatchWithTxn(f, txn)
	}

	if err != nil {
		return false, err
	}

	return result.(bool), err

}

func (tidis *Tidis) Setex(dbId uint8, key []byte, sec int64, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}
	f := func(txn interface{}) (interface{}, error) {
		return nil, tidis.SetexWithTxn(dbId, txn, key, sec, value)
	}

	// execute in txn
	_, err := tidis.db.BatchInTxn(f)

	return err
}

func (tidis *Tidis) SetexWithTxn(dbId uint8, txn interface{}, key []byte, sec int64, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	obj := StringObj{
		Object: Object{
			Type:     TSTRING,
			Tomb:     0,
			ExpireAt: utils.Now() + uint64(sec)*1000,
		},
		Value: value,
	}
	value = MarshalStringObj(&obj)
	f := func(txn interface{}) (interface{}, error) {
		err := tidis.Set(dbId, txn, key, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	_, err := tidis.db.BatchWithTxn(f, txn)

	return err
}

func (tidis *Tidis) MSet(dbId uint8, txn interface{}, keyvals [][]byte) (int, error) {
	if len(keyvals) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	kvm := make(map[string][]byte, len(keyvals))
	for i := 0; i < len(keyvals)-1; i += 2 {
		k := string(tidis.RawKeyPrefix(dbId, keyvals[i]))
		obj := StringObj{
			Object: Object{
				Type:     TSTRING,
				Tomb:     0,
				ExpireAt: 0,
			},
			Value:  keyvals[i+1],
		}
		v := MarshalStringObj(&obj)
		kvm[k] = v
	}
	if txn == nil {
		return tidis.db.MSet(kvm)
	}
	return tidis.db.MSetWithTxn(kvm, txn)
}

// Delete is a generic api for all type keys
func (tidis *Tidis) Delete(dbId uint8, txn interface{}, keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = tidis.RawKeyPrefix(dbId, keys[i])
	}

	var (
		ret interface{}
		err error

	)

	// check object type
	f := func(txn interface{}) (interface{}, error) {
		var deleted int = 0
		for idx, key := range nkeys {
			metaValue, err := tidis.db.GetWithTxn(key, txn)
			if err != nil {
				return 0, err
			}
			if metaValue == nil {
				return 0, nil
			}
			objType := metaValue[0]
			switch objType {
			case TSTRING:
				ret, err = tidis.db.DeleteWithTxn([][]byte{key}, txn)
				deleted++
			case THASHMETA:
				var hasDeleted uint8
				hasDeleted, err = tidis.HclearWithTxn(dbId, txn, keys[idx])
				if hasDeleted == 1 {
					deleted++
				}
			case TLISTMETA:
				var deleteCount int
				deleteCount, err = tidis.LdelWithTxn(dbId, txn, keys[idx])
				if deleteCount > 0 {
					deleted++
				}
			case TSETMETA:
				var deleteCount int
				deleteCount, err = tidis.SclearKeyWithTxn(dbId, txn, keys[idx])
				if deleteCount > 0 {
					deleted++
				}
			case TZSETMETA:
				var deleteCount uint64
				deleteCount, err = tidis.ZremrangebyscoreWithTxn(dbId, txn, keys[idx], ScoreMin, ScoreMax)
				if deleteCount > 0 {
					deleted++
				}
			}
		}
		return deleted, err
	}

	if txn == nil {
		ret, err = tidis.db.BatchInTxn(f)
	} else {
		ret, err = tidis.db.BatchWithTxn(f, txn)
	}
	if err != nil {
		return 0, err
	}

	return ret.(int), nil
}

func (tidis *Tidis) Incr(dbId uint8, key []byte, step int64) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// inner func for tikv backend
	f := func(txn interface{}) (interface{}, error) {
		return tidis.IncrWithTxn(dbId, txn, key, step)
	}

	// execute in txn
	ret, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	retInt, ok := ret.(int64)
	if !ok {
		return 0, terror.ErrTypeAssertion
	}
	return retInt, nil
}

func (tidis *Tidis) IncrWithTxn(dbId uint8, txn interface{}, key []byte, step int64) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	metaKey := tidis.RawKeyPrefix(dbId, key)

	// inner func for tikv backend
	f := func(txn1 interface{}) (interface{}, error) {
		var (
			ev []byte
			dv int64
		)

		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		// get from db
		objType, obj, err := tidis.GetObject(dbId, txn, key)
		if err != nil {
			return 0, err
		}
		if obj == nil {
			obj = &StringObj{
				Object: Object{
					Type:     TSTRING,
					Tomb:     0,
					ExpireAt: 0,
				},
				Value: nil,
			}
		}
		if objType != TSTRING {
			return nil, terror.ErrNotInteger
		}
		strObj := obj.(*StringObj)

		if strObj.Value == nil {
			dv = 0
		} else {
			dv, err = util.StrBytesToInt64(strObj.Value)
			if err != nil {
				return nil, terror.ErrNotInteger
			}
		}
		// incr by step
		dv = dv + step

		ev, _ = util.Int64ToStrBytes(dv)
		// update object
		strObj.Value = ev
		// marshal object to bytes
		rawValue := MarshalStringObj(strObj)
		err = txn.Set(metaKey, rawValue)
		if err != nil {
			return nil, err
		}
		return dv, nil
	}

	// execute in txn
	ret, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	retInt, ok := ret.(int64)
	if !ok {
		return 0, terror.ErrTypeAssertion
	}
	return retInt, nil
}

func (tidis *Tidis) Decr(dbId uint8, key []byte, step int64) (int64, error) {
	return tidis.Incr(dbId, key, -1*step)
}

func (tidis *Tidis) DecrWithTxn(dbId uint8, txn interface{}, key []byte, step int64) (int64, error) {
	return tidis.IncrWithTxn(dbId, txn, key, -1*step)
}

// expire is also a series generic commands for all kind type keys
func (tidis *Tidis) PExpireAt(dbId uint8, key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn interface{}) (interface{}, error) {
		return tidis.PExpireAtWithTxn(dbId, txn, key, ts)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) PExpireAtWithTxn(dbId uint8, txn interface{}, key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}
		var (
			err error
			obj IObject
		)

		// check key exists
		_, obj, err = tidis.GetObject(dbId, txn, key)
		if err != nil {
			return 0, err
		}
		if obj == nil {
			// not exists
			return 0, nil
		}
		metaKey := tidis.RawKeyPrefix(dbId, key)
		if obj.ObjectExpired(utils.Now()) {
			// TODO delete data key
			tidis.Delete(dbId, txn, [][]byte{key})
			return 0, nil
		}

		// update expireAt
		obj.SetExpireAt(uint64(ts))
		metaValue := MarshalObj(obj)

		err = txn.Set(metaKey, metaValue)
		if err != nil {
			return 0, err
		}
		return 1, nil
	}

	// execute txn
	v, err := tidis.db.BatchWithTxn(f, txn)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) PExpire(dbId uint8, key []byte, ms int64) (int, error) {
	return tidis.PExpireAt(dbId, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) ExpireAt(dbId uint8, key []byte, ts int64) (int, error) {
	return tidis.PExpireAt(dbId, key, ts*1000)
}

func (tidis *Tidis) Expire(dbId uint8, key []byte, s int64) (int, error) {
	return tidis.PExpire(dbId, key, s*1000)
}

func (tidis *Tidis) PExpireWithTxn(dbId uint8, txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.PExpireAtWithTxn(dbId, txn, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) ExpireAtWithTxn(dbId uint8, txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.PExpireAtWithTxn(dbId, txn, key, ts*1000)
}

func (tidis *Tidis) ExpireWithTxn(dbId uint8, txn interface{}, key []byte, s int64) (int, error) {
	return tidis.PExpireWithTxn(dbId, txn, key, s*1000)
}

// generic command
func (tidis *Tidis) PTtl(dbId uint8, txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}
	var ttl int64

	now := utils.Now()

	_, obj, err := tidis.GetObject(dbId, txn, key)
	if err != nil {
		return 0, err
	}
	if obj == nil {
		return -2, nil
	}

	if !obj.IsExpireSet() {
		ttl = -1
	} else if !obj.ObjectExpired(now) {
		ttl = int64(obj.TTL(now))
	} else {
		ttl = 0
		tidis.Delete(dbId, txn, [][]byte{key})
	}

	return ttl, nil
}

func (tidis *Tidis) Ttl(dbId uint8, txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.PTtl(dbId, txn, key)
	if ttl < 0 {
		return ttl, err
	}
	return ttl / 1000, err
}

