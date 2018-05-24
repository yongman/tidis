//
// t_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"time"

	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"

	"github.com/pingcap/tidb/kv"
)

func (tidis *Tidis) Get(txn interface{}, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		v   []byte
		err error
	)

	key = SEncoder(key)

	if txn == nil {
		v, err = tidis.db.Get(key)
	} else {
		v, err = tidis.db.GetWithTxn(key, txn)
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (tidis *Tidis) MGet(txn interface{}, keys [][]byte) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	var (
		m   map[string][]byte
		err error
	)
	for i := 0; i < len(keys); i++ {
		keys[i] = SEncoder(keys[i])
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
			resp[i] = v
		} else {
			resp[i] = nil
		}
	}
	return resp, nil
}

func (tidis *Tidis) Set(txn interface{}, key, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	var err error
	key = SEncoder(key)

	if txn == nil {
		err = tidis.db.Set(key, value)
	} else {
		err = tidis.db.SetWithTxn(key, value, txn)
	}
	if err != nil {
		return err
	}
	return nil
}

func (tidis *Tidis) MSet(txn interface{}, keyvals [][]byte) (int, error) {
	if len(keyvals) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	kvm := make(map[string][]byte, len(keyvals))
	for i := 0; i < len(keyvals)-1; i += 2 {
		k, v := string(SEncoder(keyvals[i])), keyvals[i+1]
		kvm[k] = v
	}
	if txn == nil {
		return tidis.db.MSet(kvm)
	} else {
		return tidis.db.MSetWithTxn(kvm, txn)
	}
}

func (tidis *Tidis) Delete(txn interface{}, keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	var (
		ret int
		err error
	)

	if txn == nil {
		ret, err = tidis.db.Delete(nkeys)
	} else {
		ret, err = tidis.db.DeleteWithTxn(nkeys, txn)
	}
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (tidis *Tidis) Incr(key []byte, step int64) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	// inner func for tikv backend
	f := func(txn interface{}) (interface{}, error) {
		return tidis.IncrWithTxn(txn, key, step)
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

func (tidis *Tidis) IncrWithTxn(txn interface{}, key []byte, step int64) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	key = SEncoder(key)

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
		ev, err := tidis.db.GetWithTxn(key, txn)
		if err != nil {
			return nil, err
		}
		if ev == nil {
			dv = 0
		} else {
			dv, err = util.StrBytesToInt64(ev)
			if err != nil {
				return nil, err
			}
		}
		// incr by step
		dv = dv + step

		ev, err = util.Int64ToStrBytes(dv)
		if err != nil {
			return nil, err
		}
		err = txn.Set(key, ev)

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

func (tidis *Tidis) Decr(key []byte, step int64) (int64, error) {
	return tidis.Incr(key, -1*step)
}

func (tidis *Tidis) DecrWithTxn(txn interface{}, key []byte, step int64) (int64, error) {
	return tidis.IncrWithTxn(txn, key, -1*step)
}

func (tidis *Tidis) PExpireAt(key []byte, ts int64) (int, error) {

	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn interface{}) (interface{}, error) {
		return tidis.PExpireAtWithTxn(txn, key, ts)
	}

	// execute txn
	v, err := tidis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return v.(int), nil
}

func (tidis *Tidis) PExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}
		var (
			tMetaKey []byte
			tDataKey []byte
			sKey     []byte
			err      error
		)

		// check key exists
		sKey = SEncoder(key)
		v, err := tidis.db.GetWithTxn(sKey, txn)
		if err != nil {
			return 0, err
		}
		if v == nil {
			// not exists
			return 0, nil
		}

		// check expire time already set before
		tDataKey = TDSEncoder(key)
		v, err = tidis.db.GetWithTxn(tDataKey, txn)
		if err != nil {
			return 0, err
		}
		if v != nil {
			// expire already set, delete it first
			tsOld, err := util.BytesToUint64(v)
			if err != nil {
				return 0, err
			}
			tMetaKey = TMSEncoder(key, tsOld)
			err = txn.Delete(tMetaKey)
			if err != nil {
				return 0, err
			}
		}

		tMetaKey = TMSEncoder(key, uint64(ts))
		err = txn.Set(tMetaKey, []byte{0})
		if err != nil {
			return 0, err
		}

		tsRaw, _ := util.Int64ToBytes(ts)
		err = txn.Set(tDataKey, tsRaw)
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

func (tidis *Tidis) PExpire(key []byte, ms int64) (int, error) {
	return tidis.PExpireAt(key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) ExpireAt(key []byte, ts int64) (int, error) {
	return tidis.PExpireAt(key, ts*1000)
}

func (tidis *Tidis) Expire(key []byte, s int64) (int, error) {
	return tidis.PExpire(key, s*1000)
}

func (tidis *Tidis) PExpireWithTxn(txn interface{}, key []byte, ms int64) (int, error) {
	return tidis.PExpireAtWithTxn(txn, key, ms+(time.Now().UnixNano()/1000/1000))
}

func (tidis *Tidis) ExpireAtWithTxn(txn interface{}, key []byte, ts int64) (int, error) {
	return tidis.PExpireAtWithTxn(txn, key, ts*1000)
}

func (tidis *Tidis) ExpireWithTxn(txn interface{}, key []byte, s int64) (int, error) {
	return tidis.PExpireWithTxn(txn, key, s*1000)
}

func (tidis *Tidis) PTtl(txn interface{}, key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}
	var (
		ss  interface{}
		err error
		v   []byte
	)
	if txn == nil {
		ss, err = tidis.db.GetNewestSnapshot()
		if err != nil {
			return 0, err
		}
	}

	sKey := SEncoder(key)
	if txn == nil {
		v, err = tidis.db.GetWithSnapshot(sKey, ss)
	} else {
		v, err = tidis.db.GetWithTxn(sKey, txn)
	}
	if err != nil {
		return 0, err
	}
	if v == nil {
		// key not exists
		return -2, nil
	}

	tDataKey := TDSEncoder(key)

	if txn == nil {
		v, err = tidis.db.GetWithSnapshot(tDataKey, ss)
	} else {
		v, err = tidis.db.GetWithTxn(tDataKey, txn)
	}
	if err != nil {
		return 0, err
	}
	if v == nil {
		// no expire associated
		return -1, nil
	}

	ts, err := util.BytesToInt64(v)
	if err != nil {
		return 0, err
	}

	ts = ts - time.Now().UnixNano()/1000/1000
	if ts < 0 {
		ts = 0
	}

	return ts, nil
}

func (tidis *Tidis) Ttl(txn interface{}, key []byte) (int64, error) {
	ttl, err := tidis.PTtl(txn, key)
	if ttl < 0 {
		return ttl, err
	} else {
		return ttl / 1000, err
	}
}
