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

func (tidis *Tidis) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	key = SEncoder(key)

	v, err := tidis.db.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (tidis *Tidis) MGet(keys [][]byte) ([]interface{}, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	for i := 0; i < len(keys); i++ {
		keys[i] = SEncoder(keys[i])
	}

	m, err := tidis.db.MGet(keys)
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

func (tidis *Tidis) Set(key, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	key = SEncoder(key)
	err := tidis.db.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (tidis *Tidis) MSet(keyvals [][]byte) (int, error) {
	if len(keyvals) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	kvm := make(map[string][]byte, len(keyvals))
	for i := 0; i < len(keyvals)-1; i += 2 {
		k, v := string(SEncoder(keyvals[i])), keyvals[i+1]
		kvm[k] = v
	}

	return tidis.db.MSet(kvm)
}

func (tidis *Tidis) Delete(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	ret, err := tidis.db.Delete(nkeys)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (tidis *Tidis) Incr(key []byte, step int64) (int64, error) {
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
		ev, err := txn.GetSnapshot().Get(key)
		if err != nil {
			if kv.IsErrNotFound(err) {
				dv = 0
			} else {
				return nil, err
			}
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

func (tidis *Tidis) Decr(key []byte, step int64) (int64, error) {
	return tidis.Incr(key, -1*step)
}

func (tidis *Tidis) PExpireAt(key []byte, ts int64) (int, error) {
	if len(key) == 0 || ts < 0 {
		return 0, terror.ErrCmdParams
	}

	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return 0, terror.ErrBackendType
		}

		ss := txn.GetSnapshot()
		// check key exists
		sKey := SEncoder(key)
		v, err := tidis.db.GetWithSnapshot(sKey, ss)
		if err != nil {
			return 0, err
		}
		if v == nil {
			// not exists
			return 0, nil
		}

		tMetaKey := TMSEncoder(key, uint64(ts))
		tDataKey := TDSEncoder(key)

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
	v, err := tidis.db.BatchInTxn(f)
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

func (tidis *Tidis) PTtl(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	ss, err := tidis.db.GetNewestSnapshot()
	if err != nil {
		return 0, err
	}

	sKey := SEncoder(key)
	v, err := tidis.db.GetWithSnapshot(sKey, ss)
	if err != nil {
		return 0, err
	}
	if v == nil {
		// key not exists
		return -2, nil
	}

	tDataKey := TDSEncoder(key)

	v, err = tidis.db.GetWithSnapshot(tDataKey, ss)
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

func (tidis *Tidis) Ttl(key []byte) (int64, error) {
	ttl, err := tidis.PTtl(key)
	return ttl / 1000, err
}
