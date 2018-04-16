//
// t_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
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

func (tidis *Tidis) MGet(keys [][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	m, err := tidis.db.MGet(nkeys)
	if err != nil {
		return nil, err
	}
	return m, nil
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

func (tidis *Tidis) MSet(kv map[string][]byte) (int, error) {
	if len(kv) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	return tidis.db.MSet(kv)
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
