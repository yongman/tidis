//
// t_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

import (
	"github.com/YongMan/go/util"
	"github.com/YongMan/tedis/terror"

	"github.com/pingcap/tidb/kv"
)

func (tedis *Tedis) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	key = SEncoder(key)

	v, err := tedis.db.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (tedis *Tedis) MGet(keys [][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	m, err := tedis.db.MGet(nkeys)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (tedis *Tedis) Set(key, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	key = SEncoder(key)
	err := tedis.db.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (tedis *Tedis) MSet(kv map[string][]byte) (int, error) {
	if len(kv) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	return tedis.db.MSet(kv)
}

func (tedis *Tedis) Delete(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	ret, err := tedis.db.Delete(nkeys)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (tedis *Tedis) Incr(key []byte, step int64) (int64, error) {
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
	ret, err := tedis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	retInt, ok := ret.(int64)
	if !ok {
		return 0, terror.ErrTypeAssertion
	}
	return retInt, nil
}

func (tedis *Tedis) Decr(key []byte, step int64) (int64, error) {
	return tedis.Incr(key, -1*step)
}
