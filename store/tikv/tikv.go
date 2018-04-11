//
// tikv.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tikv

import (
	"fmt"

	"github.com/YongMan/tedis/config"
	"github.com/pingcap/tidb/kv"
	ti "github.com/pingcap/tidb/store/tikv"
	"golang.org/x/net/context"
)

type Tikv struct {
	store kv.Storage
}

func Open(conf *config.Config) (*Tikv, error) {
	d := ti.Driver{}
	store, err := d.Open(fmt.Sprintf("tikv://%s/pd?cluster=1", conf.PdAddr))
	if err != nil {
		return nil, err
	}
	return &Tikv{store: store}, nil
}

func (tikv *Tikv) Close() error {
	return tikv.store.Close()
}

func (tikv *Tikv) Get(key []byte) ([]byte, error) {
	ss, err := tikv.store.GetSnapshot(kv.MaxVersion)
	if err != nil {
		return nil, err
	}
	v, err := ss.Get(key)
	if err != nil {
		if kv.IsErrNotFound(err) {
			return nil, nil
		}
	}
	return v, err
}

func (tikv *Tikv) MGet(keys [][]byte) (map[string][]byte, error) {
	ss, err := tikv.store.GetSnapshot(kv.MaxVersion)
	if err != nil {
		return nil, err
	}
	// TODO
	nkeys := make([]kv.Key, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = keys[i]
	}
	return ss.BatchGet(nkeys)
}

// set must be run in txn
func (tikv *Tikv) Set(key []byte, value []byte) error {
	// get txn, get ts from pd oracle
	txn, err := tikv.store.Begin()
	if err != nil {
		return err
	}

	err = txn.Set(key, value)
	if err != nil {
		txn.Rollback()
		return err
	}

	// commit txn
	err = txn.Commit(context.Background())
	if err != nil {
		// rollback without retry
		txn.Rollback()
		return err
	}

	return nil
}

// map key cannot be []byte, use string
func (tikv *Tikv) MSet(kv map[string][]byte) (int, error) {
	// get txn
	txn, err := tikv.store.Begin()
	if err != nil {
		return 0, err
	}

	for k, v := range kv {
		err = txn.Set([]byte(k), v)
		if err != nil {
			txn.Rollback()
			return 0, err
		}
	}

	err = txn.Commit(context.Background())
	if err != nil {
		txn.Rollback()
		return 0, err
	}
	return len(kv), nil
}

func (tikv *Tikv) Delete(keys [][]byte) (int, error) {
	var deleted int = 0
	txn, err := tikv.store.Begin()
	if err != nil {
		return 0, err
	}

	for _, k := range keys {
		v, err := tikv.Get(k)
		if v != nil {
			deleted++
		}
		err = txn.Delete(k)
		if err != nil {
			txn.Rollback()
			return 0, err
		}
	}

	err = txn.Commit(context.Background())
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	return deleted, nil
}

func (tikv *Tikv) BatchInTxn(f func(txn interface{}) (interface{}, error)) (interface{}, error) {
	txn, err := tikv.store.Begin()
	if err != nil {
		return nil, err
	}

	res, err := f(txn)
	if err != nil {
		txn.Rollback()
		return nil, err
	}
	err = txn.Commit(context.Background())
	if err != nil {
		txn.Rollback()
		return nil, err
	}
	return res, nil
}
