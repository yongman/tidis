//
// db.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package store

type DB interface {
	Close() error
	Get(key []byte) ([]byte, error)
	MGet(key [][]byte) (map[string][]byte, error)
	Set(key []byte, value []byte) error
	MSet(kv map[string][]byte) (int, error)
	Delete(keys [][]byte) (int, error)
	BatchInTxn(f func(txn interface{}) (interface{}, error)) (interface{}, error)
}
