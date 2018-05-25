//
// db.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package store

// backend db interface
type DB interface {
	Close() error
	Get(key []byte) ([]byte, error)
	GetWithTxn(key []byte, txn1 interface{}) ([]byte, error)
	GetWithSnapshot(key []byte, ss interface{}) ([]byte, error)
	GetNewestSnapshot() (interface{}, error)
	GetWithVersion(key []byte, version uint64) ([]byte, error)
	MGet(key [][]byte) (map[string][]byte, error)
	MGetWithVersion(key [][]byte, version uint64) (map[string][]byte, error)
	MGetWithSnapshot(keys [][]byte, ss interface{}) (map[string][]byte, error)
	MGetWithTxn(keys [][]byte, txn1 interface{}) (map[string][]byte, error)
	Set(key []byte, value []byte) error
	SetWithTxn(key []byte, value []byte, txn interface{}) error
	MSet(kv map[string][]byte) (int, error)
	MSetWithTxn(kvm map[string][]byte, txn interface{}) (int, error)
	Delete(keys [][]byte) (int, error)
	DeleteWithTxn(keys [][]byte, txn interface{}) (int, error)
	BatchInTxn(f func(txn interface{}) (interface{}, error)) (interface{}, error)
	GetRangeKeysWithFrontier(start []byte, withstart bool, end []byte, withend bool, offset, limit uint64, snapshot interface{}) ([][]byte, error)
	GetRangeKeysWithFrontierWithTxn(start []byte, withstart bool, end []byte, withend bool, offset, limit uint64, txn interface{}) ([][]byte, error)
	GetRangeKeys(start []byte, end []byte, offset, limit uint64, snapshot interface{}) ([][]byte, error)
	GetRangeKeysWithTxn(start []byte, end []byte, offset, limit uint64, txn interface{}) ([][]byte, error)
	GetRangeKeysCount(start []byte, withstart bool, end []byte, withend bool, limit uint64, snapshot interface{}) (uint64, error)
	GetRangeKeysCountWithTxn(start []byte, withstart bool, end []byte, withend bool, limit uint64, txn interface{}) (uint64, error)
	GetRangeVals(start []byte, end []byte, limit uint64, snapshot interface{}) ([][]byte, error)
	GetRangeValsWithTxn(start []byte, end []byte, limit uint64, txn1 interface{}) ([][]byte, error)
	GetRangeKeysVals(start []byte, end []byte, limit uint64, snapshot interface{}) ([][]byte, error)
	GetRangeKeysValsWithTxn(start []byte, end []byte, limit uint64, txn1 interface{}) ([][]byte, error)
	DeleteRange(start []byte, end []byte, limit uint64) (uint64, error)
	DeleteRangeWithTxn(start []byte, end []byte, limit uint64, txn1 interface{}) (uint64, error)

	BatchWithTxn(f func(txn interface{}) (interface{}, error), txn1 interface{}) (interface{}, error)
	NewTxn() (interface{}, error)
}

// iterator for backend store
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}
