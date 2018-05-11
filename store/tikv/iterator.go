//
// iterator.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tikv

import (
	"bytes"

	"github.com/pingcap/tidb/kv"
)

type Iterator struct {
	it      kv.Iterator
	reverse bool
	start   []byte
	stop    []byte
}

// reverse not support by tikv yet
func NewIterator(start []byte, stop []byte, snapshot kv.Snapshot, reverse bool) (*Iterator, error) {
	var (
		it  kv.Iterator
		err error
	)
	if !reverse {
		it, err = snapshot.Seek(start)
	} else {
		it, err = snapshot.SeekReverse(stop)
	}
	if err != nil {
		return nil, err
	}
	return &Iterator{
		it:      it,
		reverse: reverse,
		start:   start,
		stop:    stop,
	}, nil
}

func (it *Iterator) Valid() bool {
	if !it.it.Valid() {
		return false
	}
	if !it.reverse {
		if bytes.Compare(it.Key(), it.stop) > 0 {
			return false
		}
	} else {
		if bytes.Compare(it.Key(), it.start) < 0 {
			return false
		}
	}
	return true
}

func (it *Iterator) Key() []byte {
	return it.it.Key()
}

func (it *Iterator) Value() []byte {
	return it.it.Value()
}

func (it *Iterator) Next() error {
	return it.it.Next()
}

func (it *Iterator) Close() {
	it.it.Close()
}
