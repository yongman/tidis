//
// tikv_test.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tikv

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/tidis/config"
)

func TestNext(t *testing.T) {
	conf := &config.Config{PdAddr: "10.240.200.200:2379"}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}

	ss, err := tikv.GetNewestSnapshot()
	if err != nil {
		fmt.Println(err)
	}

	startKey := []byte{0}
	endKey := []byte{255, 255, 255, 255, 255, 255, 255}

	// reverse not support yet by tikv
	iter, err := NewIterator(startKey, endKey, ss.(kv.Snapshot), false)
	if err != nil {
		fmt.Println(err)
	}

	var keys int
	for iter.Valid() && keys < 100 {
		keys++
		fmt.Println("key:", iter.Key())
		iter.Next()
	}
	iter.Close()
}
