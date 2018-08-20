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

	"github.com/yongman/tidis/config"
)

func TestSet(t *testing.T) {
	conf := &config.Config{PdAddr: "10.240.200.200:2379"}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}
	err = tikv.Set([]byte("foo"), []byte("bar"))
	if err != nil {
		fmt.Println(err)
	}
}

func TestGet(t *testing.T) {
	conf := &config.Config{PdAddr: "10.240.200.200:2379"}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}
	value, err := tikv.Get([]byte("foo"))
	fmt.Println(string(value), err)
}

func TestDelete(t *testing.T) {
	conf := &config.Config{PdAddr: "10.240.200.200:2379"}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}
	keys := make([][]byte, 1)
	keys[0] = []byte("foo")
	_, err = tikv.Delete(keys)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("after delete")
	value, err := tikv.Get([]byte("foo"))
	fmt.Println(string(value), err)
}
