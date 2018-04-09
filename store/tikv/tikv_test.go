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

	"github.com/YongMan/tedis/config"
)

func TestSet(t *testing.T) {
	conf := &config.Config{PdAddr: []string{"tikv://10.240.200.200:2379/pd?cluster=1"}}
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
	conf := &config.Config{PdAddr: []string{"tikv://10.240.200.200:2379/pd?cluster=1"}}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}
	value, err := tikv.Get([]byte("foo"))
	fmt.Println(string(value))
}

func TestDelete(t *testing.T) {
	conf := &config.Config{PdAddr: []string{"tikv://10.240.200.200:2379/pd?cluster=1"}}
	tikv, err := Open(conf)
	if err != nil {
		fmt.Println(err)
	}
	err = tikv.Delete([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("after delete")
	value, err := tikv.Get([]byte("foo"))
	fmt.Println(string(value))
}
