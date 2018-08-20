//
// t_hash_test.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"fmt"
	"testing"

	"github.com/yongman/tidis/config"
)

var tidis *Tidis

const TestKey string = "__h_test__"

func init() {
	conf := config.LoadConfig()

	var err error
	tidis, err = NewTidis(conf)
	if err != nil {
		return
	}
	fmt.Println("create app")
}

func TestHset(t *testing.T) {
	_, err := tidis.Hset([]byte(TestKey), []byte("foo"), []byte("bar"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestHget(t *testing.T) {
	ret, err := tidis.Hget([]byte(TestKey), []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(ret) != "bar" {
		t.Fatal("hget data error")
	}
}

func TestHstrlen(t *testing.T) {
	ret, err := tidis.Hstrlen([]byte(TestKey), []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	if ret != 3 {
		t.Fatalf("error value length: %d", ret)
	}
}

func TestHexists(t *testing.T) {
	ret, err := tidis.Hexists([]byte(TestKey), []byte("foo"))

	if err != nil {
		t.Fatal(err)
	}
	if ret == false {
		t.Fatal("data loss")
	}
}

func TestHmset(t *testing.T) {
	err := tidis.Hmset([]byte(TestKey), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2"))
	if err != nil {
		t.Fatal(err)
	}

	//hmget
	v, err := tidis.Hmget([]byte(TestKey), []byte("f1"), []byte("f2"))
	if err != nil {
		t.Fatal(err)
	}

	v0, _ := v[0].([]byte)
	v1, _ := v[1].([]byte)
	if len(v) != 2 || string(v0) != "v1" || string(v1) != "v2" {
		t.Fatal("hmget hmset value error")
	}
}

func TestHdel(t *testing.T) {
	v, err := tidis.Hdel([]byte(TestKey), []byte("f1"))
	if err != nil {
		t.Fatal(err)
	}

	if v != 1 {
		t.Fatalf("hdel failed, %v", v)
	}
}

func TestHsetnx(t *testing.T) {
	v, err := tidis.Hsetnx([]byte(TestKey), []byte("f1"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}

	if v != 1 {
		t.Fatal("hsetnx failed")
	}

	v, err = tidis.Hsetnx([]byte(TestKey), []byte("f1"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}

	if v != 0 {
		t.Fatal("hsetnx failed")
	}
}

func TestHkeys(t *testing.T) {
	_, err := tidis.Hset([]byte(TestKey), []byte("f4"), []byte("v4"))
	if err != nil {
		t.Fatal(err)
	}

	keys, err := tidis.Hkeys([]byte(TestKey))
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys {
		kk, _ := k.([]byte)
		fmt.Println(string(kk))
	}
}

func TestHvals(t *testing.T) {
	_, err := tidis.Hset([]byte(TestKey), []byte("f4"), []byte("v4"))
	if err != nil {
		t.Fatal(err)
	}

	vals, err := tidis.Hvals([]byte(TestKey))
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range vals {
		vv, _ := v.([]byte)
		fmt.Println(string(vv))
	}
}

func TestHgetall(t *testing.T) {
	_, err := tidis.Hset([]byte(TestKey), []byte("f4"), []byte("v4"))
	if err != nil {
		t.Fatal(err)
	}

	kvs, err := tidis.Hgetall([]byte(TestKey))
	if err != nil {
		t.Fatal(err)
	}

	for i, kv := range kvs {
		kvkv, _ := kv.([]byte)
		fmt.Printf("index:%d %s\n", i, string(kvkv))
	}
}
