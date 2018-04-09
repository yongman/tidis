//
// tikv.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tikv

import (
	"github.com/YongMan/tedis/config"
	ticonfig "github.com/pingcap/tidb/config"
	ti "github.com/pingcap/tidb/store/tikv"
)

type Tikv struct {
	raw *ti.RawKVClient
}

func Open(conf *config.Config) (*Tikv, error) {
	client, err := ti.NewRawKVClient(conf.PdAddr, ticonfig.Security{})

	if err != nil {
		return nil, err
	}
	return &Tikv{raw: client}, nil
}

func (tikv *Tikv) Close() error {
	return tikv.raw.Close()
}

func (tikv *Tikv) Get(key []byte) ([]byte, error) {
	return tikv.raw.Get(key)
}

func (tikv *Tikv) Set(key []byte, value []byte) error {
	return tikv.raw.Put(key, value)
}

func (tikv *Tikv) Delete(key []byte) error {
	return tikv.raw.Delete(key)
}
