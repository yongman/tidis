//
// tedis.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

// wrapper for kv storage engine  operation

import (
	"sync"

	"github.com/YongMan/tedis/config"
	"github.com/YongMan/tedis/store"
)

type Tedis struct {
	conf *config.Config
	db   store.DB

	wLock sync.RWMutex
	Lock  sync.Mutex
	wg    sync.WaitGroup
}

func NewTedis(conf *config.Config) (*Tedis, error) {
	var err error

	tedis := &Tedis{
		conf: conf,
	}
	tedis.db, err = store.Open(conf)
	if err != nil {
		return nil, err
	}

	return tedis, nil
}

func (tedis *Tedis) Close() error {
	err := tedis.db.Close()
	if err != nil {
		return err
	}
	return nil
}
