//
// tidis.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

// wrapper for kv storage engine  operation

import (
	"sync"

	"github.com/yongman/tidis/config"
	"github.com/yongman/tidis/store"
)

type Tidis struct {
	conf *config.Config
	db   store.DB

	wLock sync.RWMutex
	Lock  sync.Mutex
	wg    sync.WaitGroup
}

func NewTidis(conf *config.Config) (*Tidis, error) {
	var err error

	tidis := &Tidis{
		conf: conf,
	}
	tidis.db, err = store.Open(conf)
	if err != nil {
		return nil, err
	}

	return tidis, nil
}

func (tidis *Tidis) Close() error {
	err := tidis.db.Close()
	if err != nil {
		return err
	}
	return nil
}
