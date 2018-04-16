//
// store.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package store

import (
	"github.com/yongman/tidis/config"
	"github.com/yongman/tidis/store/tikv"
)

func init() {
}

func Open(conf *config.Config) (DB, error) {
	db, err := tikv.Open(conf)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Close(db DB) error {
	return db.Close()
}
