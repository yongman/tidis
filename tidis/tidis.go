//
// tidis.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

// wrapper for kv storage engine  operation

import (
	"github.com/google/uuid"
	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/log"
	"github.com/yongman/go/util"
	"sync"
	"time"

	"github.com/deckarep/golang-set"
	"github.com/yongman/tidis/config"
	"github.com/yongman/tidis/store"
)

type Tidis struct {
	uuid uuid.UUID
	conf *config.Config
	db   store.DB

	wLock sync.RWMutex
	Lock  sync.Mutex
	wg    sync.WaitGroup

	asyncDelCh  chan AsyncDelItem
	asyncDelSet mapset.Set
}

func NewTidis(conf *config.Config) (*Tidis, error) {
	var err error

	tidis := &Tidis{
		uuid:        uuid.New(),
		conf:        conf,
		asyncDelCh:  make(chan AsyncDelItem, 10240),
		asyncDelSet: mapset.NewSet(),
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

func (tidis *Tidis) NewTxn() (interface{}, error) {
	return tidis.db.NewTxn()
}

func (tidis *Tidis) TenantId() string {
	return tidis.conf.Tidis.TenantId
}

func (tidis *Tidis) RawKeyPrefix(dbid uint8, key []byte) []byte {
	return RawKeyPrefix(tidis.TenantId(), dbid, key)
}

func (tidis *Tidis) RunGC(safePoint uint64, concurrency int) error {
	return tidis.db.RunGC(safePoint, concurrency)
}

func (tidis *Tidis) IsLeader() bool {
	leaderKey := RawSysLeaderKey()
	val, err := tidis.db.Get(leaderKey)
	if err != nil {
		return false
	}

	// val should be in format uuid(36 bytes)+time(8 bytes)
	if val == nil || len(val) < 44 {
		return false
	}

	uuid, tsBytes := val[:36], val[36:]

	now := time.Now().UTC()
	ts, err := util.BytesToInt64(tsBytes)
	if err != nil {
		return false
	}
	tsTime := time.Unix(ts, 0).UTC()

	if string(uuid) == tidis.uuid.String() && now.Sub(tsTime) <
		time.Duration(tidis.conf.Tidis.LeaderLeaseDuration) * time.Second {
		return true
	}
	return false
}

func (tidis *Tidis) CheckLeader(leaderLeaseDuration int) {
	// check leader lease timeout, release if needed
	// 1. check is leader
	// 2. check leader and lease time out
	// 3. try to be leader and write lease uuid and time
	f := func(txn interface{}) (interface{}, error) {
		leaderKey := RawSysLeaderKey()
		val, err := tidis.db.GetWithTxn(leaderKey, txn)
		if err != nil {
			return nil, err
		}

		if len(val) == 44 {
			uuid, tsBytes := val[:36], val[36:]

			now := time.Now().UTC()
			ts, err := util.BytesToInt64(tsBytes)
			if err != nil {
				return nil, err
			}
			tsTime := time.Unix(ts, 0).UTC()

			if now.Sub(tsTime) < time.Duration(leaderLeaseDuration) * time.Second {
				if string(uuid) == tidis.uuid.String() {
					log.Infof("I am already leader, renew lease with uuid %s and timestamp %d", uuid, ts)
					err = tidis.renewLeader(txn)
					if err != nil {
						return false, err
					}
					return true, nil
				} else {
					log.Infof("leader already exists in lease duration, stay in follower")
					return false, nil
				}
			}
		}

		// renew lease with my uuid and timestamp
		err = tidis.renewLeader(txn)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	log.Infof("check leader lease with uuid %s", tidis.uuid)
	isLeader, err := tidis.db.BatchInTxn(f)
	if err != nil {
		log.Errorf("check leader and renew lease failed, error: %s", err.Error())
	}
	if isLeader.(bool) {
		log.Infof("I am leader with new release")
	}
}

func (tidis *Tidis) renewLeader(txn interface{}) error {
	leaderKey := RawSysLeaderKey()
	now := time.Now().UTC().Unix()
	tsBytes, _ := util.Int64ToBytes(now)

	log.Infof("try to renew lease with uuid %s and timestamp %d", tidis.uuid, now)

	val := append([]byte(tidis.uuid.String()), tsBytes...)

	txn1, _ := txn.(kv.Transaction)
	return txn1.Set(leaderKey, val)
}
