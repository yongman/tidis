//
// ttl.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"math"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/log"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

// ttl for user key checker and operater

type ttlChecker struct {
	dataType   int
	maxPerLoop int
	interval   int
	tdb        *tidis.Tidis
}

func NewTTLChecker(datatype, max, interval int, tdb *tidis.Tidis) *ttlChecker {
	return &ttlChecker{
		dataType:   datatype,
		maxPerLoop: max,
		interval:   interval,
		tdb:        tdb,
	}
}

func (ch *ttlChecker) Run() {
	c := time.Tick(ch.interval * time.Millisecond)
	for t := range c {
		if ch.dataType == TSTRING {
			startKey := TMSEncoder([]byte{0}, 0)
			endKey := TMSEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss, err := txn.GetSnapshot()
				if err != nil {
					return 0, err
				}
				// create iterater
				it, err := ch.tdb.db.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					loops--
					// decode user key
					key, ts, err := TMSDecoder(it.Key())
					if err != nil {
						return 0, err
					}
					if ts > time.Now.UnixNano()/1000/1000 {
						// no key expired
						break
					}
					// delete ttlmetakey ttldatakey key
					tDataKey := TDSEncoder(key)
					sKey := SEncoder(key)

					if err = txn.Delete(it.Key()); err != nil {
						return 0, err
					}
					if err = txn.Delete(tDataKey); err != nil {
						return 0, err
					}
					if err = txn.Delete(sKey); err != nil {
						return 0, err
					}

					it.Next()
				}
				return ch.maxPerLoop - loops, nil
			}

			// exe txn
			v, err := ch.tdb.db.BachInTxn(f)
			if err != nil {
				log.Warnf("ttl checker decode key failed, %s", err.Error())
			}
			log.Infof("ttl checker delete %d keys in this loop", v.(int))
		}
	}
}
