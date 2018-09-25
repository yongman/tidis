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
	ti "github.com/yongman/tidis/store/tikv"
	"github.com/yongman/tidis/terror"
)

// check a ttl value is expired
func TTLExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	return ttl <= time.Now().UnixNano()/1000/1000
}

// ttl for user key checker and operater

type ttlChecker struct {
	dataType   byte
	maxPerLoop int
	interval   int
	tdb        *Tidis
}

func NewTTLChecker(datatype byte, max, interval int, tdb *Tidis) *ttlChecker {
	return &ttlChecker{
		dataType:   datatype,
		maxPerLoop: max,
		interval:   interval,
		tdb:        tdb,
	}
}

func (ch *ttlChecker) Run() {
	c := time.Tick(time.Duration(ch.interval) * time.Millisecond)
	flagFalse := false
	for _ = range c {
		switch ch.dataType {
		case TSTRING:
			startKey := TMSEncoder([]byte{0}, 0)
			endKey := TMSEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss := txn.GetSnapshot()
				// create iterater
				it, err := ti.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}
				defer it.Close()

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					// decode user key
					key, ts, err := TMSDecoder(it.Key())
					if err != nil {
						return 0, err
					}
					if ts > uint64(time.Now().UnixNano()/1000/1000) {
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
					loops--
				}
				return ch.maxPerLoop - loops, nil
			}

			// exe txn
			v, err := ch.tdb.db.BatchInTxn(f)
			if err != nil {
				log.Warnf("ttl checker decode key failed, %s", err.Error())
			}
			if v == nil {
				log.Warnf("BatchInTxn execute failed")
				continue
			}
			log.Debugf("string ttl checker delete %d keys in this loop", v.(int))

		case THASHMETA:
			startKey := TMHEncoder([]byte{0}, 0)
			endKey := TMHEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss := txn.GetSnapshot()

				it, err := ti.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}
				defer it.Close()

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					// decode out user key
					key, ts, err := TMHDecoder(it.Key())
					if err != nil {
						return 0, err
					}

					if ts > uint64(time.Now().UnixNano()/1000/1000) {
						break
					}

					// delete ttl meta key
					if err = txn.Delete(it.Key()); err != nil {
						return 0, err
					}
					// delete entire user key
					flag := false
					if _, err = ch.tdb.HclearWithTxn(txn1, key, &flag); err != nil {
						return 0, err
					}

					it.Next()
					loops--
				}

				return ch.maxPerLoop - loops, nil
			}

			// execute txn
			v, err := ch.tdb.db.BatchInTxn(f)
			if err != nil {
				log.Warnf("ttl checker hashkey failed, %s", err.Error())
			}
			if v == nil {
				log.Warnf("BatchInTxn execute failed")
				continue
			}
			log.Debugf("hash ttl checker delete %d keys in this loop", v.(int))

		case TLISTMETA:
			startKey := TMLEncoder([]byte{0}, 0)
			endKey := TMLEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss := txn.GetSnapshot()

				it, err := ti.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}
				defer it.Close()

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					// decode out user key
					key, ts, err := TMLDecoder(it.Key())
					if err != nil {
						return 0, err
					}

					if ts > uint64(time.Now().UnixNano()/1000/1000) {
						break
					}

					// delete ttl meta key
					if err = txn.Delete(it.Key()); err != nil {
						return 0, err
					}
					// delete entire user key
					flag := false
					if _, err = ch.tdb.LdelWithTxn(txn1, key, &flag); err != nil {
						return 0, err
					}

					it.Next()
					loops--
				}

				return ch.maxPerLoop - loops, nil
			}

			// execute txn
			v, err := ch.tdb.db.BatchInTxn(f)
			if err != nil {
				log.Warnf("ttl checker hashkey failed, %s", err.Error())
			}
			if v == nil {
				log.Warnf("BatchInTxn execute failed")
				continue
			}
			log.Debugf("list ttl checker delete %d keys in this loop", v.(int))

		case TSETMETA:
			startKey := TMSetEncoder([]byte{0}, 0)
			endKey := TMSetEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss := txn.GetSnapshot()

				it, err := ti.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}
				defer it.Close()

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					// decode out user key
					key, ts, err := TMSetDecoder(it.Key())
					if err != nil {
						return 0, err
					}

					if ts > uint64(time.Now().UnixNano()/1000/1000) {
						break
					}

					// delete ttl meta key
					if err = txn.Delete(it.Key()); err != nil {
						return 0, err
					}
					// delete entire user key
					if _, err = ch.tdb.SclearKeyWithTxn(txn1, key, &flagFalse, true); err != nil {
						return 0, err
					}

					it.Next()
					loops--
				}

				return ch.maxPerLoop - loops, nil
			}

			// execute txn
			v, err := ch.tdb.db.BatchInTxn(f)
			if err != nil {
				log.Warnf("ttl checker hashkey failed, %s", err.Error())
			}
			if v == nil {
				log.Warnf("BatchInTxn execute failed")
				continue
			}
			log.Debugf("set ttl checker delete %d keys in this loop", v.(int))

		case TZSETMETA:
			startKey := TMZEncoder([]byte{0}, 0)
			endKey := TMZEncoder([]byte{0}, math.MaxInt64)

			f := func(txn1 interface{}) (interface{}, error) {
				txn, ok := txn1.(kv.Transaction)
				if !ok {
					return 0, terror.ErrBackendType
				}

				var loops int

				ss := txn.GetSnapshot()

				it, err := ti.NewIterator(startKey, endKey, ss, false)
				if err != nil {
					return 0, err
				}
				defer it.Close()

				loops = ch.maxPerLoop
				for loops > 0 && it.Valid() {
					// decode out user key
					key, ts, err := TMZDecoder(it.Key())
					if err != nil {
						return 0, err
					}

					if ts > uint64(time.Now().UnixNano()/1000/1000) {
						break
					}

					// delete ttl meta key
					if err = txn.Delete(it.Key()); err != nil {
						return 0, err
					}
					// delete entire user key
					if _, err = ch.tdb.ZremrangebyscoreWithTxn(txn1, key, SCORE_MIN, SCORE_MAX, &flagFalse); err != nil {
						return 0, err
					}

					it.Next()
					loops--
				}

				return ch.maxPerLoop - loops, nil
			}

			// execute txn
			v, err := ch.tdb.db.BatchInTxn(f)
			if err != nil {
				log.Warnf("ttl checker zset key failed, %s", err.Error())
			}
			if v == nil {
				log.Warnf("BatchInTxn execute failed")
				continue
			}
			log.Debugf("zset ttl checker delete %d keys in this loop", v.(int))
		}

	}
}
