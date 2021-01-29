//
// gc.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"context"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/yongman/go/log"
	"github.com/yongman/go/util"
	"time"
)

// ttl for user key checker and operator

type gcChecker struct {
	interval    int
	timeout     int
	concurrency int
	tdb         *Tidis
}

func NewGCChecker(interval, timeout, concurrency int, tdb *Tidis) *gcChecker {
	return &gcChecker{
		interval:    interval,
		timeout:     timeout,
		concurrency: concurrency,
		tdb:         tdb,
	}
}

func (ch *gcChecker) Run(ctx context.Context) {
	log.Infof("start db gc checker with interval %d seconds", ch.interval)
	c := time.Tick(time.Duration(ch.interval) * time.Second)
	for {
		select {
		case <-c:
			if ch.interval == 0 {
				continue
			}
			if !ch.tdb.conf.Tidis.DBGCEnabled {
				continue
			}
			if !ch.tdb.IsLeader() {
				continue
			}
			// add leader check
			lastPoint, err := ch.loadSafePoint()
			if err != nil {
				log.Errorf("load last safe point failed, error: %s", err.Error())
				continue
			}

			newPoint, err := ch.getNewPoint(time.Duration(ch.timeout) * time.Second)
			if err != nil {
				log.Errorf("get db safe point for gc error: %s", err.Error())
				continue
			}

			lastPointTime := time.Unix(int64(lastPoint), 0)
			if newPoint.Sub(lastPointTime) < time.Duration(ch.timeout)*time.Second {
				log.Warnf("do not need run gc this time, %d seconds past after last gc", newPoint.Sub(lastPointTime)/time.Second)
				continue
			}

			safePoint := oracle.ComposeTS(oracle.GetPhysical(newPoint), 0)
			log.Debugf("start run db gc with safePoint %d, concurrency: %d", safePoint, 3)
			err = ch.tdb.RunGC(safePoint, ch.concurrency)
			if err != nil {
				log.Errorf("run gc failed, error: %s", err.Error())
			}
			err = ch.saveSafePoint(uint64(newPoint.Unix()))
		case <-ctx.Done():
			return
		}
	}
}

func (ch *gcChecker) getNewPoint(ttl time.Duration) (time.Time, error) {
	ver, err := ch.tdb.GetCurrentVersion()
	if err != nil {
		return time.Time{}, err
	}
	physical := oracle.ExtractPhysical(ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	now := time.Unix(sec, nsec)
	safePoint := now.Add(-ttl)
	return safePoint, nil
}

func (ch *gcChecker) saveSafePoint(ts uint64) error {
	gcPointKey := RawSysGCPointKey()
	val, err := util.Uint64ToBytes(ts)
	if err != nil {
		return err
	}
	err = ch.tdb.db.Set(gcPointKey, val)
	return err
}

func (ch *gcChecker) loadSafePoint() (uint64, error) {
	gcPointKey := RawSysGCPointKey()
	val, err := ch.tdb.db.Get(gcPointKey)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	ts, err := util.BytesToUint64(val)
	if err != nil {
		return 0, err
	}
	return ts, nil
}
