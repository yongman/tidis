//
// leader.go
// Copyright (C) 2021 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"context"
	"github.com/yongman/go/log"
	"time"
)

// ttl for user key checker and operator

type leaderChecker struct {
	interval int
	duration int
	tdb      *Tidis
}

func NewLeaderChecker(interval, duration int, tdb *Tidis) *leaderChecker {
	return &leaderChecker{
		interval: interval,
		duration: duration,
		tdb:      tdb,
	}
}

func (ch *leaderChecker) Run(ctx context.Context) {
	log.Infof("start leader checker with interval %d seconds", ch.interval)
	c := time.Tick(time.Duration(ch.interval) * time.Second)
	for {
		select {
			case <-c:
				ch.tdb.CheckLeader(ch.duration)
			case <-ctx.Done():
				return
		}

	}
}
