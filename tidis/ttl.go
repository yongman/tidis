//
// ttl.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"time"
)

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
	for _ = range c {
		switch ch.dataType {
		case TSTRING:

		case THASHMETA:

		case TLISTMETA:

		case TSETMETA:
		case TZSETMETA:
		}

	}
}
