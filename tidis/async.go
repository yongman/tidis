//
// async.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"context"
	"github.com/yongman/go/log"
)

type AsyncDelItem struct {
	keyType byte   // user key type
	ukey    []byte // user key
}

func (tidis *Tidis) AsyncDelAdd(keyType byte, ukey []byte) error {
	tidis.Lock.Lock()
	defer tidis.Lock.Unlock()

	key := string(keyType) + string(ukey)
	// key already added to chan queue
	if tidis.asyncDelSet.Contains(key) {
		return nil
	}
	tidis.asyncDelCh <- AsyncDelItem{keyType: keyType, ukey: ukey}
	tidis.asyncDelSet.Add(key)

	return nil
}

func (tidis *Tidis) AsyncDelDone(keyType byte, ukey []byte) error {
	tidis.Lock.Lock()
	defer tidis.Lock.Unlock()

	key := string(keyType) + string(ukey)
	if tidis.asyncDelSet.Contains(key) {
		tidis.asyncDelSet.Remove(key)
	}
	return nil
}

func (tidis *Tidis) RunAsync(ctx context.Context) {
	// TODO
	log.Infof("Async tasks started for async deletion")
	for {
		select {
		case item := <-tidis.asyncDelCh:
			key := string(item.ukey)
			log.Debugf("Async recv key deletion %s", key)

			switch item.keyType {
			case TLISTMETA:
			case THASHMETA:
			case TSETMETA:
			case TZSETMETA:
			default:
		}
		case <-ctx.Done():
			return
		}
	}
}
