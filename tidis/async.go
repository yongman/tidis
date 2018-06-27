//
// async.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import "github.com/yongman/go/log"

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

func (tidis *Tidis) RunAsync() {
	log.Infof("Async tasks started for async deletion")
	for {
		item := <-tidis.asyncDelCh
		tidis.AsyncDelDone(item.keyType, item.ukey)

		key := string(item.ukey)
		log.Debugf("Async recv key deletion %s", key)

		switch item.keyType {
		case TLISTMETA:
			deleted, err := tidis.Ldelete(item.ukey, false)
			if err != nil {
				log.Errorf("Async delete list key:%s error, %v", key, err)
				continue
			}
			log.Debugf("Async delete list key: %s result:%d", key, deleted)
		case THASHMETA:
			deleted, err := tidis.Hclear(item.ukey, false)
			if err != nil {
				log.Errorf("Aysnc delete hash key:%s error, %v", key, err)
				continue
			}
			log.Debugf("Aysnc delete hash key:%s result:%d", key, deleted)
		case TSETMETA:
			deleted, err := tidis.Sclear(false, item.ukey)
			if err != nil {
				log.Errorf("Aysnc delete set key:%s error, %v", key, err)
				continue
			}
			log.Debugf("Aysnc delete set key:%s result:%d", key, deleted)
		case TZSETMETA:
		}
	}
}
