//
// t_object.go
// Copyright (C) 2020 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

import (
	"github.com/pingcap/tidb/kv"
)

type IObject interface {
	ObjectExpired(now uint64) bool
	SetExpireAt(ts uint64)
	TTL(now uint64) uint64
	IsExpireSet() bool
}

type Object struct {
	Type     byte
	Tomb     byte
	ExpireAt uint64
}

func MarshalObj(obj IObject) []byte {
	switch v := obj.(type) {
	case *StringObj:
		return MarshalStringObj(v)
	case *HashObj:
		return MarshalHashObj(v)
	case *ListObj:
		return MarshalListObj(v)
	case *SetObj:
		return MarshalSetObj(v)
	case *ZSetObj:
		return MarshalZSetObj(v)
	}
	return nil
}

func (obj *Object) ObjectExpired(now uint64) bool {
	if obj.ExpireAt == 0 || obj.ExpireAt > now {
		return false
	}
	return true
}

func (obj *Object) IsExpireSet() bool {
	if obj.ExpireAt ==  0 {
		return false
	}
	return true
}


func (obj *Object) SetExpireAt(ts uint64) {
	obj.ExpireAt = ts
}

func (obj *Object) TTL(now uint64) uint64 {
	if obj.ExpireAt > now {
		return obj.ExpireAt - now
	} else {
		return 0
	}
}

func (tidis *Tidis) GetObject(dbId uint8, txn interface{}, key []byte) (byte, IObject, error) {
	metaKey := tidis.RawKeyPrefix(dbId, key)

	var (
		metaValue []byte
		err error
	)

	if txn != nil {
		metaValue, err = tidis.db.GetWithTxn(metaKey, txn)
	} else {
		metaValue, err = tidis.db.Get(metaKey)
	}
	if err != nil {
		return 0, nil, err
	}
	if metaValue == nil {
		return 0, nil, nil
	}

	var obj IObject

	// unmarshal with type
	objType := metaValue[0]
	switch objType {
	case TSTRING:
		obj, _ = UnmarshalStringObj(metaValue)
	case THASHMETA:
		obj, _ = UnmarshalHashObj(metaValue)
	case TLISTMETA:
		obj, _ = UnmarshalListObj(metaValue)
	case TSETMETA:
		obj, _ = UnmarshalSetObj(metaValue)
	case TZSETMETA:
		obj, _ = UnmarshalZSetObj(metaValue)
	}

	return objType, obj, nil
}

func (tidis *Tidis) FlushDB(dbId uint8) error {
	dbPrefix := RawDBPrefix(tidis.TenantId(), dbId)
	startKey := dbPrefix
	endKey := kv.Key(startKey).PrefixNext()

	err := tidis.db.UnsafeDeleteRange(startKey, endKey)
	if err != nil {
		return err
	}
	return nil
}

func (tidis *Tidis) FlushAll() error {
	tenantPrefix := RawTenantPrefix(tidis.TenantId())
	startKey := tenantPrefix
	endKey := kv.Key(startKey).PrefixNext()

	err := tidis.db.UnsafeDeleteRange(startKey, endKey)
	if err != nil {
		return err
	}
	return nil
}

func (tidis *Tidis) GetCurrentVersion() (uint64, error) {
	return tidis.db.GetCurrentVersion()
}