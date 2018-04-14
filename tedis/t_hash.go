//
// t_hash.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

func (tedis *Tedis) Hget(key, field []byte) ([]byte, error) {
	if len(key) == 0 || len(field) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eDataKey := HDataEncoder(key, field)
	v, err := tedis.db.Get(eDataKey)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (tedis *Tedis) Hstrlen(key, field []byte) (int, error) {
	v, err := tedis.Hget(key, field)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (tedis *Tedis) Hexists(key, field []byte) (bool, error) {
	v, err := tedis.Hget(key, field)
	if err != nil {
		return false, err
	}

	if v == nil || len(v) == 0 {
		return true, nil
	}

	return false, nil
}

func (tedis *Tedis) Hlen(key []byte) (uint64, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	eMetaKey := HMetaEncoder(key)
	v, err := tedis.db.Get(eMetaKey)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	hsize, err := util.BytesToUint64(v)
	if err != nil {
		return 0, err
	}
	return hsize, nil
}

func (tedis *Tedis) Hmget(key []byte, fields ...[]byte) ([][]byte, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, terror.ErrKeyOrFieldEmpty
	}

	batchKeys := make([][]byte, len(fields))
	for i, field := range fields {
		batchKeys[i] = HDataEncoder(key, field)
	}
	retMap, err := tedis.db.BatchGet(batchKeys)
	if err != nil {
		return nil, err
	}

	// convert map to slice
	ret := make([][]byte, len(fields))
	for i, ek := range batchKeys {
		v, ok := retMap[string(ek)]
		if !ok {
			ret[i] = nil
		} else {
			ret[i] = v
		}
	}
	return ret, nil
}

func (tedis *Tedis) Hdel(key []byte, fields ...[]byte) (uint64, error) {
	if len(key) == 0 || len(fields) == 0 {
		return nil, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaDecoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		ss := txn.GetSnapshot()
		hsizeRaw, err := tedis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			return nil, nil
		}

		var delCnt uint64 = 0

		for i, field := range fields {
			eDataKey := HDataEncoder(key, field)
			v, err := tedis.db.GetWithSnapshot(eDataKey, ss)
			if err != nil {
				return nil, err
			}
			if v != nil {
				delCnt++
				err = txn.Delete(eDataKey)
				if err != nil {
					return nil, err
				}
			}
		}

		hsize, err := util.BytesToUint64(hsizeRaw)
		if err != nil {
			return nil, err
		}

		hsize = hsize - delCnt
		if hsize > 0 {
			// update meta size
			eMetaValue := make([]byte, 8)
			err = txn.Set(eMetaKey, util.Uint64ToBytes(eMetaValue, hsize))
			if err != nil {
				return nil, err
			}
		} else {
			// delete entire user hash key
			err = txn.Delete(eMetaKey)
			if err != nil {
				return nil, err
			}
		}

		return delCnt, nil
	}

	// execute txn
	ret, err := tedis.db.BatchInTxn(f)
	if err != nil {
		return 0, err
	}

	return delCnt, nil
}

func (tedis *Tedis) Hset(key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var ret uint8
		var exists bool = false
		var hsize uint64

		ss := txn.GetSnapshot()

		hsizeRaw, err := tedis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			// create a new meta key
			hsize = 0
		} else {
			hsize = util.BytesToUint64(hsizeRaw)
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tedis.db.GetWithSnapshot(eDataKey, ss)
		if err != nil {
			return nil, err
		}

		if v != nil {
			ret = 0
		} else {
			// new insert field, add hsize
			ret = 1
			hsize++

			// update meta key
			eMetaData := make([]byte, 8)
			err = txn.Set(eMetaKey, util.Uint64ToBytes(eMetaData, hsize))
			if err != nil {
				return nil, err
			}
		}

		// set or update field
		err = txn.Set(eDataKey)
		if err != nil {
			return nil, err
		}

		return ret, nil
	}

	// execute txn
	ret, err := tedis.db.BatchInTnx(f)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (tedis *Tedis) Hsetnx(key, field, value []byte) (uint8, error) {
	if len(key) == 0 || len(field) == 0 || len(value) == 0 {
		return 0, terror.ErrKeyOrFieldEmpty
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var exists bool = false
		var hsize uint64

		ss := txn.GetSnapshot()

		hsizeRaw, err := tedis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			// create a new meta key
			hsize = 0
		} else {
			hsize = util.BytesToUint64(hsizeRaw)
		}

		eDataKey := HDataEncoder(key, field)
		v, err := tedis.db.GetWithSnapshot(eDataKey, ss)
		if err != nil {
			return nil, err
		}

		if v != nil {
			// field already exists, no perform update
			return 0, nil
		}

		// new insert field, add hsize
		hsize++

		// update meta key
		eMetaData := make([]byte, 8)
		err = txn.Set(eMetaKey, util.Uint64ToBytes(eMetaData, hsize))
		if err != nil {
			return nil, err
		}

		// set or update field
		err = txn.Set(eDataKey)
		if err != nil {
			return nil, err
		}

		return 1, nil
	}

	// execute txn
	ret, err := tedis.db.BatchInTnx(f)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (tedis *Tedis) Hmset(key []byte, fieldsvalues ...[]byte) error {
	if len(key) == 0 || len(fieldsvalues)%2 != 0 {
		return nil, terror.ErrParams
	}

	eMetaKey := HMetaEncoder(key)

	// txn function
	f := func(txn1 interface{}) (interface{}, error) {
		txn, ok := txn1.(kv.Transaction)
		if !ok {
			return nil, terror.ErrBackendType
		}

		var hsize uint64

		ss := txn.GetSnapshot()

		hsizeRaw, err := tedis.db.GetWithSnapshot(eMetaKey, ss)
		if err != nil {
			return nil, err
		}
		if hsizeRaw == nil {
			hsize = 0
		} else {
			hsize = util.BytesToUint64(hsizeRaw)
		}

		// multi get set
		for i := 0; i < len(fieldsvalues)-1; i = i + 2 {
			field, value := fieldsvalues[i], fieldsvalue[i+1]

			// check field already exists, update hsize
			eDataKey := HDataEncoder(key, field, value)
			v, err := tedis.db.GetWithSnapshot(eDataKey, ss)
			if err != nil {
				return nil, err
			}

			if v == nil {
				// field not exists, hsize should incr
				hsize++
			}

			// update field
			err = txn.Set(eDataKey, value)
			if err != nil {
				return nil, err
			}
		}

		// update meta
		eMetaData := make([]byte, 8)

		err = txn.Set(eMetaKey, util.Uint64ToBytes(eMetaData, hsize))
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	// execute txn
	_, err := tedis.db.BatchInTxn(f)
	if err != nil {
		return err
	}

	return nil
}
