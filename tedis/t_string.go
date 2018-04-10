//
// t_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

import "github.com/YongMan/tedis/terror"

func (tedis *Tedis) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	key = SEncoder(key)

	v, err := tedis.db.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (tedis *Tedis) MGet(keys [][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	m, err := tedis.db.MGet(nkeys)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (tedis *Tedis) Set(key, value []byte) error {
	if len(key) == 0 {
		return terror.ErrKeyEmpty
	}

	key = SEncoder(key)
	err := tedis.db.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (tedis *Tedis) MSet(kv map[string][]byte) (int, error) {
	return 0, nil
}

func (tedis *Tedis) Delete(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, terror.ErrKeyEmpty
	}

	nkeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		nkeys[i] = SEncoder(keys[i])
	}

	ret, err := tedis.db.Delete(nkeys)
	if err != nil {
		return 0, err
	}
	return ret, nil
}
