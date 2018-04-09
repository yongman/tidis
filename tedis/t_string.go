//
// t_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

func (tedis *Tedis) Get(key []byte) ([]byte, error) {
	//sample key value
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}

	v, err := tedis.db.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (tedis *Tedis) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	err := tedis.db.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}
