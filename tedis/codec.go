//
// codec.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tedis

import "github.com/YongMan/tedis/terror"

// encoder and decoder for key of data

// string
// type(1)|key
func SEncoder(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	buf[0] = TSTRING

	copy(buf[1:], key)
	return buf
}

func SDecoder(rawkey []byte) ([]byte, error) {
	t := rawkey[0]
	if t != TSTRING {
		return nil, terror.ErrTypeNotMatch
	}
	return rawkey[1:], nil
}
