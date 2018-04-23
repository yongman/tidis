//
// number.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package util

import (
	"encoding/binary"
	"errors"
	"strconv"
)

// number convert utils
var (
	ErrParams = errors.New("params error")
)

func BytesToInt64(n []byte) (int64, error) {
	if n == nil || len(n) < 8 {
		return 0, ErrParams
	}

	return int64(binary.BigEndian.Uint64(n)), nil
}

func BytesToUint16(n []byte) (uint16, error) {
	if n == nil || len(n) < 2 {
		return 0, ErrParams
	}

	return binary.BigEndian.Uint16(n), nil
}

func BytesToUint64(n []byte) (uint64, error) {
	if n == nil || len(n) < 8 {
		return 0, ErrParams
	}

	return binary.BigEndian.Uint64(n), nil
}

func Int64ToBytes(n int64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))

	return b, nil
}

func Uint16ToBytes(n uint16) ([]byte, error) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)

	return b, nil
}

func Uint16ToBytes1(dst []byte, n uint16) error {
	if len(dst) < 2 {
		return ErrParams
	}
	binary.BigEndian.PutUint16(dst, n)

	return nil
}

func Uint64ToBytes(n uint64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)

	return b, nil
}

func Uint64ToBytes1(dst []byte, n uint64) error {
	if len(dst) < 8 {
		return ErrParams
	}
	binary.BigEndian.PutUint64(dst, n)
	return nil
}

func StrBytesToInt64(n []byte) (int64, error) {
	if n == nil {
		return 0, ErrParams
	}
	return strconv.ParseInt(string(n), 10, 64)
}

func StrToInt64(n string) (int64, error) {
	return strconv.ParseInt(n, 10, 64)
}

func StrBytesToUint64(n []byte) (uint64, error) {
	if n == nil {
		return 0, ErrParams
	}
	return strconv.ParseUint(string(n), 10, 64)
}

func StrToUint64(n string) (uint64, error) {
	return strconv.ParseUint(n, 10, 64)
}

func Int64ToStrBytes(n int64) ([]byte, error) {
	return strconv.AppendInt(nil, n, 10), nil
}
