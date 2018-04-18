//
// errors.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package terror

import "errors"

var (
	ErrCommand         error = errors.New("ERR command error")
	ErrCmdParams       error = errors.New("ERR command params error")
	ErrKeyEmpty        error = errors.New("ERR key cannot be empty")
	ErrKeyOrFieldEmpty error = errors.New("ERR key or field cannot be empty")
	ErrTypeNotMatch    error = errors.New("ERR raw key type not match")
	ErrCmdInBatch      error = errors.New("ERR some command in batch not supported")
	ErrCmdNumber       error = errors.New("ERR command not enough in batch")
	ErrBackendType     error = errors.New("ERR backend type error")
	ErrTypeAssertion   error = errors.New("ERR interface type assertion failed")
	ErrOutOfIndex      error = errors.New("ERR index out of range")
	ErrInvalidMeta     error = errors.New("ERR invalid key meta")
)
