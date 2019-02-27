//
// errors.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package terror

import "errors"

var (
	ErrCommand             error = errors.New("ERR command error")
	ErrCmdParams           error = errors.New("ERR command params error")
	ErrKeyEmpty            error = errors.New("ERR key cannot be empty")
	ErrKeyOrFieldEmpty     error = errors.New("ERR key or field cannot be empty")
	ErrTypeNotMatch        error = errors.New("ERR raw key type not match")
	ErrCmdInBatch          error = errors.New("ERR some command in batch not supported")
	ErrCmdNumber           error = errors.New("ERR command not enough in batch")
	ErrBackendType         error = errors.New("ERR backend type error")
	ErrTypeAssertion       error = errors.New("ERR interface type assertion failed")
	ErrOutOfIndex          error = errors.New("ERR index out of range")
	ErrInvalidMeta         error = errors.New("ERR invalid key meta")
	ErrUnknownType         error = errors.New("ERR unknown response data type")
	ErrRunWithTxn          error = errors.New("ERR run run with txn")
	ErrAuthNoNeed          error = errors.New("ERR Client sent AUTH, but no password is set")
	ErrAuthFailed          error = errors.New("ERR invalid password")
	ErrAuthReqired         error = errors.New("NOAUTH Authentication required.")
	ErrKeyBusy             error = errors.New("BUSYKEY key is deleting, retry later")
	ErrNotInteger          error = errors.New("ERR value is not an integer or out of range")
	ErrDiscardWithoutMulti error = errors.New("ERR DISCARD without MULTI")
	ErrExecWithoutMulti    error = errors.New("ERR EXEC without MULTI")
)
