//
// errors.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package terror

import "errors"

var (
	ErrCommand      error = errors.New("command error")
	ErrCmdParams    error = errors.New("command params error")
	ErrKeyEmpty     error = errors.New("key cannot be empty")
	ErrTypeNotMatch error = errors.New("raw key type not match")
)
