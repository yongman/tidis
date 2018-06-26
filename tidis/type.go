//
// type.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package tidis

const (
	TSTRING byte = iota
	TLISTMETA
	TLISTDATA
	THASHMETA
	THASHDATA
	TSETMETA
	TSETDATA
	TZSETMETA
	TZSETSCORE
	TZSETDATA
	TTTLMETA
	TTTLDATA
)

const (
	FNORMAL byte = iota
	FDELETED
)

var (
	EmptyListOrSet []interface{} = make([]interface{}, 0)
)
