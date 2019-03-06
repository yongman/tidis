//
// command_scripting.go
// Copyright (C) 2019 Negash <i@negash.ru>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/yongman/tidis/terror"
	"github.com/yuin/gopher-lua"
	"strconv"
	"strings"
)

func init() {
	cmdRegister("eval", evalCommand)
}

// TODO
//  simulate connect to tidis
//  need understand tidis api
var redisClient = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379", // tidis addr (for container can use localhost)
	Password: "",               // no password set
	DB:       0,                // use default DB
})

const luaRedisTypeName = "redis"

// Registers my redis type to given L.
func registerRedisType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaRedisTypeName)
	L.SetGlobal("redis", mt)
	// static attributes
	L.SetField(mt, "call", L.NewFunction(newRedisCall))
}

func parseLuaValue(data lua.LValue) interface{} {
	Type := data.Type().String()
	if Type == "string" {
		if lv, ok := data.(lua.LString); ok {
			return string(lv)
		}
	} else if Type == "number" {
		if intv, ok := data.(lua.LNumber); ok {
			return int64(intv)
		}
	} else if Type == "boolean" {
		if lv, ok := data.(lua.LBool); ok {
			return bool(lv)
		}
	} else if Type == "table" {
		var rest []interface{}
		a := data.(*lua.LTable)
		a.ForEach(func(value lua.LValue, value2 lua.LValue) {
			rest = append(rest, parseLuaValue(value2))
		})
		return rest
	}
	return string(data.(lua.LString))
}

// Constructor
func newRedisCall(L *lua.LState) int {
	var rest []interface{}
	// filter data from lua to redis command
	for i := L.GetTop(); i >= 1; i-- {
		lv := L.Get(i)
		rest = append([]interface{}{parseLuaValue(lv)}, rest...)
	}
	// redis call command
	result, err := redisClient.Do(rest...).Result()
	if err != nil {
		println(err)
	}
	// return integer
	in, ok := result.(int64)
	if ok {
		L.Push(lua.LNumber(in))
		return 1
	}
	//return only on string
	s, ok := result.(string)
	if ok {
		L.Push(lua.LString(s))
		return 1
	}
	//return array of string
	array, ok := result.([]interface{})
	if ok {
		for _, key := range array {
			L.Push(lua.LString(key.(string)))
		}
		return len(array)
	}
	return 0
}

func evalCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		err     error
		keysLen int
	)
	keysLen, err = strconv.Atoi(string(c.args[1]))
	if err != nil {
		return terror.ErrCmdParams
	}
	keysLen = keysLen + 2
	// check len KEYS more than all c.args without lua-script(c.args[0]) and keysLen(c.args[1])
	if keysLen > len(c.args) {
		return terror.ErrCmdParams
	}

	luaScript := fmt.Sprintf(string(c.args[0][:]))
	// replace KEYS for lua
	keysArray := make([][]byte, len(c.args[2:keysLen]))
	if keysLen > 2 {
		for key := range keysArray {
			luaScript = strings.Replace(luaScript, fmt.Sprintf("KEYS[%d]", key+1), fmt.Sprintf("'%s'", string(c.args[key+2])), 1)
			keysArray[key] = c.args[key+2]
		}
	}

	// replace ARGVs for lua
	argsArray := make([][]byte, len(c.args[keysLen:]))
	for key := range argsArray {
		luaScript = strings.Replace(luaScript, fmt.Sprintf("ARGV[%d]", key+1), fmt.Sprintf("%s", string(c.args[key+keysLen])), 1)
		argsArray[key] = c.args[key+keysLen]
	}
	L := lua.NewState()
	defer L.Close()
	registerRedisType(L)
	err = L.DoString(luaScript)
	if err != nil {
		return err
	}
	//if eval result empty
	if L.GetTop() == 0 {
		return c.Resp(nil)
	}
	data := L.Get(-1)
	return c.Resp(parseLuaValue(data))

}
