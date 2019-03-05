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
			rest = append(rest, []interface{}{parseLuaValue(value2)})
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
	L := lua.NewState()
	defer L.Close()
	registerRedisType(L)
	// TODO need to parse keys and args -> c.args[0], c.args[1], c.args[2:]...
	err := L.DoString(fmt.Sprintf(string(c.args[0][:])))
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
