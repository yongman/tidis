//
// command_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/YongMan/go/util"
	"github.com/YongMan/tedis/tedis"
	"github.com/YongMan/tedis/terror"
)

func init() {
	cmdRegister("get", getCommand)
	cmdRegister("set", setCommand)
	cmdRegister("del", delCommand)
	cmdRegister("mget", mgetCommand)
	cmdRegister("mset", msetCommand)
	cmdRegister("incr", incrCommand)
	cmdRegister("incrby", incrbyCommand)
	cmdRegister("decr", decrCommand)
	cmdRegister("decrby", decrbyCommand)
	cmdRegister("strlen", strlenCommand)
}

func getCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Get(c.args[0])
	if err != nil {
		return err
	}
	c.rWriter.WriteBulk(v)
	return nil
}

func mgetCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.MGet(c.args)
	if err != nil {
		return err
	}

	var resp []interface{}

	for _, key := range c.args {
		ekey := tedis.SEncoder(key)
		if v, ok := ret[string(ekey)]; ok {
			resp = append(resp, v)
		} else {
			resp = append(resp, nil)
		}
	}
	c.rWriter.WriteArray(resp)

	return nil
}

func setCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	err := c.tdb.Set(c.args[0], c.args[1])
	if err != nil {
		return err
	}
	c.rWriter.WriteString("OK")

	return nil
}

func msetCommand(c *Client) error {
	if len(c.args) < 2 && len(c.args)%2 != 0 {
		return terror.ErrCmdParams
	}

	kv := make(map[string][]byte, len(c.args))

	for i := 0; i < len(c.args)-1; i += 2 {
		k, v := string(tedis.SEncoder(c.args[i])), c.args[i+1]
		kv[k] = v
	}

	_, err := c.tdb.MSet(kv)
	if err != nil {
		return err
	}
	c.rWriter.WriteString("OK")

	return nil
}

func delCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.Delete(c.args)
	if err != nil {
		return err
	}
	c.rWriter.WriteInteger(int64(ret))

	return nil
}

func incrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.Incr(c.args[0], 1)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(ret)

	return nil
}

func incrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var step int64
	var err error

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}
	ret, err := c.tdb.Incr(c.args[0], step)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(ret)

	return nil
}

func decrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.Decr(c.args[0], 1)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(ret)

	return nil
}

func decrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var step int64
	var err error

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}
	ret, err := c.tdb.Decr(c.args[0], step)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(ret)

	return nil
}

func strlenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Get(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(len(v)))

	return nil
}
