//
// command_hash.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/tidis/terror"
)

func init() {
	cmdRegister("hget", hgetCommand)
	cmdRegister("hstrlen", hstrlenCommand)
	cmdRegister("hexists", hexistsCommand)
	cmdRegister("hlen", hlenCommand)
	cmdRegister("hmget", hmgetCommand)
	cmdRegister("hdel", hdelCommand)
	cmdRegister("hset", hsetCommand)
	cmdRegister("hsetnx", hsetnxCommand)
	cmdRegister("hmset", hmsetCommand)
	cmdRegister("hkeys", hkeysCommand)
	cmdRegister("hvals", hvalsCommand)
	cmdRegister("hgetall", hgetallCommand)
}

func hgetCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hget(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hstrlenCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hstrlen(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hexistsCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hexists(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	if v {
		err = c.Resp(int64(1))
	} else {
		err = c.Resp(int64(0))
	}

	return err
}

func hlenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hlen(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hmgetCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hmget(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hdelCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Hdel(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.HdelWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hsetCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	var (
		v   uint8
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Hset(c.dbId, c.args[0], c.args[1], c.args[2])
	} else {
		v, err = c.tdb.HsetWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hsetnxCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	var (
		v   uint8
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Hsetnx(c.dbId, c.args[0], c.args[1], c.args[2])
	} else {
		v, err = c.tdb.HsetnxWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hmsetCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	var err error

	if !c.IsTxn() {
		err = c.tdb.Hmset(c.dbId, c.args[0], c.args[1:]...)
	} else {
		err = c.tdb.HmsetWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func hkeysCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hkeys(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hvalsCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hvals(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hgetallCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hgetall(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}
