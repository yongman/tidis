//
// command_hash.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
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
	cmdRegister("hclear", hclearCommand)
	cmdRegister("hpexpire", hpexpireCommand)
	cmdRegister("hpexpireat", hpexpireatCommand)
	cmdRegister("hexpire", hexpireCommand)
	cmdRegister("hexpireat", hexpireatCommand)
	cmdRegister("hpttl", hpttlCommand)
	cmdRegister("httl", httlCommand)
}

func hgetCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hget(c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hstrlenCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hstrlen(c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hexistsCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hexists(c.GetCurrentTxn(), c.args[0], c.args[1])
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

	v, err := c.tdb.Hlen(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hmgetCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hmget(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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
		v, err = c.tdb.Hdel(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.HdelWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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
		v, err = c.tdb.Hset(c.args[0], c.args[1], c.args[2])
	} else {
		v, err = c.tdb.HsetWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
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
		v, err = c.tdb.Hsetnx(c.args[0], c.args[1], c.args[2])
	} else {
		v, err = c.tdb.HsetnxWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
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
		err = c.tdb.Hmset(c.args[0], c.args[1:]...)
	} else {
		err = c.tdb.HmsetWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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

	v, err := c.tdb.Hkeys(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hvalsCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hvals(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hgetallCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hgetall(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func hclearCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		v   uint8
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Hclear(c.args[0], true)
	} else {
		flag := true
		v, err = c.tdb.HclearWithTxn(c.GetCurrentTxn(), c.args[0], &flag)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func hpexpireCommand(c *Client) error {
	return pexpireGeneric(c, tidis.THASHMETA)
}

func hpexpireatCommand(c *Client) error {
	return pexpireatGeneric(c, tidis.THASHMETA)
}

func hexpireCommand(c *Client) error {
	return expireGeneric(c, tidis.THASHMETA)
}

func hexpireatCommand(c *Client) error {
	return expireatGeneric(c, tidis.THASHMETA)
}

func hpttlCommand(c *Client) error {
	return pttlGeneric(c, tidis.THASHMETA)
}

func httlCommand(c *Client) error {
	return ttlGeneric(c, tidis.THASHMETA)
}
