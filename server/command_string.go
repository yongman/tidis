//
// command_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
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
	cmdRegister("pexpire", pexpireCommand)
	cmdRegister("pexpireat", pexpireatCommand)
	cmdRegister("expire", expireCommand)
	cmdRegister("expireat", expireatCommand)
	cmdRegister("pttl", pttlCommand)
	cmdRegister("ttl", ttlCommand)
}

func getCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}
	var (
		v   []byte
		err error
	)
	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func mgetCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	var (
		ret []interface{}
		err error
	)

	ret, err = c.tdb.MGet(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func setCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	err := c.tdb.Set(c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func msetCommand(c *Client) error {
	if len(c.args) < 2 && len(c.args)%2 != 0 {
		return terror.ErrCmdParams
	}

	_, err := c.tdb.MSet(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func delCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.Delete(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp(int64(ret))
}

func incrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		ret int64
		err error
	)

	if !c.IsTxn() {
		ret, err = c.tdb.Incr(c.args[0], 1)
	} else {
		ret, err = c.tdb.IncrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func incrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		step int64
		err  error
		ret  int64
	)

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		ret, err = c.tdb.Incr(c.args[0], step)
	} else {
		ret, err = c.tdb.IncrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func decrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		ret int64
		err error
	)

	if !c.IsTxn() {
		ret, err = c.tdb.Decr(c.args[0], 1)
	} else {
		ret, err = c.tdb.DecrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}

	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func decrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		step int64
		err  error
		ret  int64
	)

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		ret, err = c.tdb.Decr(c.args[0], step)
	} else {
		ret, err = c.tdb.DecrWithTxn(c.GetCurrentTxn(), c.args[0], step)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func strlenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		v   []byte
		err error
	)

	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(len(v)))
}

func pexpireCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		v   int
		err error
	)
	i, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		v, err = c.tdb.PExpire(c.args[0], i)
	} else {
		v, err = c.tdb.PExpireWithTxn(c.GetCurrentTxn(), c.args[0], i)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func pexpireatCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		v   int
		err error
	)
	i, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		v, err = c.tdb.PExpireAt(c.args[0], i)
	} else {
		v, err = c.tdb.PExpireAtWithTxn(c.GetCurrentTxn(), c.args[0], i)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func expireCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		v   int
		err error
	)
	i, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		v, err = c.tdb.Expire(c.args[0], i)
	} else {
		v, err = c.tdb.ExpireWithTxn(c.GetCurrentTxn(), c.args[0], i)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func expireatCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		v   int
		err error
	)

	i, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		v, err = c.tdb.ExpireAt(c.args[0], i)
	} else {
		v, err = c.tdb.ExpireAtWithTxn(c.GetCurrentTxn(), c.args[0], i)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func pttlCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.PTtl(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	c.Resp(v)

	return nil
}

func ttlCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Ttl(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	c.Resp(v)

	return nil
}
