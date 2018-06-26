//
// command_list.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

func init() {
	cmdRegister("lpush", lpushCommand)
	cmdRegister("lpop", lpopCommand)
	cmdRegister("rpush", rpushCommand)
	cmdRegister("rpop", rpopCommand)
	cmdRegister("llen", llenCommand)
	cmdRegister("lindex", lindexCommand)
	cmdRegister("lrange", lrangeComamnd)
	cmdRegister("lset", lsetCommand)
	cmdRegister("ltrim", ltrimCommand)
	cmdRegister("ldel", ldelCommand)
	cmdRegister("lpexpireat", lpexpireatCommand)
	cmdRegister("lpexpire", lpexpireCommand)
	cmdRegister("lexpireat", lexpireatCommand)
	cmdRegister("lexpire", lexpireCommand)
	cmdRegister("lpttl", lpttlCommand)
	cmdRegister("lttl", lttlCommand)
}

func lpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpush(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func lpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpop(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func rpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpush(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func rpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpop(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func llenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Llen(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func lindexCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	index, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}
	v, err := c.tdb.Lindex(c.GetCurrentTxn(), c.args[0], index)
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func lrangeComamnd(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	start, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	end, err := util.StrBytesToInt64(c.args[2])
	if err != nil {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lrange(c.GetCurrentTxn(), c.args[0], start, end)
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func lsetCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	index, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return err
	}

	if !c.IsTxn() {
		err = c.tdb.Lset(c.args[0], index, c.args[2])
	} else {
		err = c.tdb.LsetWithTxn(c.GetCurrentTxn(), c.args[0], index, c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func ltrimCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	start, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	end, err := util.StrBytesToInt64(c.args[2])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		err = c.tdb.Ltrim(c.args[0], start, end)
	} else {
		err = c.tdb.LtrimWithTxn(c.GetCurrentTxn(), c.args[0], start, end)
	}
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func ldelCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		v   int
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Ldelete(c.args[0], true)
	} else {
		// use sync delete for multi command
		flag := false
		v, err = c.tdb.LdelWithTxn(c.GetCurrentTxn(), c.args[0], &flag)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func lpexpireatCommand(c *Client) error {
	return pexpireatGeneric(c, tidis.TLISTMETA)
}

func lpexpireCommand(c *Client) error {
	return pexpireGeneric(c, tidis.TLISTMETA)
}

func lexpireCommand(c *Client) error {
	return expireGeneric(c, tidis.TLISTMETA)
}

func lexpireatCommand(c *Client) error {
	return expireatGeneric(c, tidis.TLISTMETA)
}

func lttlCommand(c *Client) error {
	return ttlGeneric(c, tidis.TLISTMETA)
}

func lpttlCommand(c *Client) error {
	return pttlGeneric(c, tidis.TLISTMETA)
}
