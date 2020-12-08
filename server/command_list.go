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
}

func lpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpush(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func lpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpop(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func rpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpush(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func rpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpop(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func llenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Llen(c.dbId, c.GetCurrentTxn(), c.args[0])
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
	v, err := c.tdb.Lindex(c.dbId, c.GetCurrentTxn(), c.args[0], index)
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

	v, err := c.tdb.Lrange(c.dbId, c.GetCurrentTxn(), c.args[0], start, end)
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
		err = c.tdb.Lset(c.dbId, c.args[0], index, c.args[2])
	} else {
		err = c.tdb.LsetWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], index, c.args[2])
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
		err = c.tdb.Ltrim(c.dbId, c.args[0], start, end)
	} else {
		err = c.tdb.LtrimWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], start, end)
	}
	if err != nil {
		return err
	}

	return c.Resp("OK")
}
