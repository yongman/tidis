//
// command_list.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/YongMan/go/util"
	"github.com/YongMan/tedis/terror"
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
}

func lpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpush(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}
	c.rWriter.WriteInteger(int64(v))

	return nil
}

func lpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Lpop(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteBulk(v)

	return nil
}

func rpushCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpush(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}
	c.rWriter.WriteInteger(int64(v))

	return nil
}

func rpopCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Rpop(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteBulk(v)

	return nil
}

func llenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Llen(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func lindexCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	index, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}
	v, err := c.tdb.Lindex(c.args[0], index)
	if err != nil {
		return err
	}

	c.rWriter.WriteBulk(v)

	return nil
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

	v, err := c.tdb.Lrange(c.args[0], start, end)
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func lsetCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	index, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return err
	}

	err = c.tdb.Lset(c.args[0], index, c.args[2])
	if err != nil {
		return err
	}

	c.rWriter.WriteString("OK")

	return nil
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

	err = c.tdb.Ltrim(c.args[0], start, end)
	if err != nil {
		return err
	}

	c.rWriter.WriteString("OK")

	return nil
}

func ldelCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	err := c.tdb.Ldelete(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteString("OK")

	return nil
}
