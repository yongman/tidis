//
// command_list.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import "github.com/YongMan/tedis/terror"

func init() {
	cmdRegister("lpush", lpushCommand)
	cmdRegister("lpop", lpopCommand)
	cmdRegister("rpush", rpushCommand)
	cmdRegister("rpop", rpopCommand)
	cmdRegister("llen", llenCommand)
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
