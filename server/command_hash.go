//
// command_hash.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import "github.com/yongman/tidis/terror"

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

	v, err := c.tdb.Hget(c.args[0], c.args[1])
	if err != nil {
		return err
	}

	c.rWriter.WriteBulk(v)

	return nil
}

func hstrlenCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hstrlen(c.args[0], c.args[1])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func hexistsCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hexists(c.args[0], c.args[1])
	if err != nil {
		return err
	}

	if v {
		c.rWriter.WriteInteger(1)
	} else {
		c.rWriter.WriteInteger(0)
	}

	return nil
}

func hlenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hlen(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func hmgetCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hmget(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func hdelCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hdel(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func hsetCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hset(c.args[0], c.args[1], c.args[2])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func hsetnxCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hsetnx(c.args[0], c.args[1], c.args[2])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func hmsetCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	err := c.tdb.Hmset(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteString("OK")

	return nil
}

func hkeysCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hkeys(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func hvalsCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hvals(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func hgetallCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Hgetall(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}
