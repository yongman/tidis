//
// command_set.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import "github.com/yongman/tidis/terror"

func init() {
	cmdRegister("sadd", saddCommand)
	cmdRegister("scard", scardCommand)
	cmdRegister("sismember", sismemberCommand)
	cmdRegister("smembers", smembersCommand)
}

func saddCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sadd(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func scardCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Scard(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func sismemberCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sismember(c.args[0], c.args[1])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func smembersCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Smembers(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}
