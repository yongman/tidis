//
// command_server.go
// Copyright (C) 2020 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/tidis/terror"
	"strconv"
)

func init() {
	cmdRegister("flushdb", flushdbCommand)
	cmdRegister("flushall", flushallCommand)
	cmdRegister("select", selectCommand)
}

func flushdbCommand(c *Client) error {
	err := c.tdb.FlushDB(c.DBID())
	if err != nil {
		return err
	}
	return c.Resp("OK")
}

func  flushallCommand(c *Client) error {
	err := c.tdb.FlushAll()
	if err != nil {
		return err
	}
	return c.Resp("OK")
}

func selectCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}
	dbId, err := strconv.Atoi(string(c.args[0]))
	if err != nil {
		return terror.ErrCmdParams
	}
	c.SelectDB(uint8(dbId))
	return c.Resp("OK")
}