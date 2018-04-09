//
// command_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

func init() {
	cmdRegister("get", getCommand)
	cmdRegister("set", setCommand)
}

func getCommand(c *Client) error {
	if len(c.args) != 1 {
		return ErrCmdParams
	}

	v, err := c.tdb.Get(c.args[0])
	if err != nil {
		return err
	}
	c.rWriter.WriteBulk(v)
	return nil
}

func setCommand(c *Client) error {
	if len(c.args) != 2 {
		return ErrCmdParams
	}

	err := c.tdb.Set(c.args[0], c.args[1])
	if err != nil {
		return err
	}
	c.rWriter.WriteString("OK")
	return nil
}
