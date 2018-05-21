//
// command_set.go
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
	cmdRegister("sadd", saddCommand)
	cmdRegister("scard", scardCommand)
	cmdRegister("sismember", sismemberCommand)
	cmdRegister("smembers", smembersCommand)
	cmdRegister("srem", sremCommand)
	cmdRegister("sdiff", sdiffCommand)
	cmdRegister("sunion", sunionCommand)
	cmdRegister("sinter", sinterCommand)
	cmdRegister("sdiffstore", sdiffstoreCommand)
	cmdRegister("sunionstore", sunionstoreCommand)
	cmdRegister("sinterstore", sinterstoreCommand)
	cmdRegister("sclear", sclearCommand)
	cmdRegister("spexpireat", spexpireatCommand)
	cmdRegister("spexpire", spexpireCommand)
	cmdRegister("sexpireat", sexpireatCommand)
	cmdRegister("sexpire", sexpireCommand)
	cmdRegister("spttl", spttlCommand)
	cmdRegister("sttl", sttlCommand)
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

func sremCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Srem(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func sdiffCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sdiff(c.args...)
	if err != nil {
		return err
	}

	c.rWriter.WriteStr2BytesArray(v)

	return nil
}

func sunionCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sunion(c.args...)
	if err != nil {
		return err
	}

	c.rWriter.WriteStr2BytesArray(v)

	return nil
}

func sinterCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sinter(c.args...)
	if err != nil {
		return err
	}

	c.rWriter.WriteStr2BytesArray(v)

	return nil
}
func sdiffstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sdiffstore(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func sinterstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sinterstore(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func sunionstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sunionstore(c.args[0], c.args[1:]...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func sclearCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sclear(c.args...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func spexpireatCommand(c *Client) error {
	return pexpireatGeneric(c, tidis.TSETMETA)
}

func spexpireCommand(c *Client) error {
	return pexpireGeneric(c, tidis.TSETMETA)
}

func sexpireCommand(c *Client) error {
	return expireGeneric(c, tidis.TSETMETA)
}

func sexpireatCommand(c *Client) error {
	return expireatGeneric(c, tidis.TSETMETA)
}

func sttlCommand(c *Client) error {
	return ttlGeneric(c, tidis.TSETMETA)
}

func spttlCommand(c *Client) error {
	return pttlGeneric(c, tidis.TSETMETA)
}
