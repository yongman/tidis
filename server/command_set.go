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
	var (
		v   uint64
		err error
	)
	if !c.IsTxn() {
		v, err = c.tdb.Sadd(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SaddWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func scardCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Scard(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sismemberCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sismember(c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func smembersCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Smembers(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func sremCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)
	if !c.IsTxn() {
		v, err = c.tdb.Srem(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SremWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sdiffCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sdiff(c.GetCurrentTxn(), c.args...)
	if err != nil {
		return err
	}

	return c.Resp1(v)
}

func sunionCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sunion(c.GetCurrentTxn(), c.args...)
	if err != nil {
		return err
	}

	return c.Resp1(v)
}

func sinterCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sinter(c.GetCurrentTxn(), c.args...)
	if err != nil {
		return err
	}

	return c.Resp1(v)
}
func sdiffstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Sdiffstore(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SdiffstoreWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sinterstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Sinterstore(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SinterstoreWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sunionstoreCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Sunionstore(c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SunionstoreWithTxn(c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sclearCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	flag := true
	if !c.IsTxn() {
		v, err = c.tdb.Sclear(true, c.args...)
	} else {
		v, err = c.tdb.SclearWithTxn(&flag, true, c.GetCurrentTxn(), c.args...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
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
