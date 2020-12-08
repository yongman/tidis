//
// command_set.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/tidis/terror"
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
		v, err = c.tdb.Sadd(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SaddWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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

	v, err := c.tdb.Scard(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func sismemberCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sismember(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func smembersCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Smembers(c.dbId, c.GetCurrentTxn(), c.args[0])
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
		v, err = c.tdb.Srem(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SremWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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

	v, err := c.tdb.Sdiff(c.dbId, c.GetCurrentTxn(), c.args...)
	if err != nil {
		return err
	}

	return c.Resp1(v)
}

func sunionCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sunion(c.dbId, c.GetCurrentTxn(), c.args...)
	if err != nil {
		return err
	}

	return c.Resp1(v)
}

func sinterCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Sinter(c.dbId, c.GetCurrentTxn(), c.args...)
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
		v, err = c.tdb.Sdiffstore(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SdiffstoreWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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
		v, err = c.tdb.Sinterstore(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SinterstoreWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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
		v, err = c.tdb.Sunionstore(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.SunionstoreWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
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

	if !c.IsTxn() {
		v, err = c.tdb.Sclear(c.dbId, c.args...)
	} else {
		v, err = c.tdb.SclearWithTxn(c.dbId, c.GetCurrentTxn(), c.args...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}
