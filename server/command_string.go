//
// command_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
	"fmt"
	"strconv"
)

func init() {
	cmdRegister("get", getCommand)
	cmdRegister("set", setCommand)
	cmdRegister("setbit", setBitCommand)
	cmdRegister("setex", setexCommand)
	cmdRegister("del", delCommand)
	cmdRegister("mget", mgetCommand)
	cmdRegister("mset", msetCommand)
	cmdRegister("incr", incrCommand)
	cmdRegister("incrby", incrbyCommand)
	cmdRegister("decr", decrCommand)
	cmdRegister("decrby", decrbyCommand)
	cmdRegister("strlen", strlenCommand)
	cmdRegister("pexpire", pexpireCommand)
	cmdRegister("pexpireat", pexpireatCommand)
	cmdRegister("expire", expireCommand)
	cmdRegister("expireat", expireatCommand)
	cmdRegister("pttl", pttlCommand)
	cmdRegister("ttl", ttlCommand)
}

func getCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}
	var (
		v   []byte
		err error
	)
	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func mgetCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	var (
		ret []interface{}
		err error
	)

	ret, err = c.tdb.MGet(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func setCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	err := c.tdb.Set(c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func setBitCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	} else if c.args[1][0] == '-' {
		return terror.ErrCmdParams
	} else if (len(c.args[2]) != 1) || (c.args[2][0] != '0' && c.args[2][0] != '1') {
		return terror.ErrCmdParams
	}
	
        var (
		i int
                v   []byte
		v_new []byte
                err error
		bitsCnt int
		bitPos int
		bytesCnt int
        )

	bitPos,err = strconv.Atoi(string(c.args[1]))
	bitsCnt = bitPos+1
	if err != nil {
		return terror.ErrCmdParams
	}

	// offset starts with 0, we need to +1 to do calculation
	if bitsCnt % 8 == 0 {
		bytesCnt = bitsCnt / 8
	} else {
		bytesCnt = (bitsCnt / 8) + 1
	}

        v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
        if err != nil {
		// error
		return err
        } else if v == nil {
		// the key is not exist yet, we should create it.
		v = make([]byte, bytesCnt)

		// init all the bits with zero
		for i:=0;i<bytesCnt;i++ {
			v[i] = 0
		}

		// set bit 0,1 operation
		if c.args[2][0] == '0' {
			v[bytesCnt-1]&=^(1<<(uint)(bitPos % 8))
		} else if c.args[2][0] == '1' {
			v[bytesCnt-1]|=(1<<(uint)(bitPos % 8))
		}
		
		fmt.Println("bytesCnt is: %d", bytesCnt)
	} else {
		// get the key, then change its value
		if bitsCnt <= len(v)*8  {
		/// if set bit pos is less than or equal to it's length, just set it
			// set bit 0,1 operation
			if c.args[2][0] == '0' {
				v[bytesCnt-1]&=^(1<<(uint)(bitPos % 8))
			} else if c.args[2][0] == '1' {
				v[bytesCnt-1]|=(1<<(uint)(bitPos % 8))
			}
		} else {
		/// if set bit pos is bigger than it's length, append it and then chagne it
			v_new = make([]byte, bytesCnt)
			for i=0;i<bytesCnt-1;i++ {
				v_new[i] = v[i]
			}
			for i=bytesCnt-1;i<bytesCnt;i++ {
				v_new[i] = 0
			}
			// set bit 0,1 operation
                        if c.args[2][0] == '0' {
                                v_new[bytesCnt-1]&=^(1<<(uint)(bitPos % 8))
                        } else if c.args[2][0] == '1' {
                                v_new[bytesCnt-1]|=(1<<(uint)(bitPos % 8))
                        }
			v = v_new

		}
	}

	err = c.tdb.Set(c.GetCurrentTxn(), c.args[0], v)
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func setexCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	var (
		err error
		sec int64
	)

	sec, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		err = c.tdb.Setex(c.args[0], sec, c.args[2])
	} else {
		err = c.tdb.SetexWithTxn(c.GetCurrentTxn(), c.args[0], sec, c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func msetCommand(c *Client) error {
	if len(c.args) < 2 && len(c.args)%2 != 0 {
		return terror.ErrCmdParams
	}

	_, err := c.tdb.MSet(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp("OK")
}

func delCommand(c *Client) error {
	if len(c.args) < 1 {
		return terror.ErrCmdParams
	}

	ret, err := c.tdb.Delete(c.GetCurrentTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp(int64(ret))
}

func incrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		ret int64
		err error
	)

	if !c.IsTxn() {
		ret, err = c.tdb.Incr(c.args[0], 1)
	} else {
		ret, err = c.tdb.IncrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func incrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		step int64
		err  error
		ret  int64
	)

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		ret, err = c.tdb.Incr(c.args[0], step)
	} else {
		ret, err = c.tdb.IncrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func decrCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		ret int64
		err error
	)

	if !c.IsTxn() {
		ret, err = c.tdb.Decr(c.args[0], 1)
	} else {
		ret, err = c.tdb.DecrWithTxn(c.GetCurrentTxn(), c.args[0], 1)
	}

	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func decrbyCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	var (
		step int64
		err  error
		ret  int64
	)

	step, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return terror.ErrCmdParams
	}

	if !c.IsTxn() {
		ret, err = c.tdb.Decr(c.args[0], step)
	} else {
		ret, err = c.tdb.DecrWithTxn(c.GetCurrentTxn(), c.args[0], step)
	}
	if err != nil {
		return err
	}

	return c.Resp(ret)
}

func strlenCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		v   []byte
		err error
	)

	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(len(v)))
}

func pexpireCommand(c *Client) error {
	return pexpireGeneric(c, tidis.TSTRING)
}

func pexpireatCommand(c *Client) error {
	return pexpireatGeneric(c, tidis.TSTRING)
}

func expireCommand(c *Client) error {
	return expireGeneric(c, tidis.TSTRING)
}

func expireatCommand(c *Client) error {
	return expireatGeneric(c, tidis.TSTRING)
}

func pttlCommand(c *Client) error {
	return pttlGeneric(c, tidis.TSTRING)
}

func ttlCommand(c *Client) error {
	return ttlGeneric(c, tidis.TSTRING)
}
