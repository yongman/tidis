//
// command_string.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"strconv"
	"strings"

	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

func init() {
	cmdRegister("get", getCommand)
	cmdRegister("getbit", getBitCommand)
	cmdRegister("set", setCommand)
	cmdRegister("setbit", setBitCommand)
	cmdRegister("bitcount", bitCountCommand)
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

func getBitCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	} else if c.args[1][0] == '-' {
		return terror.ErrCmdParams
	}

	var (
		v        []byte
		vRet     byte
		err      error
		bitsCnt  int
		bitPos   int
		bytesCnt int
	)

	bitPos, err = strconv.Atoi(string(c.args[1]))
	if err != nil {
		return terror.ErrCmdParams
	}
	bitsCnt = bitPos + 1

	if bitsCnt%8 == 0 {
		bytesCnt = bitsCnt / 8
	} else {
		bytesCnt = (bitsCnt / 8) + 1
	}

	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	} else if v == nil {
		// the key is not exist yet, return zero.
		vRet = 0
	} else {
		// get the key, then change its value
		if bitsCnt <= len(v)*8 {
			// if get bit pos is less than or equal to it's length.
			// get bit operation
			vRet = (v[bytesCnt-1] >> (uint)(bitPos%8)) & 1
		} else {
			// if get bit pos is bigger than it's length, return zero.
			vRet = 0
		}
	}

	return c.Resp(int64(vRet))
}

func bitCountCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	var (
		i          int
		v          []byte
		x          uint8
		err        error
		bitsOneCnt int
		bytesCnt   int
	)

	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	} else if v == nil {
		// the key is not exist yet, return zero.
		bitsOneCnt = 0
	} else {
		// get the key, then calculate the one value bits count
		bitsOneCnt = 0
		bytesCnt = len(v)
		for i = 0; i < bytesCnt; i++ {
			x = v[i]
			for ; x > 0; bitsOneCnt++ {
				x &= x - 1
			}
		}
	}

	return c.Resp(int64(bitsOneCnt))
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
	if len(c.args) < 2 || len(c.args) > 5 {
		return terror.ErrCmdParams
	}
	//SET key value
	if len(c.args) == 2 {
		err := c.tdb.Set(c.GetCurrentTxn(), c.args[0], c.args[1])
		if err != nil {
			return err
		}
	}

	if len(c.args) >= 3 {
		ttlMs := int64(0)
		nxFlag := false
		xxFlag := false
		ttlFlag := false
		var err error

		i := 2
		for i < len(c.args) {
			commandItem := strings.ToLower(string(c.args[i]))
			if commandItem == "nx" {
				nxFlag = true
			} else if commandItem == "xx" {
				xxFlag = true
			} else if commandItem == "ex" {
				//get px param
				if ttlFlag == true {
					return terror.ErrCmdParams
				}

				i++
				if i < len(c.args) {
					ttlMs, err = util.StrBytesToInt64(c.args[i])
					if err != nil {
						return terror.ErrCmdParams
					}
					ttlMs *= 1000
					ttlFlag = true
				} else {
					return terror.ErrCmdParams
				}
			} else if commandItem == "px" {
				//get px param
				if ttlFlag == true {
					return terror.ErrCmdParams
				}
				i++
				if i < len(c.args) {
					ttlMs, err = util.StrBytesToInt64(c.args[i])
					if err != nil {
						return terror.ErrCmdParams
					}
				} else {
					return terror.ErrCmdParams
				}
			}
			i++
		}

		//Can not set nx and xx at sametime
		if nxFlag == true && xxFlag == true {
			return terror.ErrCmdParams
		}

		var result bool
		result, err = c.tdb.SetWithParam(c.GetCurrentTxn(), c.args[0], c.args[1], ttlMs, nxFlag, xxFlag)
		if err != nil {
			return err
		}

		if result == false {
			return c.Resp(nil)
		}
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
		i        int
		v        []byte
		err      error
		bitsCnt  int
		bitPos   int
		bytesCnt int
	)

	bitPos, err = strconv.Atoi(string(c.args[1]))
	if err != nil || (bitPos+1) > 1*1024*1024*8 {
		return terror.ErrCmdParams
	}
	bitsCnt = bitPos + 1

	// offset starts with 0, we need to +1 to do calculation
	if bitsCnt%8 == 0 {
		bytesCnt = bitsCnt / 8
	} else {
		bytesCnt = (bitsCnt / 8) + 1
	}

	v, err = c.tdb.Get(c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	} else if v == nil {
		// the key is not exist yet, we should create it.
		v = make([]byte, bytesCnt)

		// init all the bits with zero
		for i := 0; i < bytesCnt; i++ {
			v[i] = 0
		}

		// set bit 0,1 operation
		if c.args[2][0] == '0' {
			v[bytesCnt-1] &= ^(1 << (uint)(bitPos%8))
		} else if c.args[2][0] == '1' {
			v[bytesCnt-1] |= (1 << (uint)(bitPos%8))
		}
	} else {
		// get the key, then change its value
		if bitsCnt <= len(v)*8 {
			// if set bit pos is less than or equal to it's length, just set it
			// set bit 0,1 operation
			if c.args[2][0] == '0' {
				v[bytesCnt-1] &= ^(1 << (uint)(bitPos%8))
			} else if c.args[2][0] == '1' {
				v[bytesCnt-1] |= (1 << (uint)(bitPos%8))
			}
		} else {
			// if set bit pos is bigger than it's length, append it and then chagne it
			j := bytesCnt - len(v)
			for i = 0; i < j; i++ {
				v = append(v, 0)
			}
			// set bit 0,1 operation
			if c.args[2][0] == '0' {
				v[bytesCnt-1] &= ^(1 << (uint)(bitPos%8))
			} else if c.args[2][0] == '1' {
				v[bytesCnt-1] |= (1 << (uint)(bitPos%8))
			}
		}
	}

	err = c.tdb.Set(c.GetCurrentTxn(), c.args[0], v)
	if err != nil {
		return err
	}
	if c.args[2][0] == '0' {
		return c.Resp(int64(1))
	}
	return c.Resp(int64(0))
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
