//
// command_zset.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"strings"

	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

func init() {
	cmdRegister("zadd", zaddCommand)
	cmdRegister("zcard", zcardCommand)
	cmdRegister("zrange", zrangeCommand)
	cmdRegister("zrevrange", zrevrangeCommand)
	cmdRegister("zrangebyscore", zrangebyscoreCommand)
	cmdRegister("zrevrangebyscore", zrevrangebyscoreCommand)
	cmdRegister("zremrangebyscore", zremrangebyscoreCommand)
	cmdRegister("zrangebylex", zrangebylexCommand)
	cmdRegister("zrevrangebylex", zrevrangebylexCommand)
	cmdRegister("zremrangebylex", zremrangebylexCommand)
}

func zaddCommand(c *Client) error {
	if len(c.args) < 3 && len(c.args)%2 == 0 {
		return terror.ErrCmdParams
	}

	mps := make([]*tidis.MemberPair, 0)

	for i := 1; i < len(c.args); i += 2 {
		score, err := util.StrBytesToInt64(c.args[i])
		if err != nil {
			return err
		}
		mp := &tidis.MemberPair{
			Score:  score,
			Member: c.args[i+1],
		}
		mps = append(mps, mp)
	}

	v, err := c.tdb.Zadd(c.args[0], mps...)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func zcardCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Zcard(c.args[0])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func zrangeCommand(c *Client) error {
	return zrangeGeneric(c, false)
}

func zrevrangeCommand(c *Client) error {
	return zrangeGeneric(c, true)
}

func zrangeGeneric(c *Client, reverse bool) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}
	var (
		start, end int64
		err        error
		withscores bool = false
	)
	if len(c.args) == 4 {
		str := strings.ToLower(string(c.args[3]))
		if str == "withscores" {
			withscores = true
		} else {
			return terror.ErrCmdParams
		}
	}

	start, err = util.StrBytesToInt64(c.args[1])
	if err != nil {
		return err
	}
	end, err = util.StrBytesToInt64(c.args[2])
	if err != nil {
		return err
	}

	v, err := c.tdb.Zrange(c.args[0], start, end, withscores, reverse)
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func zrangebyscoreCommand(c *Client) error {
	return zrangebyscoreGeneric(c, false)
}

func zrevrangebyscoreCommand(c *Client) error {
	return zrangebyscoreGeneric(c, true)
}

func zrangebyscoreGeneric(c *Client, reverse bool) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	var (
		start, end int64
		err        error
		withscores bool = false
		offset     int  = -1
		count      int  = -1
	)

	for i := 3; i < len(c.args); i++ {
		str := strings.ToLower(string(c.args[i]))
		if str == "withscores" {
			withscores = true
		} else if str == "limit" {
			if len(c.args) <= i+2 {
				return terror.ErrCmdParams
			}
			of, err := util.StrBytesToInt64(c.args[i+1])
			if err != nil {
				return err
			}
			offset = int(of)

			co, err := util.StrBytesToInt64(c.args[i+2])
			if err != nil {
				return err
			}
			count = int(co)
			break
		}
	}

	// score pre-process
	strScore := strings.ToLower(string(c.args[1]))
	switch strScore {
	case "-inf":
		start = tidis.SCORE_MIN
	case "+inf":
		start = tidis.SCORE_MAX
	default:
		start, err = util.StrBytesToInt64(c.args[1])
		if err != nil {
			return err
		}
	}

	strScore = strings.ToLower(string(c.args[2]))
	switch strScore {
	case "-inf":
		end = tidis.SCORE_MIN
	case "+inf":
		end = tidis.SCORE_MAX
	default:
		end, err = util.StrBytesToInt64(c.args[2])
		if err != nil {
			return err
		}
	}

	v, err := c.tdb.Zrangebyscore(c.args[0], start, end, withscores, offset, count, reverse)
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func zremrangebyscoreCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	var start, end int64
	var err error

	// score pre-process
	strScore := strings.ToLower(string(c.args[1]))
	switch strScore {
	case "-inf":
		start = tidis.SCORE_MIN
	case "+inf":
		start = tidis.SCORE_MAX
	default:
		start, err = util.StrBytesToInt64(c.args[1])
		if err != nil {
			return err
		}
	}

	strScore = strings.ToLower(string(c.args[2]))
	switch strScore {
	case "-inf":
		end = tidis.SCORE_MIN
	case "+inf":
		end = tidis.SCORE_MAX
	default:
		end, err = util.StrBytesToInt64(c.args[2])
		if err != nil {
			return err
		}
	}
	v, err := c.tdb.Zremrangebyscore(c.args[0], start, end)
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}

func zrangebylexGeneric(c *Client, reverse bool) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	var offset, count int64 = 0, -1
	var err error

	if len(c.args) > 3 {
		if len(c.args) != 6 {
			return terror.ErrCmdParams
		}
		if strings.ToLower(string(c.args[3])) != "limit" {
			return terror.ErrCmdParams
		}
		offset, err = util.StrBytesToInt64(c.args[4])
		if err != nil {
			return err
		}
		count, err = util.StrBytesToInt64(c.args[5])
		if err != nil {
			return err
		}
		if offset < 0 || count < 0 {
			return terror.ErrCmdParams
		}
	}

	v, err := c.tdb.Zrangebylex(c.args[0], c.args[1], c.args[2], int(offset), int(count), reverse)
	if err != nil {
		return err
	}

	c.rWriter.WriteArray(v)

	return nil
}

func zrangebylexCommand(c *Client) error {
	return zrangebylexGeneric(c, false)
}

func zrevrangebylexCommand(c *Client) error {
	return zrangebylexGeneric(c, true)
}

func zremrangebylexCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Zremrangebylex(c.args[0], c.args[1], c.args[2])
	if err != nil {
		return err
	}

	c.rWriter.WriteInteger(int64(v))

	return nil
}
