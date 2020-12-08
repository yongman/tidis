//
// command_zset.go
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
	cmdRegister("zcount", zcountCommand)
	cmdRegister("zlexcount", zlexcountCommand)
	cmdRegister("zscore", zscoreCommand)
	cmdRegister("zrem", zremCommand)
	cmdRegister("zincrby", zincrbyCommand)
	cmdRegister("zrank", zrankCommand)
	cmdRegister("zrevrank", zrevrankCommand)
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

	var (
		v   int
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Zadd(c.dbId, c.args[0], mps...)
	} else {
		v, err = c.tdb.ZaddWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], mps...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func zcardCommand(c *Client) error {
	if len(c.args) != 1 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Zcard(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
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
		withscores bool
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

	v, err := c.tdb.Zrange(c.dbId, c.GetCurrentTxn(), c.args[0], start, end, withscores, reverse)
	if err != nil {
		return err
	}

	return c.Resp(v)
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
		withscores bool
		offset     int = -1
		count      int = -1
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
		start = tidis.ScoreMin
	case "+inf":
		start = tidis.ScoreMax
	default:
		start, err = util.StrBytesToInt64(c.args[1])
		if err != nil {
			return err
		}
	}

	strScore = strings.ToLower(string(c.args[2]))
	switch strScore {
	case "-inf":
		end = tidis.ScoreMin
	case "+inf":
		end = tidis.ScoreMax
	default:
		end, err = util.StrBytesToInt64(c.args[2])
		if err != nil {
			return err
		}
	}

	v, err := c.tdb.Zrangebyscore(c.dbId, c.GetCurrentTxn(), c.args[0], start, end, withscores, offset, count, reverse)
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func zremrangebyscoreCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}

	var (
		start int64
		end   int64
		v     uint64
		err   error
	)

	// score pre-process
	strScore := strings.ToLower(string(c.args[1]))
	switch strScore {
	case "-inf":
		start = tidis.ScoreMin
	case "+inf":
		start = tidis.ScoreMax
	default:
		start, err = util.StrBytesToInt64(c.args[1])
		if err != nil {
			return err
		}
	}

	strScore = strings.ToLower(string(c.args[2]))
	switch strScore {
	case "-inf":
		end = tidis.ScoreMin
	case "+inf":
		end = tidis.ScoreMax
	default:
		end, err = util.StrBytesToInt64(c.args[2])
		if err != nil {
			return err
		}
	}

	if !c.IsTxn() {
		v, err = c.tdb.Zremrangebyscore(c.dbId, c.args[0], start, end)
	} else {
		v, err = c.tdb.ZremrangebyscoreWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], start, end)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
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

	v, err := c.tdb.Zrangebylex(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2], int(offset), int(count), reverse)
	if err != nil {
		return err
	}

	return c.Resp(v)
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

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Zremrangebylex(c.dbId, c.args[0], c.args[1], c.args[2])
	} else {
		v, err = c.tdb.ZremrangebylexWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func zcountCommand(c *Client) error {
	if len(c.args) < 3 {
		return terror.ErrCmdParams
	}
	var min, max int64
	var err error

	// score pre-process
	strScore := strings.ToLower(string(c.args[1]))
	switch strScore {
	case "-inf":
		min = tidis.ScoreMin
	case "+inf":
		min = tidis.ScoreMax
	default:
		min, err = util.StrBytesToInt64(c.args[1])
		if err != nil {
			return err
		}
	}

	strScore = strings.ToLower(string(c.args[2]))
	switch strScore {
	case "-inf":
		max = tidis.ScoreMin
	case "+inf":
		max = tidis.ScoreMax
	default:
		max, err = util.StrBytesToInt64(c.args[2])
		if err != nil {
			return err
		}
	}

	v, err := c.tdb.Zcount(c.dbId, c.GetCurrentTxn(), c.args[0], min, max)
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func zlexcountCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	v, err := c.tdb.Zlexcount(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], c.args[2])
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func zscoreCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	v, exist, err := c.tdb.Zscore(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}

	if exist {
		str := strconv.AppendInt([]byte(nil), v, 10)
		return c.Resp(str)
	} else {
		return c.Resp([]byte(nil))
	}
}

func zremCommand(c *Client) error {
	if len(c.args) < 2 {
		return terror.ErrCmdParams
	}

	var (
		v   uint64
		err error
	)

	if !c.IsTxn() {
		v, err = c.tdb.Zrem(c.dbId, c.args[0], c.args[1:]...)
	} else {
		v, err = c.tdb.ZremWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1:]...)
	}
	if err != nil {
		return err
	}

	return c.Resp(int64(v))
}

func zincrbyCommand(c *Client) error {
	if len(c.args) != 3 {
		return terror.ErrCmdParams
	}

	delta, err := util.StrBytesToInt64(c.args[1])
	if err != nil {
		return err
	}

	var v int64

	if !c.IsTxn() {
		v, err = c.tdb.Zincrby(c.dbId, c.args[0], delta, c.args[2])
	} else {
		v, err = c.tdb.ZincrbyWithTxn(c.dbId, c.GetCurrentTxn(), c.args[0], delta, c.args[2])
	}
	if err != nil {
		return err
	}

	return c.Resp(v)
}

func zrankCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	// 1. check the member exist or not
	score, exist, err := c.tdb.Zscore(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}
	if !exist {
		// not exist, just return nil
		return c.Resp([]byte(nil))
	}

	// 2. calc the rank
	v, exist, err := c.tdb.Zrank(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], score)
	if err != nil {
		return err
	}

	if exist {
		str := strconv.AppendInt([]byte(nil), v, 10)
		return c.Resp(str)
	} else {
		return c.Resp([]byte(nil))
	}
}

func zrevrankCommand(c *Client) error {
	if len(c.args) != 2 {
		return terror.ErrCmdParams
	}

	// 1. check the member exist or not
	score, exist, err := c.tdb.Zscore(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1])
	if err != nil {
		return err
	}
	if !exist {
		// not exist, just return nil
		return c.Resp([]byte(nil))
	}

	// 2. calc the zset count
	count, err := c.tdb.Zcard(c.dbId, c.GetCurrentTxn(), c.args[0])
	if err != nil {
		return err
	}

	// 3. calc the rank
	v, exist, err := c.tdb.Zrank(c.dbId, c.GetCurrentTxn(), c.args[0], c.args[1], score)
	if err != nil {
		return err
	}

	if exist {
		r := int64(count) - 1 - v
		str := strconv.AppendInt([]byte(nil), r, 10)
		return c.Resp(str)
	} else {
		return c.Resp([]byte(nil))
	}
}
