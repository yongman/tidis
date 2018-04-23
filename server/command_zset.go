//
// command_zset.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"github.com/yongman/go/util"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

func init() {
	cmdRegister("zadd", zaddCommand)
	cmdRegister("zcard", zcardCommand)
}

func zaddCommand(c *Client) error {
	if len(c.args) < 3 && len(c.args)%2 == 0 {
		return terror.ErrCmdParams
	}

	mps := make([]*tidis.MemberPair, 0)

	for i := 1; i < len(c.args); i += 2 {
		score, err := util.StrBytesToUint64(c.args[i])
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
