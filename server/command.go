//
// command.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

type CmdFunc func(c *Client) error

var cmds map[string]CmdFunc

func init() {
	cmds = make(map[string]CmdFunc, 50)
}

func cmdRegister(cmdName string, f CmdFunc) {
	if _, ok := cmds[cmdName]; ok {
		// cmd already exists
		return
	}
	cmds[cmdName] = f
}

func cmdFind(cmdName string) (CmdFunc, bool) {
	cmd, ok := cmds[cmdName]
	return cmd, ok
}
