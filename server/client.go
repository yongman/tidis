//
// client.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"time"

	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/yongman/go/goredis"
	"github.com/yongman/go/log"
	"github.com/yongman/tidis/terror"
	"github.com/yongman/tidis/tidis"
)

type Command struct {
	cmd  string
	args [][]byte
}

type Client struct {
	app *App

	tdb *tidis.Tidis

	// request is processing
	cmd  string
	args [][]byte

	// transaction block
	isTxn   bool
	cmds    []Command
	txn     kv.Transaction
	respTxn []interface{}

	// connection authentation
	isAuthed bool

	buf bytes.Buffer

	conn net.Conn

	rReader *goredis.RespReader
	rWriter *goredis.RespWriter
}

func newClient(app *App) *Client {
	authed := false
	if app.auth == "" {
		authed = true
	}

	client := &Client{
		app:      app,
		tdb:      app.tdb,
		isAuthed: authed,
	}
	return client
}

func ClientHandler(conn net.Conn, app *App) {
	c := newClient(app)

	c.conn = conn
	// connection buffer setting

	br := bufio.NewReader(conn)
	c.rReader = goredis.NewRespReader(br)

	bw := bufio.NewWriter(conn)
	c.rWriter = goredis.NewRespWriter(bw)

	app.clientWG.Add(1)

	go c.connHandler()
}

// for multi transaction commands
func (c *Client) NewTxn() error {
	var ok bool
	txn, err := c.tdb.NewTxn()
	c.txn, ok = txn.(kv.Transaction)
	if !ok {
		return terror.ErrBackendType
	}
	return err
}

func (c *Client) GetCurrentTxn() kv.Transaction {
	if c.isTxn {
		return c.txn
	}
	return nil
}

func (c *Client) addResp(resp interface{}) {
	c.respTxn = append(c.respTxn, resp)
}

func (c *Client) CommitTxn() error {
	return c.txn.Commit(context.Background())
}

func (c *Client) RollbackTxn() error {
	return c.txn.Rollback()
}

func (c *Client) IsTxn() bool {
	return c.isTxn
}

func (c *Client) Resp(resp interface{}) error {
	var err error

	if c.isTxn {
		c.addResp(resp)
	} else {
		switch v := resp.(type) {
		case []interface{}:
			err = c.rWriter.WriteArray(v)
		case []byte:
			err = c.rWriter.WriteBulk(v)
		case nil:
			err = c.rWriter.WriteBulk(nil)
		case int64:
			err = c.rWriter.WriteInteger(v)
		case string:
			err = c.rWriter.WriteString(v)
		case error:
			err = c.rWriter.WriteError(v)
		default:
			err = terror.ErrUnknownType
		}
	}

	return err
}

func (c *Client) FlushResp(resp interface{}) error {
	err := c.Resp(resp)
	if err != nil {
		return err
	}
	return c.rWriter.Flush()
}

// treat string as bulk array
func (c *Client) Resp1(resp interface{}) error {
	var err error

	if c.isTxn {
		c.addResp(resp)
	} else {
		switch v := resp.(type) {
		case []interface{}:
			err = c.rWriter.WriteArray(v)
		case []byte:
			err = c.rWriter.WriteBulk(v)
		case nil:
			err = c.rWriter.WriteBulk(nil)
		case int64:
			err = c.rWriter.WriteInteger(v)
		case string:
			err = c.rWriter.WriteBulk([]byte(v))
		case error:
			err = c.rWriter.WriteError(v)
		default:
			err = terror.ErrUnknownType
		}
	}

	return err
}
func (c *Client) connHandler() {

	defer func(c *Client) {
		c.conn.Close()
		c.app.clientWG.Done()
	}(c)

	select {
	case <-c.app.quitCh:
		return
	default:
		break
	}

	for {
		c.cmd = ""
		c.args = nil

		req, err := c.rReader.ParseRequest()
		if err != nil && err != io.EOF {
			log.Error(err.Error())
			return
		} else if err != nil {
			return
		}
		err = c.handleRequest(req)
		if err != nil && err != io.EOF {
			log.Error(err.Error())
			return
		}
	}
}

func (c *Client) resetTxnStatus() {
	c.isTxn = false
	c.cmds = []Command{}
	c.respTxn = []interface{}{}
}

func (c *Client) handleRequest(req [][]byte) error {
	if len(req) == 0 {
		c.cmd = ""
		c.args = nil
	} else {
		c.cmd = strings.ToLower(string(req[0]))
		c.args = req[1:]
	}

	// auth check
	if c.cmd != "auth" {
		if !c.isAuthed {
			c.FlushResp(terror.ErrAuthReqired)
			return nil
		}
	}

	var err error

	log.Debugf("command: %s argc:%d", c.cmd, len(c.args))
	switch c.cmd {
	case "multi":
		// mark connection as transactional
		log.Debugf("client in transaction")
		c.isTxn = true
		c.cmds = []Command{}
		c.respTxn = []interface{}{}

		c.rWriter.FlushString("OK")
		return nil
	case "exec":
		err = c.NewTxn()
		if err != nil {
			c.resetTxnStatus()
			c.rWriter.FlushBulk(nil)
			return nil
		}

		// execute transactional commands in txn
		// execute commands
		log.Debugf("command length:%d txn:%v", len(c.cmds), c.isTxn)
		if len(c.cmds) == 0 || !c.isTxn {
			c.rWriter.FlushBulk(nil)
			c.resetTxnStatus()
			return nil
		}

		for _, cmd := range c.cmds {
			log.Debugf("execute command: %s", cmd.cmd)
			// set cmd and args processing
			c.cmd = cmd.cmd
			c.args = cmd.args
			if err = c.execute(); err != nil {
				break
			}
		}
		if err != nil {
			c.RollbackTxn()
			c.rWriter.FlushBulk(nil)
		} else {
			err = c.CommitTxn()
			if err == nil {
				c.rWriter.FlushArray(c.respTxn)
			} else {
				c.rWriter.FlushBulk(nil)
			}
		}

		c.resetTxnStatus()
		return nil

	case "discard":
		// discard transactional commands
		err = c.RollbackTxn()
		c.rWriter.FlushString("OK")
		c.resetTxnStatus()

		return err

	case "auth":
		// auth connection
		if len(c.args) != 1 {
			c.FlushResp(terror.ErrCmdParams)
		}
		if c.app.auth == "" {
			c.FlushResp(terror.ErrAuthNoNeed)
		} else if string(c.args[0]) != c.app.auth {
			c.isAuthed = false
			c.FlushResp(terror.ErrAuthFailed)
		} else {
			c.isAuthed = true
			c.FlushResp("OK")
		}
		return nil

	case "ping":
		if len(c.args) != 0 {
			c.FlushResp(terror.ErrCmdParams)
		}
		c.FlushResp("PONG")

		return nil
	}

	if c.isTxn {
		command := Command{cmd: c.cmd, args: c.args}
		c.cmds = append(c.cmds, command)
		log.Debugf("command:%s added to transaction queue, queue size:%d", c.cmd, len(c.cmds))
		c.rWriter.FlushString("QUEUED")
	} else {
		c.execute()
	}
	return nil
}

func (c *Client) execute() error {
	var err error

	start := time.Now()

	if len(c.cmd) == 0 {
		err = terror.ErrCommand
	} else if f, ok := cmdFind(c.cmd); !ok {
		err = terror.ErrCommand
	} else {
		err = f(c)
	}
	// TODO
	if err != nil && !c.isTxn {
		c.rWriter.FlushError(err)
	}

	c.rWriter.Flush()

	log.Debugf("command time cost %d", time.Now().Sub(start).Nanoseconds())
	return err
}
