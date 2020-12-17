//
// app.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package server

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/yongman/go/log"
	"github.com/yongman/tidis/config"
	"github.com/yongman/tidis/tidis"
)

type App struct {
	conf *config.Config

	listener net.Listener

	// wrapper and manager for db instance
	tdb *tidis.Tidis

	// connection authentication
	auth string

	tenanId string

	quitCh chan bool

	clientWG sync.WaitGroup

	clientCount int32

	//client map?
}

// initialize an app
func NewApp(conf *config.Config) *App {
	var err error
	app := &App{
		conf: conf,
		auth: conf.Tidis.Auth,
	}

	app.tdb, err = tidis.NewTidis(conf)
	if err != nil {
		log.Fatal(err.Error())
	}

	app.listener, err = net.Listen("tcp", conf.Tidis.Listen)
	log.Infof("server listen in %s", conf.Tidis.Listen)
	if err != nil {
		log.Fatal(err.Error())
	}

	return app
}

func (app *App) GetTidis() *tidis.Tidis {
	return app.tdb
}

func (app *App) Close() error {
	return nil
}

func (app *App) Run() {
	go app.tdb.RunAsync()
	var currentClients int32

	// accept connections
	for {
		select {
		case <-app.quitCh:
			return
		default:
			// accept new client connect and perform
			log.Debug("waiting for new connection")
			conn, err := app.listener.Accept()
			if err != nil {
				log.Error(err.Error())
				continue
			}
			currentClients = atomic.LoadInt32(&app.clientCount)
			if app.conf.Tidis.MaxConn > 0 && currentClients > app.conf.Tidis.MaxConn {
				log.Warnf("too many client connections, max client connections:%d, now:%d, reject it.", app.conf.Tidis.MaxConn, currentClients)
				conn.Close()
				continue
			}
			// handle conn
			log.Debug("handle new connection")
			ClientHandler(conn, app)
		}
	}
}
