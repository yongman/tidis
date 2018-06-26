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

	quitCh chan bool

	clientWG sync.WaitGroup

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
	// run ttl checker
	ttlStringChecker := tidis.NewTTLChecker(tidis.TSTRING,
		app.conf.Tidis.StringCheckerLoop,
		app.conf.Tidis.StringCheckerInterval,
		app.GetTidis())
	go ttlStringChecker.Run()

	ttlHashChecker := tidis.NewTTLChecker(tidis.THASHMETA,
		app.conf.Tidis.HashCheckerLoop,
		app.conf.Tidis.HashCheckerInterval,
		app.GetTidis())
	go ttlHashChecker.Run()

	ttlListChecker := tidis.NewTTLChecker(tidis.TLISTMETA,
		app.conf.Tidis.ListCheckerLoop,
		app.conf.Tidis.ListCheckerInterval,
		app.GetTidis())
	go ttlListChecker.Run()

	ttlSetChecker := tidis.NewTTLChecker(tidis.TSETMETA,
		app.conf.Tidis.SetCheckerLoop,
		app.conf.Tidis.SetCheckerInterval,
		app.GetTidis())
	go ttlSetChecker.Run()

	ttlZsetChecker := tidis.NewTTLChecker(tidis.TZSETMETA,
		app.conf.Tidis.ZsetCheckerLoop,
		app.conf.Tidis.ZsetCheckerInterval,
		app.GetTidis())
	go ttlZsetChecker.Run()

	go app.tdb.RunAsync()

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
			// handle conn
			log.Debug("handle new connection")
			ClientHandler(conn, app)
		}
	}
}
