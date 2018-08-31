//
// main.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/yongman/go/log"
	"github.com/yongman/tidis/config"
	"github.com/yongman/tidis/server"
)

var (
	listen   string
	backend  string
	txnRetry int
	conf     string
	loglevel string
	auth     string
)

func init() {
	flag.StringVar(&listen, "listen", ":5379", "server listen address")
	flag.StringVar(&backend, "backend", "", "tikv storage backend address")
	flag.IntVar(&txnRetry, "retry", 5, "transaction retry time when commit failed")
	flag.StringVar(&conf, "conf", "", "config file")
	flag.StringVar(&loglevel, "loglevel", "info", "loglevel output, format:info/debug/warn")
	flag.StringVar(&auth, "auth", "", "connection authentication")
}

func setLogLevel(loglevel string) {
	switch loglevel {
	case "info":
		log.SetLevel(log.INFO)
	case "debug":
		log.SetLevel(log.DEBUG)
	case "warn":
		log.SetLevel(log.WARN)
	default:
		log.SetLevel(log.INFO)
	}
}

func main() {
	flag.Parse()

	log.Info("server started")

	var (
		c   *config.Config
		err error
	)

	if conf != "" {
		c, err = config.LoadConfig(conf)
		if err != nil {
			return
		}
	} else {
		if c == nil && backend == "" {
			log.Fatal("backend argument must be assign")
		}
	}
	c = config.NewConfig(c, listen, backend, txnRetry, auth)

	config.FillWithDefaultConfig(c)

	setLogLevel(c.Tidis.LogLevel)

	app := server.NewApp(c)

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go app.Run()

	<-quitCh
}
