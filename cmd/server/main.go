//
// main.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/YongMan/go/log"

	"github.com/YongMan/tedis/config"
	"github.com/YongMan/tedis/server"
)

func main() {
	log.SetLevel(log.INFO)
	log.Info("server started")
	conf := config.LoadConfig()

	app := server.NewApp(conf)

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go app.Run()

	<-quitCh
}
