//
// config.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

type Config struct {
	PdAddr   string
	Listen   string
	TxnRetry int
	Auth     string
}

func LoadConfig() *Config {
	c := &Config{PdAddr: "127.0.0.1:2379"}
	return c
}

func NewConfig(listen, addr string, retry int, auth string) *Config {
	c := &Config{
		PdAddr:   addr,
		Listen:   listen,
		TxnRetry: retry,
		Auth:     auth,
	}
	return c
}
