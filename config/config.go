//
// config.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

type Config struct {
	PdAddr string
	Listen string
}

func LoadConfig() *Config {
	c := &Config{PdAddr: "10.240.200.200:2379"}
	return c
}

func NewConfig(listen, addr string) *Config {
	c := &Config{PdAddr: addr, Listen: listen}
	return c
}
