//
// config.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

type Config struct {
	PdAddr []string
}

func LoadConfig() *Config {
	c := &Config{PdAddr: []string{"tikv://10.240.200.200:2379/pd?cluster=1"}}
	//c := &Config{PdAddr: []string{"tikv://192.168.100.214:3379/pd?cluster=1"}}

	return c
}
