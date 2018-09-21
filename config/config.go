//
// config.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

import (
	"github.com/BurntSushi/toml"
	"github.com/yongman/go/log"
)

type Config struct {
	Desc    string
	Tidis   tidisConfig   `toml:"tidis"`
	Backend backendConfig `toml:"backend"`
}

type tidisConfig struct {
	Listen                string
	MaxConn               int    `toml:"max_connection"`
	Auth                  string `toml:"auth"`
	LogLevel              string `toml:"loglevel"`
	TxnRetry              int    `toml:"txn_retry"`
	StringCheckerLoop     int    `toml:"string_checker_loop"`
	StringCheckerInterval int    `toml:"string_checker_interval"`
	ListCheckerLoop       int    `toml:"list_checker_loop"`
	ListCheckerInterval   int    `toml:"list_checker_interval"`
	HashCheckerLoop       int    `toml:"hash_checker_loop"`
	HashCheckerInterval   int    `toml:"hash_checker_interval"`
	SetCheckerLoop        int    `toml:"set_checker_loop"`
	SetCheckerInterval    int    `toml:"set_checker_interval"`
	ZsetCheckerLoop       int    `toml:"zset_checker_loop"`
	ZsetCheckerInterval   int    `toml:"zset_checker_interval"`
	TtlCheckerLazy        bool   `toml:"ttl_checker_lazy"`
}

type backendConfig struct {
	Pds string
}

func LoadConfig(path string) (*Config, error) {
	var c Config
	if _, err := toml.DecodeFile(path, &c); err != nil {
		log.Errorf("config file parse failed, %v", err)
		return nil, err
	}
	return &c, nil
}

func NewConfig(c *Config, listen, addr string, retry int, auth string) *Config {
	if c == nil {
		backend := backendConfig{
			Pds: addr,
		}
		tidis := tidisConfig{
			Listen:  listen,
			MaxConn: 0,
			Auth:    auth,
		}
		c = &Config{
			Desc:    "new config",
			Tidis:   tidis,
			Backend: backend,
		}
	} else {
		// update config load previous
		if listen != "" {
			c.Tidis.Listen = listen
		}
		if addr != "" {
			c.Backend.Pds = addr
		}
		if addr != "" {
			c.Tidis.Auth = auth
		}
		if retry != 0 {
			c.Tidis.TxnRetry = retry
		}
	}
	return c
}

// update config fields with default value if not filled
func FillWithDefaultConfig(c *Config) {
	if c.Tidis.Listen == "" {
		c.Tidis.Listen = ":5379"
	}
	if c.Tidis.StringCheckerLoop == 0 {
		c.Tidis.StringCheckerLoop = 10
	}
	if c.Tidis.StringCheckerInterval == 0 {
		c.Tidis.StringCheckerInterval = 100
	}
	if c.Tidis.HashCheckerLoop == 0 {
		c.Tidis.HashCheckerLoop = 10
	}
	if c.Tidis.HashCheckerInterval == 0 {
		c.Tidis.HashCheckerInterval = 100
	}
	if c.Tidis.SetCheckerLoop == 0 {
		c.Tidis.SetCheckerLoop = 10
	}
	if c.Tidis.SetCheckerInterval == 0 {
		c.Tidis.SetCheckerInterval = 100
	}
	if c.Tidis.ListCheckerLoop == 0 {
		c.Tidis.ListCheckerLoop = 10
	}
	if c.Tidis.ListCheckerInterval == 0 {
		c.Tidis.ListCheckerInterval = 100
	}
	if c.Tidis.ZsetCheckerLoop == 0 {
		c.Tidis.ZsetCheckerLoop = 10
	}
	if c.Tidis.ZsetCheckerInterval == 0 {
		c.Tidis.ZsetCheckerInterval = 100
	}
}
