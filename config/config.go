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
	MaxConn               int32    `toml:"max_connection"`
	Auth                  string `toml:"auth"`
	LogLevel              string `toml:"loglevel"`
	TxnRetry              int    `toml:"txn_retry"`
	TenantId              string `toml:"tenantid"`
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
}
