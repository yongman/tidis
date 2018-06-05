//
// config_test.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package config

import (
	"fmt"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	conf, err := LoadConfig("../config.toml")
	fmt.Println(*conf, err)
	if err == nil {
		FillWithDefaultConfig(conf)
		fmt.Println(*conf)
	}
}
