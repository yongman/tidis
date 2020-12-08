package utils

import "time"

func Now() uint64 {
	return uint64(time.Now().UnixNano() / 1000 / 1000)
}
