package main

import (
	"fmt"
	"os"
)

func GetOSEnvStr(key string) string {
	v := os.Getenv(key)
	fmt.Printf("Env %s:%s\n", key, v)

	return v
}
