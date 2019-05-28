package main

import (
	"fmt"
	"testing"
)

func TestGenerateUUIDPrefix(t *testing.T) {
	l := GenerateUUIDPrefix()
	fmt.Printf("%+v", l)
}
