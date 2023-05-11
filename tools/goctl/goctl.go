package main

import (
	"github.com/Peakchen/go-zero/core/load"
	"github.com/Peakchen/go-zero/core/logx"
	"github.com/Peakchen/go-zero/tools/goctl/cmd"
)

func main() {
	logx.Disable()
	load.Disable()
	cmd.Execute()
}
