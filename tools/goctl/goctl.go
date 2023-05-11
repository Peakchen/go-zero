package main

import (
	"github.com/Peakchen/peakchen-go-zero/core/load"
	"github.com/Peakchen/peakchen-go-zero/core/logx"
	"github.com/Peakchen/peakchen-go-zero/tools/goctl/cmd"
)

func main() {
	logx.Disable()
	load.Disable()
	cmd.Execute()
}
