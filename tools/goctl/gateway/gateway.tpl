package main

import (
	"flag"

	"github.com/Peakchen/peakchen-go-zero/core/conf"
	"github.com/Peakchen/peakchen-go-zero/gateway"
)

var configFile = flag.String("f", "etc/gateway.yaml", "config file")

func main() {
	flag.Parse()

	var c gateway.GatewayConf
	conf.MustLoad(*configFile, &c)
	gw := gateway.MustNewServer(c)
	defer gw.Stop()
	gw.Start()
}
