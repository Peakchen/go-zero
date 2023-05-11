package upgrade

import "github.com/Peakchen/peakchen-go-zero/tools/goctl/internal/cobrax"

// Cmd describes an upgrade command.
var Cmd = cobrax.NewCommand("upgrade", cobrax.WithRunE(upgrade))
