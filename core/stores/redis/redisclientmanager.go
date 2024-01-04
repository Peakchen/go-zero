package redis

import (
	"crypto/tls"
	"io"

	red "github.com/go-redis/redis/v8"
	"github.com/Peakchen/go-zero/core/syncx"
)

const (
	defaultDatabase = 0
	maxRetries      = 3
	idleConns       = 8
)

var clientManager = syncx.NewResourceManager()

func getClient(r *Redis) (*red.Client, error) {
	val, err := clientManager.GetResource(r.Addr, func() (io.Closer, error) {
		var tlsConfig *tls.Config
		if r.tls {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		store := red.NewClient(&red.Options{
			Addr:         r.Addr,
			Password:     r.Pass,
			DB:           r.DB,
			MaxRetries:   r.MaxRetries,
			MinIdleConns: r.MinIdleConns,
			TLSConfig:    tlsConfig,
		})
		store.AddHook(durationHook)
		for _, hook := range r.hooks {
			store.AddHook(hook)
		}
		return store, nil
	})
	if err != nil {
		return nil, err
	}
	r.red = val.(*red.Client)
	return val.(*red.Client), nil
}
