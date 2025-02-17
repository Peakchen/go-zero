package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Peakchen/go-zero/core/errorx"
	"github.com/Peakchen/go-zero/core/hash"
	"github.com/Peakchen/go-zero/core/stores/redis"
	"github.com/Peakchen/go-zero/core/syncx"
)

type (
	// Cache interface is used to define the cache implementation.
	Cache interface {
		Exists(key string) (bool, error)
		ExistsCtx(ctx context.Context, key string) (bool, error)
		Hexists(key string, field string) (bool, error)
		HexistsCtx(ctx context.Context, key string, field string) (bool, error)
		// Del deletes cached values with keys.
		Del(keys ...string) error
		// DelCtx deletes cached values with keys.
		DelCtx(ctx context.Context, keys ...string) error
		// Delx deletes cached values with key and fields.
		Delx(key string, fields ...string) error
		// DelxCtx deletes cached values with key and fields.
		DelxCtx(ctx context.Context, key string, fields ...string) error
		// Get gets the cache with key and fills into v.
		Get(key string, val any) error
		// GetCtx gets the cache with key and fills into v.
		GetCtx(ctx context.Context, key string, val any) error
		// IsNotFound checks if the given error is the defined errNotFound.
		IsNotFound(err error) bool
		// Set sets the cache with key and v, using c.expiry.
		Set(key string, val any) error
		// SetCtx sets the cache with key and v, using c.expiry.
		SetCtx(ctx context.Context, key string, val any) error
		// Set sets the cache with key and v, using c.expiry.
		HSet(key string, fields string, val any) error
		// SetCtx sets the cache with key and v, using c.expiry.
		HsetCtx(ctx context.Context, key string, fields string, val any) error
		// Get gets the cache with key and fills into v.
		HGet(key string, fields string, val any) error
		// GetCtx gets the cache with key and fills into v.
		HGetCtx(ctx context.Context, key string, fields string, val any) error
		// SetWithExpire sets the cache with key and v, using given expire.
		SetWithExpire(key string, val any, expire time.Duration) error
		// SetWithExpireCtx sets the cache with key and v, using given expire.
		SetWithExpireCtx(ctx context.Context, key string, val any, expire time.Duration) error
		// Take takes the result from cache first, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		Take(val any, key string, query func(val any) error) error
		// Takex takes the result from cache first, key and field, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		Takex(val any, key, field string, query func(val any) error) error
		// TakeCtx takes the result from cache first, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		TakeCtx(ctx context.Context, val any, key string, query func(val any) error) error
		// TakexCtx takes the result from cache first, key and field, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		TakexCtx(ctx context.Context, val any, key, field string, query func(val any) error) error
		// TakeWithExpire takes the result from cache first, if not found,
		// query from DB and set cache using given expire, then return the result.
		TakeWithExpire(val any, key string, query func(val any, expire time.Duration) error) error
		// TakeWithExpireCtx takes the result from cache first, if not found,
		// query from DB and set cache using given expire, then return the result.
		TakeWithExpireCtx(ctx context.Context, val any, key string,
			query func(val any, expire time.Duration) error) error
		// TakeWithExpire2 takes the result from cache first, if not found,
		// query from DB and set cache using given expire, then return the result.
		TakeWithExpire2(val any, key string, field string, query func(val any) error) error
		// TakeWithExpire2Ctx takes the result from cache first, if not found,
		// query from DB and set cache using given expire, then return the result.
		TakeWithExpire2Ctx(ctx context.Context, val any, key string, field string, query func(val any) error) error
		// TakeAllOne takes the result from cache first, key and fields, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		TakeAllOne(val any, key string, query func(val any, leftFields ...string) error) error

		// TakeAllOneCtx takes the result from cache first by key and struct tag fields, if not found,
		// query from DB and set cache using c.expiry, then return the result.
		TakeAllOneCtx(ctx context.Context, val any, key string,
			query func(val any, leftFields ...string) error) error

		// Expire is the implementation of redis expire command.
		Expire(key string, expire time.Duration) error
		// ExpireCtx is the implementation of redis expire command.
		ExpireCtx(ctx context.Context, key string, expire time.Duration) error
	}

	cacheCluster struct {
		dispatcher  *hash.ConsistentHash
		errNotFound error
	}
)

// New returns a Cache.
func New(c ClusterConf, barrier syncx.SingleFlight, st *Stat, errNotFound error,
	opts ...Option) Cache {
	if len(c) == 0 || TotalWeights(c) <= 0 {
		log.Fatal("no cache nodes")
	}

	if len(c) == 1 {
		return NewNode(redis.MustNewRedis(c[0].RedisConf), barrier, st, errNotFound, opts...)
	}

	dispatcher := hash.NewConsistentHash()
	for _, node := range c {
		cn := NewNode(redis.MustNewRedis(node.RedisConf), barrier, st, errNotFound, opts...)
		dispatcher.AddWithWeight(cn, node.Weight)
	}

	return cacheCluster{
		dispatcher:  dispatcher,
		errNotFound: errNotFound,
	}
}

// Del deletes cached values with keys.
func (cc cacheCluster) Del(keys ...string) error {
	return cc.DelCtx(context.Background(), keys...)
}

// Delx deletes cached values with key and fields.
func (cc cacheCluster) Delx(key string, fields ...string) error {
	return cc.DelxCtx(context.Background(), key, fields...)
}

// DelxCtx deletes cached values with key and fields.
func (cc cacheCluster) DelxCtx(ctx context.Context, key string, fields ...string) error {
	switch len(fields) {
	case 0:
		return nil
	case 1:
		field := fields[0]
		c, ok := cc.dispatcher.Get(field)
		if !ok {
			return cc.errNotFound
		}

		return c.(Cache).DelxCtx(ctx, key, field)
	default:
		var be errorx.BatchError
		nodes := make(map[any][]string)
		for _, field := range fields {
			c, ok := cc.dispatcher.Get(field)
			if !ok {
				be.Add(fmt.Errorf("key %q not found", field))
				continue
			}

			nodes[c] = append(nodes[c], field)
		}
		for c, fs := range nodes {
			if err := c.(Cache).DelxCtx(ctx, key, fs...); err != nil {
				be.Add(err)
			}
		}

		return be.Err()
	}
}

// DelCtx deletes cached values with keys.
func (cc cacheCluster) DelCtx(ctx context.Context, keys ...string) error {
	switch len(keys) {
	case 0:
		return nil
	case 1:
		key := keys[0]
		c, ok := cc.dispatcher.Get(key)
		if !ok {
			return cc.errNotFound
		}

		return c.(Cache).DelCtx(ctx, key)
	default:
		var be errorx.BatchError
		nodes := make(map[any][]string)
		for _, key := range keys {
			c, ok := cc.dispatcher.Get(key)
			if !ok {
				be.Add(fmt.Errorf("key %q not found", key))
				continue
			}

			nodes[c] = append(nodes[c], key)
		}
		for c, ks := range nodes {
			if err := c.(Cache).DelCtx(ctx, ks...); err != nil {
				be.Add(err)
			}
		}

		return be.Err()
	}
}

// Get gets the cache with key and fills into v.
func (cc cacheCluster) Get(key string, val any) error {
	return cc.GetCtx(context.Background(), key, val)
}

// GetCtx gets the cache with key and fills into v.
func (cc cacheCluster) GetCtx(ctx context.Context, key string, val any) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).GetCtx(ctx, key, val)
}

// IsNotFound checks if the given error is the defined errNotFound.
func (cc cacheCluster) IsNotFound(err error) bool {
	return errors.Is(err, cc.errNotFound)
}

// Set sets the cache with key and v, using c.expiry.
func (cc cacheCluster) Set(key string, val any) error {
	return cc.SetCtx(context.Background(), key, val)
}

// SetCtx sets the cache with key and v, using c.expiry.
func (cc cacheCluster) SetCtx(ctx context.Context, key string, val any) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).SetCtx(ctx, key, val)
}

// Get gets the cache with key and fills into v.
func (cc cacheCluster) HGet(key string, field string, val any) error {
	return cc.HGetCtx(context.Background(), key, field, val)
}

// GetCtx gets the cache with key and fills into v.
func (cc cacheCluster) HGetCtx(ctx context.Context, key string, field string, val any) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).HGetCtx(ctx, key, field, val)
}

// Set sets the cache with key and v, using c.expiry.
func (cc cacheCluster) HSet(key string, field string, val any) error {
	return cc.HsetCtx(context.Background(), key, field, val)
}

// SetCtx sets the cache with key and v, using c.expiry.
func (cc cacheCluster) HsetCtx(ctx context.Context, key string, field string, val any) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).HsetCtx(ctx, key, field, val)
}

// SetWithExpire sets the cache with key and v, using given expire.
func (cc cacheCluster) SetWithExpire(key string, val any, expire time.Duration) error {
	return cc.SetWithExpireCtx(context.Background(), key, val, expire)
}

// SetWithExpireCtx sets the cache with key and v, using given expire.
func (cc cacheCluster) SetWithExpireCtx(ctx context.Context, key string, val any, expire time.Duration) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).SetWithExpireCtx(ctx, key, val, expire)
}

// Take takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) Take(val any, key string, query func(val any) error) error {
	return cc.TakeCtx(context.Background(), val, key, query)
}

// TakeCtx takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) TakeCtx(ctx context.Context, val any, key string, query func(val any) error) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).TakeCtx(ctx, val, key, query)
}

// Takex takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) Takex(val any, key string, field string, query func(val any) error) error {
	return cc.TakexCtx(context.Background(), val, key, field, query)
}

// TakexCtx takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) TakexCtx(ctx context.Context, val any, key string, field string, query func(val any) error) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).TakexCtx(ctx, val, key, field, query)
}

// TakeAllOne takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) TakeAllOne(val any, key string, query func(val any, fields ...string) error) error {
	return cc.TakeAllOneCtx(context.Background(), val, key, query)
}

// TakeAllOneCtx takes the result from cache first, if not found,
// query from DB and set cache using c.expiry, then return the result.
func (cc cacheCluster) TakeAllOneCtx(ctx context.Context, val any, key string, query func(val any, fields ...string) error) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).TakeAllOneCtx(ctx, val, key, query)
}

// TakeWithExpire takes the result from cache first, if not found,
// query from DB and set cache using given expire, then return the result.
func (cc cacheCluster) TakeWithExpire(val any, key string, query func(val any, expire time.Duration) error) error {
	return cc.TakeWithExpireCtx(context.Background(), val, key, query)
}

// TakeWithExpireCtx takes the result from cache first, if not found,
// query from DB and set cache using given expire, then return the result.
func (cc cacheCluster) TakeWithExpireCtx(ctx context.Context, val any, key string, query func(val any, expire time.Duration) error) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).TakeWithExpireCtx(ctx, val, key, query)
}

// TakeWithExpire2 takes the result from cache first, if not found,
// query from DB and set cache using given expire, then return the result.
func (cc cacheCluster) TakeWithExpire2(val any, key string, field string, query func(val any) error) error {
	return cc.TakeWithExpire2Ctx(context.Background(), val, key, field, query)
}

// TakeWithExpire2Ctx takes the result from cache first, if not found,
// query from DB and set cache using given expire, then return the result.
func (cc cacheCluster) TakeWithExpire2Ctx(ctx context.Context, val any, key string, field string, query func(val any) error) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}

	return c.(Cache).TakeWithExpire2Ctx(ctx, val, key, field, query)
}

func (cc cacheCluster) Exists(key string) (bool, error) {
	return cc.ExistsCtx(context.Background(), key)
}

func (cc cacheCluster) ExistsCtx(ctx context.Context, key string) (bool, error) {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return false, cc.errNotFound
	}
	return c.(Cache).ExistsCtx(ctx, key)
}

func (cc cacheCluster) Hexists(key string, field string) (bool, error) {
	return cc.HexistsCtx(context.Background(), key, field)
}

func (cc cacheCluster) HexistsCtx(ctx context.Context, key string, field string) (bool, error) {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return false, cc.errNotFound
	}
	return c.(Cache).HexistsCtx(context.Background(), key, field)
}

// Expire is the implementation of redis expire command.
func (cc cacheCluster) Expire(key string, expire time.Duration) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}
	return c.(Cache).Expire(key, expire)
}

// ExpireCtx is the implementation of redis expire command.
func (cc cacheCluster) ExpireCtx(ctx context.Context, key string, expire time.Duration) error {
	c, ok := cc.dispatcher.Get(key)
	if !ok {
		return cc.errNotFound
	}
	return c.(Cache).ExpireCtx(ctx, key, expire)
}
