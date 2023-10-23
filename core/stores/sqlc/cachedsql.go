package sqlc

import (
	"context"
	"database/sql"
	"time"

	"github.com/Peakchen/go-zero/core/stores/cache"
	"github.com/Peakchen/go-zero/core/stores/kv"
	"github.com/Peakchen/go-zero/core/stores/redis"
	"github.com/Peakchen/go-zero/core/stores/sqlx"
	"github.com/Peakchen/go-zero/core/syncx"
)

// see doc/sql-cache.md
const cacheSafeGapBetweenIndexAndPrimary = time.Second * 5

var (
	// ErrNotFound is an alias of sqlx.ErrNotFound.
	ErrNotFound = sqlx.ErrNotFound

	// can't use one SingleFlight per conn, because multiple conns may share the same cache key.
	singleFlights = syncx.NewSingleFlight()
	stats         = cache.NewStat("sqlc")
)

type (
	// ExecFn defines the sql exec method.
	ExecFn func(conn sqlx.SqlConn) (sql.Result, error)
	// ExecCtxFn defines the sql exec method.
	ExecCtxFn func(ctx context.Context, conn sqlx.SqlConn) (sql.Result, error)
	// IndexQueryFn defines the query method that based on unique indexes.
	IndexQueryFn func(conn sqlx.SqlConn, v any) (any, error)
	// IndexQueryCtxFn defines the query method that based on unique indexes.
	IndexQueryCtxFn func(ctx context.Context, conn sqlx.SqlConn, v any) (any, error)
	// PrimaryQueryFn defines the query method that based on primary keys.
	PrimaryQueryFn func(conn sqlx.SqlConn, v, primary any) error
	// PrimaryQueryCtxFn defines the query method that based on primary keys.
	PrimaryQueryCtxFn func(ctx context.Context, conn sqlx.SqlConn, v, primary any) error
	// QueryFn defines the query method.
	QueryFn func(conn sqlx.SqlConn, v any) error
	// QueryCtxFn defines the query method.
	QueryCtxFn func(ctx context.Context, conn sqlx.SqlConn, v any) error

	// QueryFnx defines the query method for table field.
	QueryFnx func(conn sqlx.SqlConn, v any, field string) error
	// QueryCtxFnx defines the query method for table field.
	QueryCtxFnx func(ctx context.Context, conn sqlx.SqlConn, v any, field string) error
	// QueryAllOneFnx defines the query method for table field.
	QueryAllOneFnx func(conn sqlx.SqlConn, v any, fields ...string) error
	// QueryAllOneCtxFnx defines the query method for table fields.
	QueryAllOneCtxFnx func(ctx context.Context, conn sqlx.SqlConn, v any, fields ...string) error

	// ExecFn defines the sql exec method.
	ExecxFn func(conn sqlx.SqlConn, fields ...string) (sql.Result, error)
	// ExecxCtxFn defines the sql exec method.
	ExecxCtxFn func(ctx context.Context, conn sqlx.SqlConn, fields ...string) (sql.Result, error)

	// A CachedConn is a DB connection with cache capability.
	CachedConn struct {
		db    sqlx.SqlConn
		cache cache.Cache
		red   kv.Store
	}
)

// NewConn returns a CachedConn with a redis cluster cache.
func NewConn(db sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) CachedConn {
	cc := cache.New(c, singleFlights, stats, sql.ErrNoRows, opts...)
	conn := NewConnWithCache(db, cc)
	conn.red = kv.NewStore(c)
	return conn
}

// NewConnWithCache returns a CachedConn with a custom cache.
func NewConnWithCache(db sqlx.SqlConn, c cache.Cache) CachedConn {
	return CachedConn{
		db:    db,
		cache: c,
	}
}

// NewNodeConn returns a CachedConn with a redis node cache.
func NewNodeConn(db sqlx.SqlConn, rds *redis.Redis, opts ...cache.Option) CachedConn {
	c := cache.NewNode(rds, singleFlights, stats, sql.ErrNoRows, opts...)
	return NewConnWithCache(db, c)
}

// store operate redis
func (cc CachedConn) GetRedis() kv.Store {
	return cc.red
}

// DelCache deletes cache with keys.
func (cc CachedConn) DelCache(keys ...string) error {
	return cc.DelCacheCtx(context.Background(), keys...)
}

// DelCacheCtx deletes cache with keys.
func (cc CachedConn) DelCacheCtx(ctx context.Context, keys ...string) error {
	return cc.cache.DelCtx(ctx, keys...)
}

// DelCache deletes cache with keys.
func (cc CachedConn) DelxCache(key string, fields ...string) error {
	return cc.DelxCacheCtx(context.Background(), key, fields...)
}

// DelCacheCtx deletes cache with keys.
func (cc CachedConn) DelxCacheCtx(ctx context.Context, key string, fields ...string) error {
	return cc.cache.DelxCtx(ctx, key, fields...)
}

// GetCache unmarshals cache with given key into v.
func (cc CachedConn) GetCache(key string, v any) error {
	return cc.GetCacheCtx(context.Background(), key, v)
}

// GetCacheCtx unmarshals cache with given key into v.
func (cc CachedConn) GetCacheCtx(ctx context.Context, key string, v any) error {
	return cc.cache.GetCtx(ctx, key, v)
}

// Exec runs given exec on given keys, and returns execution result.
func (cc CachedConn) Exec(exec ExecFn, keys ...string) (sql.Result, error) {
	execCtx := func(_ context.Context, conn sqlx.SqlConn) (sql.Result, error) {
		return exec(conn)
	}
	return cc.ExecCtx(context.Background(), execCtx, keys...)
}

// ExecCtx runs given exec on given keys, and returns execution result.
func (cc CachedConn) ExecCtx(ctx context.Context, exec ExecCtxFn, keys ...string) (
	sql.Result, error) {
	res, err := exec(ctx, cc.db)
	if err != nil {
		return nil, err
	}

	if err := cc.DelCacheCtx(ctx, keys...); err != nil {
		return nil, err
	}

	return res, nil
}

// Execx runs given exec on given key and fields, and returns execution result.
func (cc CachedConn) Execx(exec ExecxFn, key string, fields ...string) (sql.Result, error) {
	execCtx := func(_ context.Context, conn sqlx.SqlConn, fields ...string) (sql.Result, error) {
		return exec(conn, fields...)
	}
	return cc.ExecxCtx(context.Background(), execCtx, key, fields...)
}

// ExecCtx runs given exec on given keys, and returns execution result.
func (cc CachedConn) ExecxCtx(ctx context.Context, exec ExecxCtxFn, key string, fields ...string) (
	sql.Result, error) {
	res, err := exec(ctx, cc.db, fields...)
	if err != nil {
		return nil, err
	}

	if err := cc.DelxCacheCtx(ctx, key, fields...); err != nil {
		return nil, err
	}

	return res, nil
}

// ExecNoCache runs exec with given sql statement, without affecting cache.
func (cc CachedConn) ExecNoCache(q string, args ...any) (sql.Result, error) {
	return cc.ExecNoCacheCtx(context.Background(), q, args...)
}

// ExecNoCacheCtx runs exec with given sql statement, without affecting cache.
func (cc CachedConn) ExecNoCacheCtx(ctx context.Context, q string, args ...any) (
	sql.Result, error) {
	return cc.db.ExecCtx(ctx, q, args...)
}

// QueryRow unmarshals into v with given key and query func.
func (cc CachedConn) QueryRow(v any, key string, query QueryFn) error {
	queryCtx := func(_ context.Context, conn sqlx.SqlConn, v any) error {
		return query(conn, v)
	}
	return cc.QueryRowCtx(context.Background(), v, key, queryCtx)
}

// QueryRowCtx unmarshals into v with given key and query func.
func (cc CachedConn) QueryRowCtx(ctx context.Context, v any, key string, query QueryCtxFn) error {
	return cc.cache.TakeCtx(ctx, v, key, func(v any) error {
		return query(ctx, cc.db, v)
	})
}

// QueryRowx unmarshals into v with given key and query func.
func (cc CachedConn) QueryRowx(v any, key, field string, query QueryFnx) error {
	queryCtx := func(_ context.Context, conn sqlx.SqlConn, v any, field string) error {
		return query(conn, v, field)
	}
	return cc.QueryRowxCtx(context.Background(), v, key, field, queryCtx)
}

// QueryRowxCtx unmarshals into v with given key and query func.
func (cc CachedConn) QueryRowxCtx(ctx context.Context, v any, key, field string, query QueryCtxFnx) error {
	return cc.cache.TakexCtx(ctx, v, key, field, func(v any) error {
		return query(ctx, cc.db, v, field)
	})
}

// QueryRowx unmarshals into v with given key and query func.
func (cc CachedConn) QueryRowAllOne(v any, key, field string, query QueryAllOneFnx) error {
	queryCtx := func(_ context.Context, conn sqlx.SqlConn, v any, fields ...string) error {
		return query(conn, v, fields...)
	}
	return cc.QueryRowAllOneCtx(context.Background(), v, key, queryCtx)
}

// QueryRowxCtx unmarshals into v with given key and query func.
func (cc CachedConn) QueryRowAllOneCtx(ctx context.Context, v any, key string, query QueryAllOneCtxFnx) error {
	return cc.cache.TakeAllOneCtx(ctx, v, key, func(v any, fields ...string) error {
		return query(ctx, cc.db, v, fields...)
	})
}

// QueryRowIndex unmarshals into v with given key.
func (cc CachedConn) QueryRowIndex(v any, key string, keyer func(primary any) string,
	indexQuery IndexQueryFn, primaryQuery PrimaryQueryFn) error {
	indexQueryCtx := func(_ context.Context, conn sqlx.SqlConn, v any) (any, error) {
		return indexQuery(conn, v)
	}
	primaryQueryCtx := func(_ context.Context, conn sqlx.SqlConn, v, primary any) error {
		return primaryQuery(conn, v, primary)
	}

	return cc.QueryRowIndexCtx(context.Background(), v, key, keyer, indexQueryCtx, primaryQueryCtx)
}

// QueryRowIndexCtx unmarshals into v with given key.
func (cc CachedConn) QueryRowIndexCtx(ctx context.Context, v any, key string,
	keyer func(primary any) string, indexQuery IndexQueryCtxFn,
	primaryQuery PrimaryQueryCtxFn) error {
	var primaryKey any
	var found bool

	if err := cc.cache.TakeWithExpireCtx(ctx, &primaryKey, key,
		func(val any, expire time.Duration) (err error) {
			primaryKey, err = indexQuery(ctx, cc.db, v)
			if err != nil {
				return
			}

			found = true
			return cc.cache.SetWithExpireCtx(ctx, keyer(primaryKey), v,
				expire+cacheSafeGapBetweenIndexAndPrimary)
		}); err != nil {
		return err
	}

	if found {
		return nil
	}

	return cc.cache.TakeCtx(ctx, v, keyer(primaryKey), func(v any) error {
		return primaryQuery(ctx, cc.db, v, primaryKey)
	})
}

// QueryRowNoCache unmarshals into v with given statement.
func (cc CachedConn) QueryRowNoCache(v any, q string, args ...any) error {
	return cc.QueryRowNoCacheCtx(context.Background(), v, q, args...)
}

// QueryRowNoCacheCtx unmarshals into v with given statement.
func (cc CachedConn) QueryRowNoCacheCtx(ctx context.Context, v any, q string,
	args ...any) error {
	return cc.db.QueryRowCtx(ctx, v, q, args...)
}

// QueryRowsNoCache unmarshals into v with given statement.
// It doesn't use cache, because it might cause consistency problem.
func (cc CachedConn) QueryRowsNoCache(v any, q string, args ...any) error {
	return cc.QueryRowsNoCacheCtx(context.Background(), v, q, args...)
}

// QueryRowsNoCacheCtx unmarshals into v with given statement.
// It doesn't use cache, because it might cause consistency problem.
func (cc CachedConn) QueryRowsNoCacheCtx(ctx context.Context, v any, q string,
	args ...any) error {
	return cc.db.QueryRowsCtx(ctx, v, q, args...)
}

// SetCache sets v into cache with given key.
func (cc CachedConn) SetCache(key string, val any) error {
	return cc.SetCacheCtx(context.Background(), key, val)
}

// SetCacheCtx sets v into cache with given key.
func (cc CachedConn) SetCacheCtx(ctx context.Context, key string, val any) error {
	return cc.cache.SetCtx(ctx, key, val)
}

// Transact runs given fn in transaction mode.
func (cc CachedConn) Transact(fn func(sqlx.Session) error) error {
	fnCtx := func(_ context.Context, c cache.Cache, session sqlx.Session) error {
		return fn(session)
	}
	return cc.TransactCtx(context.Background(), fnCtx)
}

// TransactCtx runs given fn in transaction mode.
func (cc CachedConn) TransactCtx(ctx context.Context, fn func(context.Context, cache.Cache, sqlx.Session) error) error {
	return cc.db.TransactCtx(ctx, cc.cache, fn)
}
