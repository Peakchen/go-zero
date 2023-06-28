package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Peakchen/go-zero/core/stores/cache"
)

type (
	beginnable func(*sql.DB, cache.Cache) (trans, error)

	trans interface {
		Session
		Commit() error
		Rollback() error
	}

	txSession struct {
		*sql.Tx
		c cache.Cache
		key string
		field string
	}
)

// NewSessionFromTx returns a Session with the given sql.Tx.
// Use it with caution, it's provided for other ORM to interact with.
func NewSessionFromTx(tx *sql.Tx) Session {
	return &txSession{Tx: tx}
}

func (t *txSession) SetContext(key string, field string){
	t.key = key
	t.field = field
}

func (t txSession) Exec(q string, args ...any) (sql.Result, error) {
	return t.ExecCtx(context.Background(), q, args...)
}

func (t txSession) ExecCtx(ctx context.Context, q string, args ...any) (result sql.Result, err error) {
	ctx, span := startSpan(ctx, "Exec")
	defer func() {
		endSpan(span, err)
	}()

	result, err = exec(ctx, t.Tx, q, args...)
	if t.field == "" && t.key != ""{
		if exist, _ := t.c.ExistsCtx(ctx, t.key); !exist {
			return result, err
		}
		if err := t.c.DelCtx(ctx, t.key); err != nil {
			return nil, err
		}
	}else {
		if t.field != "" && t.key != ""{
			if exist, _ := t.c.Hexists(ctx, t.key, t.field); !exist {
				return result, err
			}
			if err := t.c.DelxCtx(ctx, t.key, t.field); err != nil {
				return nil, err
			}
		}
	}
	return
}

func (t txSession) Prepare(q string) (StmtSession, error) {
	return t.PrepareCtx(context.Background(), q)
}

func (t txSession) PrepareCtx(ctx context.Context, q string) (stmtSession StmtSession, err error) {
	ctx, span := startSpan(ctx, "Prepare")
	defer func() {
		endSpan(span, err)
	}()

	stmt, err := t.Tx.PrepareContext(ctx, q)
	if err != nil {
		return nil, err
	}

	return statement{
		query: q,
		stmt:  stmt,
	}, nil
}

func (t txSession) QueryRow(v any, q string, args ...any) error {
	return t.QueryRowCtx(context.Background(), v, q, args...)
}

func (t txSession) QueryRowCtx(ctx context.Context, v any, q string, args ...any) (err error) {
	ctx, span := startSpan(ctx, "QueryRow")
	defer func() {
		endSpan(span, err)
	}()

	return query(ctx, t.Tx, func(rows *sql.Rows) error {
		return unmarshalRow(v, rows, true)
	}, q, args...)
}

func (t txSession) QueryRowPartial(v any, q string, args ...any) error {
	return t.QueryRowPartialCtx(context.Background(), v, q, args...)
}

func (t txSession) QueryRowPartialCtx(ctx context.Context, v any, q string,
	args ...any) (err error) {
	ctx, span := startSpan(ctx, "QueryRowPartial")
	defer func() {
		endSpan(span, err)
	}()

	return query(ctx, t.Tx, func(rows *sql.Rows) error {
		return unmarshalRow(v, rows, false)
	}, q, args...)
}

func (t txSession) QueryRows(v any, q string, args ...any) error {
	return t.QueryRowsCtx(context.Background(), v, q, args...)
}

func (t txSession) QueryRowsCtx(ctx context.Context, v any, q string, args ...any) (err error) {
	ctx, span := startSpan(ctx, "QueryRows")
	defer func() {
		endSpan(span, err)
	}()

	return query(ctx, t.Tx, func(rows *sql.Rows) error {
		return unmarshalRows(v, rows, true)
	}, q, args...)
}

func (t txSession) QueryRowsPartial(v any, q string, args ...any) error {
	return t.QueryRowsPartialCtx(context.Background(), v, q, args...)
}

func (t txSession) QueryRowsPartialCtx(ctx context.Context, v any, q string,
	args ...any) (err error) {
	ctx, span := startSpan(ctx, "QueryRowsPartial")
	defer func() {
		endSpan(span, err)
	}()

	return query(ctx, t.Tx, func(rows *sql.Rows) error {
		return unmarshalRows(v, rows, false)
	}, q, args...)
}

func begin(db *sql.DB, cc cache.Cache) (trans, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	return &txSession{
		Tx: tx,
		c: cc,
	}, nil
}

func transact(ctx context.Context, db *commonSqlConn, c cache.Cache, b beginnable,
	fn func(context.Context, cache.Cache, Session) error) (err error) {
	conn, err := db.connProv()
	if err != nil {
		db.onError(ctx, err)
		return err
	}

	return transactOnConn(ctx, conn, c, b, fn)
}

func transactOnConn(ctx context.Context, conn *sql.DB, c cache.Cache, b beginnable,
	fn func(context.Context, cache.Cache, Session) error) (err error) {
	var tx trans
	tx, err = b(conn, c)
	if err != nil {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %w", p, e)
			} else {
				err = fmt.Errorf("recover from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %w", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(ctx, c, tx)
}
