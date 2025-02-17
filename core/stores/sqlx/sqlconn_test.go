package sqlx

import (
	"database/sql"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/Peakchen/go-zero/core/logx"
	"github.com/Peakchen/go-zero/core/trace/tracetest"
)

const mockedDatasource = "sqlmock"

func init() {
	logx.Disable()
}

func TestSqlConn(t *testing.T) {
	me := tracetest.NewInMemoryExporter(t)
	mock, err := buildConn()
	assert.Nil(t, err)
	mock.ExpectExec("any")
	mock.ExpectQuery("any").WillReturnRows(sqlmock.NewRows([]string{"foo"}))
	conn := NewMysql(mockedDatasource)
	db, err := conn.RawDB()
	assert.Nil(t, err)
	rawConn := NewSqlConnFromDB(db, withMysqlAcceptable())
	badConn := NewMysql("badsql")
	_, err = conn.Exec("any", "value")
	assert.NotNil(t, err)
	_, err = badConn.Exec("any", "value")
	assert.NotNil(t, err)
	_, err = rawConn.Prepare("any")
	assert.NotNil(t, err)
	_, err = badConn.Prepare("any")
	assert.NotNil(t, err)
	var val string
	assert.NotNil(t, conn.QueryRow(&val, "any"))
	assert.NotNil(t, badConn.QueryRow(&val, "any"))
	assert.NotNil(t, conn.QueryRowPartial(&val, "any"))
	assert.NotNil(t, badConn.QueryRowPartial(&val, "any"))
	assert.NotNil(t, conn.QueryRows(&val, "any"))
	assert.NotNil(t, badConn.QueryRows(&val, "any"))
	assert.NotNil(t, conn.QueryRowsPartial(&val, "any"))
	assert.NotNil(t, badConn.QueryRowsPartial(&val, "any"))
	assert.NotNil(t, conn.Transact(nil, func(session Session) error {
		return nil
	}))
	assert.NotNil(t, badConn.Transact(nil,func(session Session) error {
		return nil
	}))
	assert.Equal(t, 14, len(me.GetSpans()))
}

func buildConn() (mock sqlmock.Sqlmock, err error) {
	connManager.GetResource(mockedDatasource, func() (io.Closer, error) {
		var db *sql.DB
		var err error
		db, mock, err = sqlmock.New()
		return db, err
	})
	return
}
