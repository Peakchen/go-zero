package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/Peakchen/go-zero/core/logx/logtest"
)

func TestInfo(t *testing.T) {
	collector := new(LogCollector)
	req := httptest.NewRequest(http.MethodGet, "http://localhost", http.NoBody)
	req = req.WithContext(context.WithValue(req.Context(), LogContext, collector))
	Info(req, "first")
	Infof(req, "second %s", "third")
	val := collector.Flush()
	assert.True(t, strings.Contains(val, "first"))
	assert.True(t, strings.Contains(val, "second"))
	assert.True(t, strings.Contains(val, "third"))
	assert.True(t, strings.Contains(val, "\n"))
}

func TestError(t *testing.T) {
	c := logtest.NewCollector(t)
	req := httptest.NewRequest(http.MethodGet, "http://localhost", http.NoBody)
	Error(req, "first")
	Errorf(req, "second %s", "third")
	val := c.String()
	assert.True(t, strings.Contains(val, "first"))
	assert.True(t, strings.Contains(val, "second"))
	assert.True(t, strings.Contains(val, "third"))
}

func TestContextKey_String(t *testing.T) {
	val := contextKey("foo")
	assert.True(t, strings.Contains(val.String(), "foo"))
}
