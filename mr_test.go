package gowf

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

func TestFinish(t *testing.T) {
	var total uint32
	_ = Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})
	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
	assert.Nil(t, nil)
}
