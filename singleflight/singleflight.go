package singleflight

import (
	"fmt"
	"sync"
)

type call struct {
	wg    sync.WaitGroup
	value any
	err   error
}

type Group struct {
	m sync.Map
}

func (g *Group) Do(key string, f func() (any, error)) (any, error) {
	c := &call{}
	c.wg.Add(1)

	// LoadOrStore 原子地检查 key 是否存在，避免 check-then-set 竞态
	actual, loaded := g.m.LoadOrStore(key, c)
	if loaded {
		// 已有其他 goroutine 在处理，等待其结果
		c.wg.Done() // 抵消 Add(1)，因为我们自己的 call 没被用上
		existing, ok := actual.(*call)
		if !ok {
			return nil, fmt.Errorf("类型错误")
		}
		existing.wg.Wait()
		return existing.value, existing.err
	}

	// 我们是第一个，执行函数
	c.value, c.err = f()
	c.wg.Done()
	g.m.Delete(key)

	return c.value, c.err
}
