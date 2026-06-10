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

	if val, ok := g.m.Load(key); ok {
		c, ok := val.(*call)
		if !ok {
			return nil, fmt.Errorf("类型错误")
		}
		c.wg.Wait()
		return c.value, c.err
	}

	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c)
	c.value, c.err = f()
	c.wg.Done()

	return c.value, c.err
}
