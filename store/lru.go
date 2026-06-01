package store

import (
	"container/list"
	"sync"
	"time"
)

type lruEntry struct {
	Key   string
	Value Value
}

type lruCache struct {
	mu              sync.RWMutex
	list            *list.List
	elems           map[string]*list.Element
	expires         map[string]time.Time // 过期时间映射
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value)
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

func newlruCache(opt Options) *lruCache {
	if opt.CleanupInterval <= 0 {
		opt.CleanupInterval = time.Minute
	}

	cache := &lruCache{
		list:          new(list.List),
		elems:         make(map[string]*list.Element),
		expires:       make(map[string]time.Time),
		maxBytes:      opt.MaxBytes,
		onEvicted:     opt.OnEvicted,
		cleanupTicker: time.NewTicker(opt.CleanupInterval),
		closeCh:       make(chan struct{}),
	}

	go cache.cleanup()
	return cache
}

func (l *lruCache) cleanup() {
	for {
		select {
		case <-l.closeCh:
			return
		case <-l.cleanupTicker.C:
			l.mu.Lock()
			l.evict()
			l.mu.Unlock()
		}
	}
}

func (l *lruCache) evict() {
	now := time.Now()
	for key, expTime := range l.expires {
		if expTime.Before(now) {
			if elem, ok := l.elems[key]; ok {
				l.removeElement(elem)
			}
		}
	}

	if l.maxBytes <= 0 {
		return
	}
	for l.usedBytes > l.maxBytes {
		elem := l.list.Back()
		if elem == nil {
			break
		}
		l.removeElement(elem)
	}
}

func (l *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	l.list.Remove(elem)
	delete(l.elems, entry.Key)
	delete(l.expires, entry.Key)
	l.usedBytes -= int64(len(entry.Key) + entry.Value.Len())

	if l.onEvicted != nil {
		l.onEvicted(entry.Key, entry.Value)
	}
}

func (l *lruCache) Get(key string) (Value, bool) {
	l.mu.RLock()

	// 查看数据是否存在,这里不需要值，以免使用旧值
	_, ok := l.elems[key]
	if !ok {
		l.mu.RUnlock()
		return nil, false
	}

	// 查看数据是否过期
	if expTime, ok := l.expires[key]; ok && expTime.Before(time.Now()) {
		l.mu.RUnlock()
		return nil, false
	}
	l.mu.RUnlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	item, ok := l.elems[key]
	if !ok {
		return nil, false
	}
	l.moveToFront(item)

	return item.Value.(*lruEntry).Value, true
}

func (l *lruCache) Delete(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	elem, ok := l.elems[key]
	if !ok {
		return false
	}
	l.removeElement(elem)
	return true
}

func (l *lruCache) moveToFront(elem *list.Element) {
	l.list.MoveBefore(elem, l.list.Front())
}

func (l *lruCache) Set(key string, value Value) error {
	return l.SetWithExpiration(key, value, 0)
}

func (l *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {

	// 值为空，删除
	if value == nil {
		_ = l.Delete(key)
		return nil
	}

	newEntry := &lruEntry{
		Key:   key,
		Value: value,
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if expiration > 0 {
		expTime := time.Now().Add(expiration)
		l.expires[key] = expTime
	} else {
		delete(l.expires, key)
	}

	// 检测值是否存在
	if elem, exist := l.elems[key]; exist {
		oldEntry := elem.Value.(*lruEntry)
		elem.Value = newEntry
		l.usedBytes += int64(newEntry.Value.Len() - oldEntry.Value.Len())
		l.moveToFront(elem)
		return nil
	}

	l.elems[key] = l.list.PushFront(newEntry)
	l.usedBytes += int64(newEntry.Value.Len() + len(newEntry.Key))
	l.evict()

	return nil
}

func (l *lruCache) Close() {
	l.cleanupTicker.Stop()
	close(l.closeCh)
}
