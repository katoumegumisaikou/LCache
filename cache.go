// cache.go 这个文件实现对本地缓存 store 封装，并对缓存命中的统计
package lcache

import (
	"context"
	"fmt"
	"lcache/store"
	"sync"
	"sync/atomic"
	"time"
)

// Cache 是对底层缓存存储的封装
type Cache struct {
	mu     sync.RWMutex
	store  store.Store  // 底层存储实现
	opts   CacheOptions // 缓存配置选项
	hits   int64        // 缓存命中次数
	misses int64        // 缓存未命中次数
	closed int32        // 原子变量，标记缓存是否已关闭
}

// CacheOptions 缓存配置选项
type CacheOptions struct {
	CacheType    store.CacheType                     // 缓存类型: LRU, LRU2 等
	MaxBytes     int64                               // 最大内存使用量
	BucketCount  uint16                              // 缓存桶数量 (用于 LRU2)
	CapPerBucket uint16                              // 每个缓存桶的容量 (用于 LRU2)
	Level2Cap    uint16                              // 二级缓存桶的容量 (用于 LRU2)
	CleanupTime  time.Duration                       // 清理间隔
	OnEvicted    func(key string, value store.Value) // 驱逐回调
}

// DefaultCacheOptions 返回默认的缓存配置
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024, // 8MB
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

// NewCache 创建一个新的缓存实例
func NewCache(opts CacheOptions) *Cache {
	op := store.Option{
		MaxBytes:        opts.MaxBytes,
		BucketCount:     opts.BucketCount,
		CapPerBucket:    opts.CapPerBucket,
		Level2Cap:       opts.Level2Cap,
		CleanupInterval: opts.CleanupTime,
		OnEvicted:       opts.OnEvicted,
	}
	c := &Cache{
		opts:  opts,
		store: store.NewStore(opts.CacheType, op),
	}
	return c
}

func (c *Cache) Set(ctx context.Context, key string, value ByteView) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrGroupClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if key == "" {
		return ErrKeyRequired
	}
	if value.Len() == 0 {
		return ErrValueRequired
	}

	ok := c.store.Set(key, &value)
	if !ok {
		return fmt.Errorf("SetWithExpiration 失败")
	}
	return nil
}

func (c *Cache) SetWithExpiration(ctx context.Context, key string, value ByteView, expiration time.Duration) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrGroupClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if key == "" {
		return ErrKeyRequired
	}
	if value.Len() == 0 {
		return ErrValueRequired
	}

	ok := c.store.SetWithExpiration(key, &value, expiration)
	if !ok {
		return fmt.Errorf("SetWithExpiration 失败")
	}
	return nil
}

func (c *Cache) Delete(ctx context.Context, key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return false
	}

	c.mu.Lock()
	defer c.mu.RUnlock()

	return c.store.Delete(key)
}

func (c *Cache) Get(ctx context.Context, key string) (ByteView, bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	b, exist := c.store.Get(key)
	if exist {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	atomic.AddInt64(&c.hits, 1)

	// 转换并返回
	if bv, ok := b.(*ByteView); ok {
		return *bv, true
	}

	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

func (c *Cache) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 等待gc回收
	c.store.Close()
	c.store = nil
}

func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"closed": atomic.LoadInt32(&c.closed) == 1,
		"hits":   atomic.LoadInt64(&c.hits),
		"misses": atomic.LoadInt64(&c.misses),
	}

	stats["size"] = c.Len()

	// 计算命中率
	totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
	if totalRequests > 0 {
		stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
	} else {
		stats["hit_rate"] = 0.0
	}

	return stats

}
