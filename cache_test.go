package lcache

import (
	"context"
	"sync"
	"testing"
	"time"

	"lcache/store"
)

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache(CacheOptions{
		CacheType:    store.LRU,
		MaxBytes:     1024,
		BucketCount:  4,
		CapPerBucket: 64,
		Level2Cap:    32,
		CleanupTime:  time.Minute,
	})
	defer c.Close()

	ctx := context.Background()
	key := "test-key"
	value := ByteView{b: cloneBytes([]byte("test-value"))}

	err := c.Set(ctx, key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if c.Len() != 1 {
		t.Fatalf("expected length 1, got %d", c.Len())
	}

	got, ok := c.Get(ctx, key)
	if !ok {
		t.Fatal("Get returned false")
	}
	if got.String() != "test-value" {
		t.Fatalf("expected 'test-value', got %q", got.String())
	}
}

func TestCache_GetNonExistent(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	_, ok := c.Get(ctx, "nonexistent")
	if ok {
		t.Fatal("expected false for nonexistent key")
	}
}

func TestCache_SetEmptyKey(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	err := c.Set(ctx, "", ByteView{b: []byte("value")})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestCache_SetEmptyValue(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	err := c.Set(ctx, "key", ByteView{b: []byte{}})
	if err == nil {
		t.Fatal("expected error for empty value")
	}
}

func TestCache_Delete(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	c.Set(ctx, "key", ByteView{b: []byte("value")})

	if !c.Delete(ctx, "key") {
		t.Fatal("Delete should return true")
	}
	if c.Len() != 0 {
		t.Fatalf("expected length 0, got %d", c.Len())
	}

	// 删除不存在的 key
	if c.Delete(ctx, "key") {
		t.Fatal("Delete should return false for nonexistent key")
	}
}

func TestCache_Clear(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	for i := range 10 {
		c.Set(ctx, string(rune('a'+i)), ByteView{b: []byte{byte('a' + i)}})
	}

	c.Clear()
	if c.Len() != 0 {
		t.Fatalf("expected length 0 after clear, got %d", c.Len())
	}
}

func TestCache_Stats(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	ctx := context.Background()
	c.Set(ctx, "hit-key", ByteView{b: []byte("value")})

	// 命中
	c.Get(ctx, "hit-key")
	// 未命中
	c.Get(ctx, "miss-key")

	stats := c.Stats()
	if stats["hits"].(int64) != 1 {
		t.Fatalf("expected 1 hit, got %d", stats["hits"])
	}
	if stats["misses"].(int64) != 1 {
		t.Fatalf("expected 1 miss, got %d", stats["misses"])
	}
}

func TestCache_SetWithExpiration(t *testing.T) {
	c := NewCache(CacheOptions{
		CacheType:    store.LRU,
		MaxBytes:     1024,
		BucketCount:  4,
		CapPerBucket: 64,
		Level2Cap:    32,
		CleanupTime:  time.Minute,
	})
	defer c.Close()

	ctx := context.Background()
	c.SetWithExpiration(ctx, "expires", ByteView{b: []byte("value")}, 50*time.Millisecond)

	// 立即获取应该成功
	_, ok := c.Get(ctx, "expires")
	if !ok {
		t.Fatal("should get value before expiration")
	}

	// 等待过期
	time.Sleep(100 * time.Millisecond)
	_, ok = c.Get(ctx, "expires")
	if ok {
		t.Fatal("should not get value after expiration")
	}
}

func TestCache_ConcurrentSafety(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := string(rune('a' + idx%26))
			c.Set(ctx, key, ByteView{b: []byte{byte(idx)}})
			c.Get(ctx, key)
		}(i)
	}
	wg.Wait()
}

func TestCache_Closed(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	c.Close()

	ctx := context.Background()
	err := c.Set(ctx, "key", ByteView{b: []byte("value")})
	if err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}

	_, ok := c.Get(ctx, "key")
	if ok {
		t.Fatal("Get should return false after Close")
	}
}
