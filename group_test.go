package lcache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGroup_GetFromGetter(t *testing.T) {
	g := NewGroup("test-getter", 1024*1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return []byte("loaded:" + key), nil
		},
	))
	defer g.Close()

	ctx := context.Background()
	view, err := g.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if view.String() != "loaded:hello" {
		t.Fatalf("expected 'loaded:hello', got %q", view.String())
	}

	// 第二次 Get 应该命中本地缓存
	view2, err := g.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	if view2.String() != "loaded:hello" {
		t.Fatalf("expected 'loaded:hello' from cache, got %q", view2.String())
	}
}

func TestGroup_GetEmptyKey(t *testing.T) {
	g := NewGroup("test-empty", 1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return nil, fmt.Errorf("should not be called")
		},
	))
	defer g.Close()

	_, err := g.Get(context.Background(), "")
	if err != ErrKeyRequired {
		t.Fatalf("expected ErrKeyRequired, got %v", err)
	}
}

func TestGroup_SetAndGet(t *testing.T) {
	g := NewGroup("test-setget", 1024*1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return []byte("fallback:" + key), nil
		},
	))
	defer g.Close()

	ctx := context.Background()
	_, err := g.Set(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	view, err := g.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if view.String() != "value1" {
		t.Fatalf("expected 'value1', got %q", view.String())
	}
}

func TestGroup_SetThenGetWithoutGetter(t *testing.T) {
	g := NewGroup("test-setnil", 1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return nil, nil
		},
	))
	defer g.Close()

	ctx := context.Background()
	g.Set(ctx, "cached", []byte("cached-value"), 0)

	view, err := g.Get(ctx, "cached")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if view.String() != "cached-value" {
		t.Fatalf("expected 'cached-value', got %q", view.String())
	}
}

func TestGroup_Delete(t *testing.T) {
	g := NewGroup("test-delete", 1024*1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return []byte("fallback:" + key), nil
		},
	))
	defer g.Close()

	ctx := context.Background()
	g.Set(ctx, "del-key", []byte("before-delete"), 0)

	deleted, err := g.Delete(ctx, "del-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !deleted {
		t.Fatal("Delete should return true")
	}

	// 删除后 Get 应回退到 getter
	view, err := g.Get(ctx, "del-key")
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if view.String() != "fallback:del-key" {
		t.Fatalf("expected getter fallback, got %q", view.String())
	}
}

func TestGroup_SingleflightDedup(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	g := NewGroup("test-singleflight", 1024*1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			return []byte("loaded:" + key), nil
		},
	))
	defer g.Close()

	ctx := context.Background()

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Get(ctx, "dedup-key")
		}()
	}
	wg.Wait()

	if callCount != 1 {
		t.Fatalf("expected 1 getter call (singleflight), got %d", callCount)
	}
}

func TestGroup_Stats(t *testing.T) {
	g := NewGroup("test-stats", 1024*1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return []byte("value:" + key), nil
		},
	))
	defer g.Close()

	ctx := context.Background()

	g.Get(ctx, "key1") // 未命中 → getter 加载
	g.Get(ctx, "key1") // 命中本地缓存
	g.Get(ctx, "key2") // 未命中 → getter 加载

	stats := g.Stats()
	t.Logf("Stats: %+v", stats)

	if stats["local_hits"].(int64) < 1 {
		t.Errorf("expected local_hits >= 1, got %d", stats["local_hits"])
	}
	if stats["local_misses"].(int64) < 1 {
		t.Errorf("expected local_misses >= 1, got %d", stats["local_misses"])
	}
	if stats["loader_hits"].(int64) < 2 {
		t.Errorf("expected loader_hits >= 2, got %d", stats["loader_hits"])
	}
}

func TestGroup_Closed(t *testing.T) {
	g := NewGroup("test-close", 1024, GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return []byte("v"), nil
		},
	))
	g.Close()

	ctx := context.Background()
	_, err := g.Get(ctx, "any")
	if err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}

	_, err = g.Set(ctx, "any", []byte("v"), 0)
	if err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}
}

func TestGroup_Expiration(t *testing.T) {
	g := NewGroup("test-exp", 1024*1024,
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("refreshed:" + key), nil
		}),
		WithEpiration(50*time.Millisecond),
	)
	defer g.Close()

	ctx := context.Background()
	g.Set(ctx, "exp-key", []byte("exp-value"), 0)

	view, err := g.Get(ctx, "exp-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if view.String() != "exp-value" {
		t.Fatalf("expected 'exp-value', got %q", view.String())
	}

	// 等待过期后 getter 回源
	time.Sleep(100 * time.Millisecond)
	view2, err := g.Get(ctx, "exp-key")
	if err != nil {
		t.Fatalf("Get after expiration failed: %v", err)
	}
	if view2.String() != "refreshed:exp-key" {
		t.Fatalf("expected refreshed value, got %q", view2.String())
	}
}

func TestGetGroup(t *testing.T) {
	dummyGetter := GetterFunc(func(ctx context.Context, key string) ([]byte, error) { return nil, nil })
	g1 := NewGroup("getgroup-test", 1024, dummyGetter)
	defer g1.Close()

	found := GetGroup("getgroup-test")
	if found != g1 {
		t.Fatal("GetGroup should return the registered group")
	}

	notFound := GetGroup("nonexistent")
	if notFound != nil {
		t.Fatal("GetGroup for nonexistent should return nil")
	}
}

func TestListGroups(t *testing.T) {
	dummyGetter := GetterFunc(func(ctx context.Context, key string) ([]byte, error) { return nil, nil })
	g1 := NewGroup("list-1", 1024, dummyGetter)
	g2 := NewGroup("list-2", 1024, dummyGetter)
	defer g1.Close()
	defer g2.Close()

	names := ListGroups()
	found1, found2 := false, false
	for _, n := range names {
		if n == "list-1" {
			found1 = true
		}
		if n == "list-2" {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Fatalf("expected both groups in list, got %v", names)
	}
}
