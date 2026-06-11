package lcache

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"lcache/store"
)

var benchmarkByteViewSink ByteView

const benchmarkKeyCount = 1 << 12

func benchmarkCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     64 << 20,
		BucketCount:  64,
		CapPerBucket: 1024,
		Level2Cap:    512,
		CleanupTime:  time.Minute,
	}
}

func benchmarkLRUCacheOptions() CacheOptions {
	opts := benchmarkCacheOptions()
	opts.CacheType = store.LRU
	return opts
}

func benchmarkKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
	}
	return keys
}

func BenchmarkLocalCache_Set(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkLRUCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	value := ByteView{b: []byte("benchmark-value")}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(ctx, keys[i&(len(keys)-1)], value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalCache_GetHit(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	value := ByteView{b: []byte("benchmark-value")}
	for _, key := range keys {
		if err := c.Set(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, ok := c.Get(ctx, keys[i&(len(keys)-1)])
		if !ok {
			b.Fatal("expected cache hit")
		}
		benchmarkByteViewSink = view
	}
}

func BenchmarkLocalCache_GetMiss(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.Get(ctx, keys[i&(len(keys)-1)]); ok {
			b.Fatal("expected cache miss")
		}
	}
}

func BenchmarkLocalCache_SetThenGet(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkLRUCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	value := ByteView{b: []byte("benchmark-value")}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i&(len(keys)-1)]
		if err := c.Set(ctx, key, value); err != nil {
			b.Fatal(err)
		}
		view, ok := c.Get(ctx, key)
		if !ok {
			b.Fatal("expected cache hit")
		}
		benchmarkByteViewSink = view
	}
}

func BenchmarkLocalCache_DeleteHit(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkLRUCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	value := ByteView{b: []byte("benchmark-value")}

	refill := func() {
		for _, key := range keys {
			if err := c.Set(ctx, key, value); err != nil {
				b.Fatal(err)
			}
		}
	}
	refill()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%len(keys) == 0 {
			b.StopTimer()
			refill()
			b.StartTimer()
		}
		if !c.Delete(ctx, keys[i&(len(keys)-1)]) {
			b.Fatal("expected delete hit")
		}
	}
}

func BenchmarkLocalCache_ParallelGetHit(b *testing.B) {
	ctx := context.Background()
	c := NewCache(benchmarkCacheOptions())
	defer c.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	value := ByteView{b: []byte("benchmark-value")}
	for _, key := range keys {
		if err := c.Set(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		var localSink ByteView
		for pb.Next() {
			view, ok := c.Get(ctx, keys[i&(len(keys)-1)])
			if !ok {
				b.Fatal("expected cache hit")
			}
			localSink = view
			i++
		}
		if localSink.Len() < 0 {
			b.Fatal("unreachable")
		}
	})
}

func BenchmarkLocalGroup_GetHit(b *testing.B) {
	logger := logrus.StandardLogger()
	oldOutput := logger.Out
	logger.SetOutput(io.Discard)
	defer logger.SetOutput(oldOutput)

	ctx := context.Background()
	g := NewGroup(
		"bench-local-hit",
		64<<20,
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("loaded:" + key), nil
		}),
		WithCacheOptions(benchmarkCacheOptions()),
	)
	defer g.Close()

	keys := benchmarkKeys(benchmarkKeyCount)
	for _, key := range keys {
		if _, err := g.Set(ctx, key, []byte("benchmark-value"), 0); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := g.Get(ctx, keys[i&(len(keys)-1)])
		if err != nil {
			b.Fatal(err)
		}
		benchmarkByteViewSink = view
	}
}
