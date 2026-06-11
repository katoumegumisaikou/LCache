package singleflight

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDo_SingleCall(t *testing.T) {
	g := &Group{}
	callCount := 0

	v, err := g.Do("key1", func() (any, error) {
		callCount++
		return "value1", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "value1" {
		t.Fatalf("expected 'value1', got %v", v)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}

func TestDo_DeduplicateConcurrentCalls(t *testing.T) {
	g := &Group{}
	callCount := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	results := make([]string, 20)

	for i := range 20 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			v, err := g.Do("same-key", func() (any, error) {
				mu.Lock()
				callCount++
				mu.Unlock()
				time.Sleep(10 * time.Millisecond) // 模拟耗时操作
				return fmt.Sprintf("result"), nil
			})
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = v.(string)
		}(i)
	}

	wg.Wait()

	if callCount != 1 {
		t.Fatalf("expected 1 call (deduplicated), got %d", callCount)
	}

	for i, r := range results {
		if r != "result" {
			t.Errorf("goroutine %d: expected 'result', got %q", i, r)
		}
	}
}

func TestDo_DifferentKeys(t *testing.T) {
	g := &Group{}
	callCount := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", idx)
			_, err := g.Do(key, func() (any, error) {
				mu.Lock()
				callCount++
				mu.Unlock()
				return key, nil
			})
			if err != nil {
				t.Errorf("unexpected error for key %s: %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	if callCount != 10 {
		t.Fatalf("expected 10 calls (different keys), got %d", callCount)
	}
}

func TestDo_ErrorPropagation(t *testing.T) {
	g := &Group{}
	expectedErr := errors.New("something went wrong")

	// 确保 goroutine A 先占据 key，goroutine B 作为等待者，
	// 然后 goroutine A 返回错误，goroutine B 收到相同错误
	firstInDo := make(chan struct{})
	secondInDo := make(chan struct{})

	var err error
	var wg sync.WaitGroup
	wg.Add(2)

	// goroutine A：先进入，占据 key
	go func() {
		defer wg.Done()
		_, errA := g.Do("err-key", func() (any, error) {
			close(firstInDo) // 已占据 key
			<-secondInDo     // 等到 B 也进了 Do 再返回
			return nil, expectedErr
		})
		// 第一个调用者也应收到 f() 的错误
		if errA != expectedErr {
			t.Errorf("first call should get expectedErr, got %v", errA)
		}
	}()

	<-firstInDo // A 已占据 key

	// goroutine B：后进入，成为等待者
	go func() {
		defer wg.Done()
		_, err = g.Do("err-key", func() (any, error) {
			t.Error("second fn should not be called")
			return nil, nil
		})
	}()

	// 确保 B 已进入 Do（LoadOrStore 会找到 A 的 call）
	// 通过短暂 sleep 给 B 时间进入 LoadOrStore+Wait
	time.Sleep(20 * time.Millisecond)
	close(secondInDo) // 放行 A
	wg.Wait()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}
