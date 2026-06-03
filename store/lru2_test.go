package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// testValue 实现 Value 接口，用于测试
type testValue string

func (v testValue) Len() int { return len(v) }

/*-----------------------------------------------------------------------------*/
// cache（底层 LRU 链表）单元测试
/*-----------------------------------------------------------------------------*/

func TestCacheBasic(t *testing.T) {
	t.Run("初始化缓存", func(t *testing.T) {
		c := newCache(10)
		if c == nil {
			t.Fatal("创建缓存失败")
		}
		if c.last != 0 {
			t.Fatalf("初始last应为0，实际为%d", c.last)
		}
		if len(c.m) != 11 { // cap+1
			t.Fatalf("缓存容量应为11，实际为%d", len(c.m))
		}
		if len(c.dlnk) != 11 {
			t.Fatalf("链表长度应为cap+1(11)，实际为%d", len(c.dlnk))
		}
	})

	t.Run("添加和获取", func(t *testing.T) {
		c := newCache(5)
		var evictCount int
		onEvicted := func(key string, value Value) {
			evictCount++
		}

		// 添加新项 — put 返回 bool，新插入返回 true
		isNew := c.put("key1", testValue("value1"), 100, onEvicted)
		if !isNew {
			t.Fatal("添加新项应返回 true")
		}
		if c.last != 1 {
			t.Fatalf("添加一项后last应为1，实际为%d", c.last)
		}

		// 获取项 — get 返回 (*node, bool)
		node, found := c.get("key1")
		if !found {
			t.Fatal("获取存在项应返回 true")
		}
		if node == nil {
			t.Fatal("获取项返回了nil")
		}
		if node.key != "key1" || node.value.(testValue) != "value1" || node.expireAt != 100 {
			t.Fatalf("获取项值不一致: %+v", *node)
		}

		// 获取不存在的项
		node, found = c.get("不存在")
		if found {
			t.Fatal("获取不存在项应返回 false")
		}
		if node != nil {
			t.Fatal("获取不存在项不应返回节点")
		}

		// 更新现有项 — put 返回 false
		isNew = c.put("key1", testValue("新值"), 200, onEvicted)
		if isNew {
			t.Fatal("更新项应返回 false")
		}

		// 验证更新后的值
		node, _ = c.get("key1")
		if node.value.(testValue) != "新值" || node.expireAt != 200 {
			t.Fatalf("更新项后值不一致: %+v", *node)
		}
	})

	t.Run("删除操作", func(t *testing.T) {
		c := newCache(5)

		// 添加项
		c.put("key1", testValue("value1"), 100, nil)

		// 删除存在的项 — delete 返回 (*node, bool, int64)
		node, found, expireAt := c.delete("key1")
		if !found {
			t.Fatal("删除存在项应返回 true")
		}
		if node == nil {
			t.Fatal("删除应返回被删除的节点")
		}
		if node.expireAt != 0 {
			t.Fatalf("删除后节点expireAt应为0，实际为%d", node.expireAt)
		}
		if expireAt != 100 {
			t.Fatalf("删除应返回原始expireAt(100)，实际为%d", expireAt)
		}

		// 验证删除后 hmap 中 key 仍存在但 expireAt 为 0
		node, found = c.get("key1")
		if !found {
			t.Fatal("获取已删除项失败，但键仍应存在于哈希表中")
		}
		if node.expireAt != 0 {
			t.Fatalf("已删除项的expireAt应为0，实际为%d", node.expireAt)
		}

		// 删除不存在的项
		node, found, _ = c.delete("不存在")
		if found {
			t.Fatal("删除不存在项应返回 false")
		}
		if node != nil {
			t.Fatal("删除不存在项不应返回节点")
		}
	})

	t.Run("容量和淘汰", func(t *testing.T) {
		c := newCache(3)
		var evictedKeys []string

		onEvicted := func(key string, value Value) {
			evictedKeys = append(evictedKeys, key)
		}

		// 填满缓存 (cap=3, 哨兵不占容量)
		for i := range 3 {
			c.put(fmt.Sprintf("key%d", i+1), testValue(fmt.Sprintf("value%d", i+1)), now()+int64(time.Hour), onEvicted)
		}

		// 再添加一项，应该淘汰最早的 key1
		c.put("key4", testValue("value4"), now()+int64(time.Hour), onEvicted)

		if len(evictedKeys) != 1 {
			t.Fatalf("应淘汰1项，实际淘汰%d项", len(evictedKeys))
		}
		if evictedKeys[0] != "key1" {
			t.Fatalf("应淘汰key1，实际淘汰%s", evictedKeys[0])
		}

		// 验证缓存状态：key1 已被淘汰
		_, found := c.get("key1")
		if found {
			t.Fatal("key1应已被淘汰")
		}

		for i := range 3 {
			key := fmt.Sprintf("key%d", i+2) // key2, key3, key4
			node, found := c.get(key)
			if !found || node == nil {
				t.Fatalf("%s应存在于缓存中", key)
			}
		}
	})

	t.Run("LRU顺序维护", func(t *testing.T) {
		c := newCache(3)

		// 按顺序添加3项
		for i := range 3 {
			c.put(fmt.Sprintf("key%d", i+1), testValue(fmt.Sprintf("value%d", i+1)), now()+int64(time.Hour), nil)
		}

		// 访问 key2 和 key1，使 key3 成为最少使用的
		c.get("key2")
		c.get("key1")

		// 添加新项，应淘汰 key3
		c.put("key4", testValue("value4"), now()+int64(time.Hour), nil)

		// 验证 key3 被淘汰
		node, found := c.get("key3")
		if found || node != nil {
			t.Fatal("key3应已被淘汰")
		}

		// 其他键应该存在
		for i := range 4 {
			if i == 2 { // key3
				continue
			}
			_, found := c.get(fmt.Sprintf("key%d", i+1))
			if !found {
				t.Fatalf("key%d应存在于缓存中", i+1)
			}
		}
	})

	t.Run("遍历缓存", func(t *testing.T) {
		c := newCache(5)

		// 添加3项（过期时间 > 0 才能被 walk 访问）
		for i := range 3 {
			c.put(fmt.Sprintf("key%d", i+1), testValue(fmt.Sprintf("value%d", i+1)), now()+int64(time.Hour), nil)
		}

		// 遍历并收集所有键
		var keys []string
		c.walk(func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})

		if len(keys) != 3 {
			t.Fatalf("应有3个键，实际有%d个", len(keys))
		}

		// 测试提前终止遍历
		var earlyKeys []string
		c.walk(func(key string, value Value, expireAt int64) bool {
			earlyKeys = append(earlyKeys, key)
			return len(earlyKeys) < 2
		})

		if len(earlyKeys) != 2 {
			t.Fatalf("应有2个键，实际有%d个", len(earlyKeys))
		}
	})
}

func TestCacheWalk(t *testing.T) {
	c := newCache(5)

	c.put("key1", testValue("value1"), now()+int64(time.Hour), nil)
	c.put("key2", testValue("value2"), now()+int64(time.Hour), nil)
	c.put("key3", testValue("value3"), now()+int64(time.Hour), nil)

	// 逻辑删除 key2
	c.delete("key2")

	// walk 只遍历有效项（expireAt > 0）
	var keys []string
	c.walk(func(key string, value Value, expireAt int64) bool {
		keys = append(keys, key)
		return true
	})

	if len(keys) != 2 || keys[0] != "key3" || keys[1] != "key1" {
		t.Errorf("Walk didn't return expected keys, got %v", keys)
	}

	// 测试提前终止
	count := 0
	c.walk(func(key string, value Value, expireAt int64) bool {
		count++
		return false
	})

	if count != 1 {
		t.Errorf("Walk didn't stop early as expected")
	}
}

/*-----------------------------------------------------------------------------*/
// lru2Store 集成测试
/*-----------------------------------------------------------------------------*/

// newTestStore 创建测试用的 store 并等待时钟校准
func newTestStore(buckets, capPer, l2cap uint16) *lru2Store {
	return newLRU2Cache(Option{
		BucketCount:     buckets,
		CapPerBucket:    capPer,
		Level2Cap:       l2cap,
		CleanupInterval: time.Minute,
	})
}

func TestLRU2StoreBasicOperations(t *testing.T) {
	s := newTestStore(4, 64, 64)
	defer s.Close()

	// Set 返回 bool
	ok := s.Set("key1", testValue("value1"))
	if !ok {
		t.Error("Set should return true")
	}

	value, found := s.Get("key1")
	if !found || value != testValue("value1") {
		t.Errorf("Get failed, expected 'value1', got %v, found: %v", value, found)
	}

	// 测试更新已存在的 key — put 对 hmap 中已存在的 key 返回 false
	ok = s.Set("key1", testValue("value1-updated"))
	if ok {
		t.Log("Set update returned true (key was not in hmap)")
	}

	value, found = s.Get("key1")
	if !found || value != testValue("value1-updated") {
		t.Errorf("Get after update failed, got %v", value)
	}

	// 测试不存在的键
	_, found = s.Get("nonexistent")
	if found {
		t.Error("Get nonexistent key should return false")
	}

	// 测试删除
	deleted := s.Delete("key1")
	if !deleted {
		t.Error("Delete should return true")
	}

	_, found = s.Get("key1")
	if found {
		t.Error("Get after delete should return false")
	}

	// 测试删除不存在的键
	deleted = s.Delete("nonexistent")
	if deleted {
		t.Error("Delete nonexistent key should return false")
	}
}

func TestLRU2StoreL1ToL2Upgrade(t *testing.T) {
	s := newTestStore(4, 64, 64)
	defer s.Close()

	// Set 进 L1
	s.Set("key", testValue("value"))

	// 第一次 Get：L1 命中 → 升级到 L2
	value, found := s.Get("key")
	if !found || value != testValue("value") {
		t.Fatalf("first Get should hit L1, got (%v, %v)", value, found)
	}

	// 第二次 Get：应从 L2 命中
	value, found = s.Get("key")
	if !found || value != testValue("value") {
		t.Fatalf("second Get should hit L2, got (%v, %v)", value, found)
	}
}

func TestLRU2StoreExpiration(t *testing.T) {
	s := newTestStore(4, 64, 64)
	defer s.Close()

	// 添加很快过期的项
	s.SetWithExpiration("expires-soon", testValue("value"), 100*time.Millisecond)

	// 添加不过期的项
	s.Set("keeps", testValue("value"))

	// 立即获取都应存在
	_, found := s.Get("expires-soon")
	if !found {
		t.Error("expires-soon should be found initially")
	}

	// 等待过期
	time.Sleep(200 * time.Millisecond)

	// 过期项应返回 false
	_, found = s.Get("expires-soon")
	if found {
		t.Error("expires-soon should have expired")
	}

	// 不过期项仍存在
	_, found = s.Get("keeps")
	if !found {
		t.Error("keeps should still be valid")
	}
}

func TestLRU2StoreClear(t *testing.T) {
	s := newTestStore(2, 64, 64)
	defer s.Close()

	for i := range 10 {
		s.Set(fmt.Sprintf("key%d", i), testValue(fmt.Sprintf("value%d", i)))
	}

	if n := s.Len(); n != 10 {
		t.Errorf("Expected length 10, got %d", n)
	}

	s.Clear()

	if n := s.Len(); n != 0 {
		t.Errorf("Expected length 0 after Clear, got %d", n)
	}

	for i := range 10 {
		_, found := s.Get(fmt.Sprintf("key%d", i))
		if found {
			t.Errorf("key%d should not be found after Clear", i)
		}
	}
}

func TestLRU2StoreConcurrent(t *testing.T) {
	s := newTestStore(8, 128, 128)
	defer s.Close()

	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			prefix := fmt.Sprintf("g%d-", id)

			// Set
			for i := range opsPerGoroutine {
				key := fmt.Sprintf("%s%d", prefix, i)
				s.Set(key, testValue(fmt.Sprintf("value-%d-%d", id, i)))
			}

			// Get
			for i := range opsPerGoroutine {
				key := fmt.Sprintf("%s%d", prefix, i)
				value, found := s.Get(key)
				if !found {
					t.Errorf("Get failed for key %s in goroutine %d", key, id)
					continue
				}
				expected := testValue(fmt.Sprintf("value-%d-%d", id, i))
				if value != expected {
					t.Errorf("Get wrong value for %s: expected %s, got %v", key, expected, value)
				}
			}

			// Delete 一半
			for i := range opsPerGoroutine / 2 {
				key := fmt.Sprintf("%s%d", prefix, i)
				if !s.Delete(key) {
					t.Errorf("Delete failed for key %s", key)
				}
			}
		}(g)
	}

	wg.Wait()
}

/*-----------------------------------------------------------------------------*/
// 辅助
/*-----------------------------------------------------------------------------*/

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
