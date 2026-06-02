package store

import (
	"sync"
	"sync/atomic"
	"time"
)

// 缓存的时钟
// time.Now() 是系统调用，在缓存这种每秒百万级调用的热路径上，它是个瓶颈
var clock = time.Now().Unix()

// 启动协程，每一秒调用一次time.Now矫正计时
func init() {
	go func() {
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// hashBKRD 实现 BKDR 哈希算法，将字符串映射为 int32 哈希值。
// hash * 131 = hash * (128 + 2 + 1) = hash << 7  +  hash << 1  +  hash
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// 将最高位的1通过移位变成连续的1
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}

	// 通过多次右移和按位或操作，将二进制中最高的 1 位右边的所有位都填充为 1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

func now() int64 {
	return atomic.LoadInt64(&clock)
}

type lru2Store struct {
	locks         []sync.Mutex
	caches        [][2]*cache
	onEvicted     func(key string, value Value)
	cleanupTicker *time.Ticker
	mask          int32 // 哈希掩码，将key映射到分片
}

// newLRU2Cache 创建一个 LRU-2 缓存实例。
// 根据 BucketCount 创建对应数量的分片，每个分片内包含一级和二级两个 cache。
// 如果 CleanupInterval > 0，会启动后台协程定期清理过期项。
func newLRU2Cache(opts Options) *lru2Store {
	if opts.BucketCount == 0 {
		opts.BucketCount = 16
	}
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}

	mask := maskOfNextPowOf2(opts.BucketCount)
	s := &lru2Store{
		locks:         make([]sync.Mutex, mask+1),
		caches:        make([][2]*cache, mask+1),
		onEvicted:     opts.OnEvicted,
		cleanupTicker: time.NewTicker(opts.CleanupInterval),
		mask:          int32(mask),
	}

	for i := range s.caches {
		s.caches[i][0] = newCache(opts.CapPerBucket)
		s.caches[i][1] = newCache(opts.Level2Cap)
	}

	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// Clear 清空所有分片的两级缓存，实现 Store 接口。
func (s *lru2Store) Clear() {
	var keys []string

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			// 检查键是否已经收集（避免重复）
			for _, k := range keys {
				if key == k {
					return true
				}
			}
			keys = append(keys, key)
			return true
		})

		s.locks[i].Unlock()
	}

	for _, key := range keys {
		s.Delete(key)
	}
}

func (l *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & l.mask
	l.locks[idx].Lock()
	defer l.locks[idx].Unlock()

	return l.delete(key, idx)
}

func (l *lru2Store) Get(key string) (Value, bool) {
	idx := hashBKRD(key) & l.mask

	currentTime := now()
	l.locks[idx].Lock()
	defer l.locks[idx].Unlock()

	n1, ok, expTime := l.caches[idx][0].delete(key)
	if ok {
		// 一级缓存中已经存在
		if expTime > 0 && currentTime > n1.expireAt {
			// 过期
			l.delete(key, idx)
			return nil, false
		}

		// 升级到二级缓存
		l.caches[idx][1].put(key, n1.value, expTime, l.onEvicted)
		return n1.value, true
	}

	// 查找二级缓存
	n2, ok := l.caches[idx][1].get(key)
	if ok && n2 != nil {
		if currentTime < n2.expireAt {
			return n2.value, true
		} else {
			l.delete(key, idx)
			return nil, false
		}

	}
	return nil, false

}

func (l *lru2Store) Set(key string, value Value) bool {
	return l.SetWithExpiration(key, value, 9999999999999999)
}

func (l *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) bool {
	expireAt := int64(0)
	if expiration > 0 {
		// now() 返回纳秒时间戳，确保 expiration 也是纳秒单位
		expireAt = now() + int64(expiration.Nanoseconds())
	}
	idx := hashBKRD(key) & l.mask

	l.locks[idx].Lock()
	defer l.locks[idx].Unlock()

	return l.caches[idx][0].put(key, value, expireAt, l.onEvicted)
}

func (l *lru2Store) delete(key string, idx int32) bool {
	n1, s1, _ := l.caches[idx][0].delete(key)
	n2, s2, _ := l.caches[idx][1].delete(key)
	deleted := s1 || s2

	if deleted && l.onEvicted != nil {
		if n1 != nil && n1.value != nil {
			l.onEvicted(key, n1.value)
		} else if n2 != nil && n2.value != nil {
			l.onEvicted(key, n2.value)
		}
	}
	return deleted
}

func (l *lru2Store) cleanupLoop() {
	for range l.cleanupTicker.C {
		currentTime := now()

		for i := range l.caches {
			l.locks[i].Lock()

			// 检查并清理过期项目
			var expireKeys []string

			l.caches[i][0].walk(func(key string, value Value, expTime int64) bool {
				if expTime > 0 && expTime < currentTime {
					expireKeys = append(expireKeys, key)
				}
				return true
			})
			l.caches[i][1].walk(func(key string, value Value, expTime int64) bool {
				if expTime > 0 && expTime < currentTime {
					expireKeys = append(expireKeys, key)
				}
				return true
			})

			for _, key := range expireKeys {
				l.delete(key, int32(i))
			}
			l.locks[i].Unlock()
		}
	}
}

/*-----------------------------------------------------------------------------*/

type node struct {
	key      string
	value    Value
	expireAt int64 // 过期时间戳，expireAt = 0 表示已删除
}

const (
	t = 0
	h = 1
)

type cache struct {
	// dlnk[0]是哨兵节点，记录链表头尾，dlnk[0][t]存储尾部索引，dlnk[0][h]存储头部索引
	dlnk [][2]uint16       // 双向链表，0 表示前驱，1 表示后继
	m    []node            // 预分配内存存储节点
	hmap map[string]uint16 // 键到节点索引的映射
	last uint16            // 最后一个节点元素在m的索引
}

func newCache(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1),
		m:    make([]node, cap+1),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

func (c *cache) get(key string) (*node, bool) {
	if idx, exist := c.hmap[key]; exist {
		c.adjust(idx, true)
		return &c.m[idx], true
	}
	return nil, false
}

func (c *cache) adjust(idx uint16, isHead bool) {
	// 修改前后对象指向
	pre := c.dlnk[idx][0]
	next := c.dlnk[idx][1]
	c.dlnk[pre][1] = next
	c.dlnk[next][0] = pre

	if isHead {
		// 将idx放到链表头部i
		c.dlnk[idx][0] = 0
		c.dlnk[idx][1] = c.dlnk[0][h]
		c.dlnk[c.dlnk[idx][1]][0] = idx
		c.dlnk[0][h] = idx
		return
	}

	// 调整到尾部
	tail := c.dlnk[0][t]
	c.dlnk[tail][1] = idx
	c.dlnk[idx][0] = tail
	c.dlnk[idx][1] = 0
	c.dlnk[0][t] = idx

}

// delete 逻辑删除一个缓存项：将 expireAt 置 0 标记为已删除，并移到链表尾部等待被覆盖。
// 返回节点指针、状态码（1=成功, 0=未找到或已删除）和原始过期时间。
func (c *cache) delete(key string) (*node, bool, int64) {
	if idx, exist := c.hmap[key]; exist {
		e := c.m[idx].expireAt
		c.m[idx].expireAt = 0
		c.adjust(idx, false)
		return &c.m[idx], true, e
	}
	return nil, false, 0
}

// key 不存在则新增（返回 false）。当数组已满时，覆盖 LRU 尾部节点。
func (c *cache) put(key string, value Value, expireAt int64, onEvicted func(string, Value)) bool {

	// 已经存在的数据
	idx, exist := c.hmap[key]
	if exist {
		c.m[idx].value = value
		c.m[idx].expireAt = expireAt
		c.adjust(idx, true)
		return false
	}

	if c.last == uint16(cap(c.m))-1 {
		// 缓存已满
		tail := c.dlnk[0][0]
		n := &c.m[tail]
		if onEvicted != nil && n.expireAt > 0 {
			onEvicted(n.key, n.value)
		}

		delete(c.hmap, n.key)
		c.hmap[key], n.key, n.value, n.expireAt = tail, key, value, expireAt
		c.adjust(tail, true)

	} else {
		// 缓存未满
		newNode := node{
			key:      key,
			value:    value,
			expireAt: expireAt,
		}

		c.last++
		c.m[c.last] = newNode
		c.hmap[key] = c.last
		c.dlnk[c.dlnk[0][1]][0] = c.last
		c.dlnk[c.last][1] = c.dlnk[0][1]
		c.dlnk[c.last][0] = 0
		c.dlnk[0][1] = c.last
		return true
	}
	return true
}

// 遍历有效的数据
func (c *cache) walk(f func(key string, value Value, expTime int64) bool) {
	for i := c.dlnk[0][1]; i != 0; i = c.dlnk[i][1] {
		if val := c.m[i]; val.expireAt > 0 && !f(val.key, val.value, val.expireAt) {
			return
		}
	}
}

func (c *cache) testOrder() []node {
	pos := c.dlnk[0][1]
	slice := make([]node, 0, c.last+1)
	for pos != 0 {
		slice = append(slice, c.m[pos])
		pos = c.dlnk[pos][1]
	}
	return slice
}
