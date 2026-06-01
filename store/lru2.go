package store

import (
	"sync"
	"time"
)

// 缓存的时钟
// time.Now() 是系统调用，在缓存这种每秒百万级调用的热路径上，它是个瓶颈
var clock=time.Now().UnixNano()

func

type lru2Store struct {
	locks       []sync.Mutex
	caches      [][2]*cache
	onEvicted   func(key string, value Value)
	cleanupTicker *time.Ticker
	mask        int32 // 哈希掩码，将key映射到分片
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
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTicker: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
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

func (l*lru2Store)cleanupLoop(){
	for range l.cleanupTicker.C{
		currentTime:=
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
func (c *cache) delete(key string) (*node, int, int64) {
	if idx, exist := c.hmap[key]; exist {
		e := c.m[idx].expireAt
		c.m[idx].expireAt = 0
		c.adjust(idx, false)
		return &c.m[idx], 1, e
	}
	return nil, 0, 0
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
		if onEvicted != nil {
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
func (c *cache) walker(f func(key string, value Value, expTime int64) bool) {
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
