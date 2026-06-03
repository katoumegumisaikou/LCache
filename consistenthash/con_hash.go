package consistenthash

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Map struct {
	mu sync.RWMutex
	// 配置信息
	config *Config
	// 哈希环
	keys []int
	// 哈希环到节点的映射
	hashMap map[int]string
	// 节点到虚拟节点数量的映射
	nodeReplicas map[string]int
	// 节点负载统计
	nodeCounts map[string]int64
	// 总请求数
	totalRequests int64
	// 平衡定时器
	ticker *time.Ticker
}

// New 创建一致性哈希实例
func New() *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
		ticker:       time.NewTicker(time.Second),
	}

	m.startBalancer() // 启动负载均衡器
	return m
}

func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	hash := int(m.config.HashFunc([]byte(key)))
	// 二分查找
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	count := m.nodeCounts[node]
	m.nodeCounts[node] = count + 1
	atomic.AddInt64(&m.totalRequests, 1)

	return node
}

func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return fmt.Errorf("没节点被添加")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}
		m.addNode(node, m.config.DefaultReplicas)
	}

	sort.Ints(m.keys)
	return nil
}

func (m *Map) Remove(node string) error {
	if node == "" {
		return fmt.Errorf("无效node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("节点 %s 未找到", node)
	}

	for i := range replicas {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := range m.keys {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeCounts, node)
	delete(m.nodeReplicas, node)
	return nil
}

// GetStats 获取负载统计信息
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

// addNode 添加完key，并不会保持keys有序
func (m *Map) addNode(node string, replicas int) {
	for i := range replicas {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

func (m *Map) startBalancer() {
	go func() {
		for range m.ticker.C {
			m.checkAndRebalance()
		}
	}()
}

func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return // 样本太少，不进行调整
	}

	// 计算负载情况
	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
	var maxDiff float64

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}

	// 如果负载不均衡度超过阈值，调整虚拟节点
	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}

// rebalanceNodes 重新平衡节点
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	// 调整每个节点的虚拟节点数量
	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(count) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// 重新添加节点的虚拟节点
			if err := m.Remove(node); err != nil {
				continue // 如果移除失败，跳过这个节点
			}
			m.addNode(node, newReplicas)
		}
	}

	// 重置计数器
	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 重新排序
	sort.Ints(m.keys)
}
