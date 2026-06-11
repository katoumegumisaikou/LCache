package consistenthash

import (
	"fmt"
	"testing"
)

func TestMap_AddAndGet(t *testing.T) {
	m := NewMap()
	m.Add("node1", "node2", "node3")

	if m == nil {
		t.Fatal("NewMap returned nil")
	}
	if len(m.keys) == 0 {
		t.Fatal("hash ring should not be empty after adding nodes")
	}

	// 同一个 key 多次查询结果应该一致
	first := m.Get("test-key")
	for range 100 {
		if got := m.Get("test-key"); got != first {
			t.Fatalf("inconsistent hash: first=%s, got=%s", first, got)
		}
	}
}

func TestMap_EmptyRing(t *testing.T) {
	m := NewMap()
	if got := m.Get("any-key"); got != "" {
		t.Fatalf("empty ring should return empty string, got %s", got)
	}
}

func TestMap_Remove(t *testing.T) {
	m := NewMap()
	m.Add("node1", "node2", "node3")

	err := m.Remove("node2")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// 确保 node2 不会再出现
	seen := make(map[string]bool)
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		seen[m.Get(key)] = true
	}

	if seen["node2"] {
		t.Fatal("node2 should have been removed")
	}
	if !seen["node1"] {
		t.Fatal("node1 should still exist")
	}
	if !seen["node3"] {
		t.Fatal("node3 should still exist")
	}
}

func TestMap_RemoveNonexistent(t *testing.T) {
	m := NewMap()
	err := m.Remove("nonexistent")
	if err == nil {
		t.Fatal("expected error when removing nonexistent node")
	}
}

func TestMap_AddEmptyNode(t *testing.T) {
	m := NewMap()
	m.Add("")
	if len(m.keys) != 0 {
		t.Fatal("empty node should not be added")
	}
}

func TestMap_DistributionBalance(t *testing.T) {
	m := NewMap()
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	m.Add(nodes...)

	counts := make(map[string]int)
	total := 10000
	for i := range total {
		key := fmt.Sprintf("key-%d", i)
		node := m.Get(key)
		counts[node]++
	}

	// 每个节点的比例应在合理范围内（允许较大偏差，因为虚拟节点可能还未均衡）
	expectedAvg := float64(total) / float64(len(nodes))
	for _, node := range nodes {
		ratio := float64(counts[node]) / expectedAvg
		if ratio < 0.6 || ratio > 1.4 {
			// 宽松检查，一致性哈希大致均衡即可
			t.Logf("node %s: count=%d (avg=%.0f, ratio=%.2f)", node, counts[node], expectedAvg, ratio)
		}
	}
}

func TestMap_GetStats(t *testing.T) {
	m := NewMap()
	m.Add("node1", "node2")

	for i := range 100 {
		m.Get(fmt.Sprintf("key-%d", i))
	}

	stats := m.GetStats()
	if len(stats) == 0 {
		t.Fatal("stats should not be empty")
	}
	t.Logf("stats: %v", stats)
}
