// lcache-test 测试分布式缓存的跨节点数据获取
package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"lcache/pb"
)

func main() {
	fmt.Println("=== lcache 分布式缓存跨节点测试 ===\n")

	nodes := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}

	clients := make(map[string]pb.PBClient)
	for _, addr := range nodes {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		)
		if err != nil {
			fmt.Printf("❌ 连接 %s 失败: %v\n", addr, err)
			continue
		}
		defer conn.Close()
		clients[addr] = pb.NewPBClient(conn)
		fmt.Printf("✅ 已连接节点: %s\n", addr)
	}

	group := "default"
	ctx := context.Background()

	// ============ 测试1: 各节点独立的 Set/Get ============
	fmt.Println("\n--- 测试1: 每个节点独立 Set/Get ---")
	testKeys := []string{"key-node1", "key-node2", "key-node3"}
	for i, addr := range nodes {
		cli, ok := clients[addr]
		if !ok {
			continue
		}
		key := testKeys[i]
		value := []byte(fmt.Sprintf("value-from-%s", addr))

		_, err := cli.Set(ctx, &pb.Request{Group: group, Key: key, Value: value})
		if err != nil {
			fmt.Printf("❌ Set 失败 [%s] key=%s: %v\n", addr, key, err)
			continue
		}
		fmt.Printf("📝 Set [%s] key=%s value=%s\n", addr, key, string(value))

		time.Sleep(100 * time.Millisecond)

		resp, err := cli.Get(ctx, &pb.Request{Group: group, Key: key})
		if err != nil {
			fmt.Printf("❌ Get 失败 [%s] key=%s: %v\n", addr, key, err)
			continue
		}
		fmt.Printf("🔍 Get [%s] key=%s → %s\n", addr, key, string(resp.Value))
	}

	// ============ 测试2: 跨节点数据获取 (Set on node1, Get from all) ============
	fmt.Println("\n--- 测试2: 跨节点数据获取 (Set on node1, Get from all nodes) ---")
	crossKey := "cross-node-key"
	crossValue := []byte("cross-node-value-from-8001")

	cli1 := clients["127.0.0.1:8001"]
	_, err := cli1.Set(ctx, &pb.Request{Group: group, Key: crossKey, Value: crossValue})
	if err != nil {
		fmt.Printf("❌ Set 失败: %v\n", err)
	} else {
		fmt.Printf("📝 Set [%s] key=%s value=%s\n", "127.0.0.1:8001", crossKey, string(crossValue))
	}

	// 等待异步同步完成
	time.Sleep(500 * time.Millisecond)

	for _, addr := range nodes {
		cli, ok := clients[addr]
		if !ok {
			continue
		}
		resp, err := cli.Get(ctx, &pb.Request{Group: group, Key: crossKey})
		if err != nil {
			fmt.Printf("❌ Get 失败 [%s] key=%s: %v\n", addr, crossKey, err)
			continue
		}
		fmt.Printf("🔍 Get [%s] key=%s → %s\n", addr, crossKey, string(resp.Value))
	}

	// ============ 测试3: Set on node2, Get from node3 ============
	fmt.Println("\n--- 测试3: Set on node2, Get from node3 ---")
	crossKey2 := "peer-to-peer-key"
	crossValue2 := []byte("value-set-on-node2")

	cli2 := clients["127.0.0.1:8002"]
	_, err = cli2.Set(ctx, &pb.Request{Group: group, Key: crossKey2, Value: crossValue2})
	if err != nil {
		fmt.Printf("❌ Set 失败: %v\n", err)
	} else {
		fmt.Printf("📝 Set [%s] key=%s value=%s\n", "127.0.0.1:8002", crossKey2, string(crossValue2))
	}

	time.Sleep(500 * time.Millisecond)

	// 从 node3 获取
	cli3 := clients["127.0.0.1:8003"]
	resp3, err := cli3.Get(ctx, &pb.Request{Group: group, Key: crossKey2})
	if err != nil {
		fmt.Printf("❌ Get 失败 [%s] key=%s: %v\n", "127.0.0.1:8003", crossKey2, err)
	} else {
		fmt.Printf("🔍 Get [%s] key=%s → %s\n", "127.0.0.1:8003", crossKey2, string(resp3.Value))
	}

	// 也从 node1 获取
	resp1, err := cli1.Get(ctx, &pb.Request{Group: group, Key: crossKey2})
	if err != nil {
		fmt.Printf("❌ Get 失败 [%s] key=%s: %v\n", "127.0.0.1:8001", crossKey2, err)
	} else {
		fmt.Printf("🔍 Get [%s] key=%s → %s\n", "127.0.0.1:8001", crossKey2, string(resp1.Value))
	}

	// ============ 测试4: 测试不存在的key（回退到getter） ============
	fmt.Println("\n--- 测试4: 获取不存在的key（触发 getter 回源） ---")
	unknownKey := "key-that-does-not-exist"
	for _, addr := range nodes {
		cli, ok := clients[addr]
		if !ok {
			continue
		}
		resp, err := cli.Get(ctx, &pb.Request{Group: group, Key: unknownKey})
		if err != nil {
			fmt.Printf("❌ Get 失败 [%s] key=%s: %v\n", addr, unknownKey, err)
			continue
		}
		fmt.Printf("🔍 Get [%s] key=%s → %s (来自 getter 回源)\n", addr, unknownKey, string(resp.Value))
	}

	// ============ 测试5: Delete 操作 ============
	fmt.Println("\n--- 测试5: Delete 和 Get 验证 ---")
	delKey := "key-to-delete"
	delValue := []byte("will-be-deleted")

	_, err = cli1.Set(ctx, &pb.Request{Group: group, Key: delKey, Value: delValue})
	if err != nil {
		fmt.Printf("❌ Set 失败: %v\n", err)
	} else {
		fmt.Printf("📝 Set [%s] key=%s value=%s\n", "127.0.0.1:8001", delKey, string(delValue))
	}

	time.Sleep(200 * time.Millisecond)

	// 删除
	_, err = cli1.Delete(ctx, &pb.Request{Group: group, Key: delKey})
	if err != nil {
		fmt.Printf("❌ Delete 失败: %v\n", err)
	} else {
		fmt.Printf("🗑️  Delete [%s] key=%s 成功\n", "127.0.0.1:8001", delKey)
	}

	time.Sleep(200 * time.Millisecond)

	// 删除后再次获取（会回退到 getter）
	resp, err := cli1.Get(ctx, &pb.Request{Group: group, Key: delKey})
	if err != nil {
		fmt.Printf("❌ Get 失败: %v\n", err)
	} else {
		fmt.Printf("🔍 Get [%s] key=%s → %s (删除后回源)\n", "127.0.0.1:8001", delKey, string(resp.Value))
	}

	fmt.Println("\n=== 测试完成 ===")
}
