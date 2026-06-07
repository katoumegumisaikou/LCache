// peers.go 这个文件定义了 PeerPicker 选择器的接口和 Peer 接口，并定义了 ClientPicker 去实现 PeerPicker 接口
// ClientPicker 结构体实现了监听 etcd 的变化，动态维护哈希环
package lcache

import (
	"context"
	"fmt"
	"net"
	"time"

	"sync"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"

	"lcache/consistenthash"
)

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

type Peer interface {
	Get(ctx context.Context, group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context, group string, key string) (bool, error)
	Close() error
}

// DefaultConfig 提供默认配置
var (
	DefaultEndpoints   = []string{"localhost:2379"}
	DefaultDialTimeout = 5 * time.Second
)

// ClientPicker 实现了PeerPicker接口
type ClientPicker struct {
	selfAddr string
	svcName  string
	mu       sync.RWMutex
	consHash *consistenthash.Map
	clients  map[string]Peer
	etcdCli  *clientv3.Client
	ctx      context.Context
	cancel   context.CancelFunc
}

// PickerOption 定义配置选项
type PickerOption func(*ClientPicker)

// WithServiceName 设置服务名称
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

func NewClientPicker(addr string, opts ...PickerOption) *ClientPicker {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		consHash: consistenthash.NewMap(),
		ctx:      ctx,
		cancel:   cancel,
		clients:  make(map[string]Peer),
	}

	for _, opt := range opts {
		opt(picker)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   DefaultEndpoints,
		DialTimeout: DefaultDialTimeout,
	})
	if err != nil {
		logrus.Error(err.Error())
		return nil
	}

	picker.etcdCli = cli

	// 启动服务发现
	if err := picker.startServer(); err != nil {

	}

	return picker
}

func (p *ClientPicker) startServer() error {
	err := p.fetchAllServices()
	if err != nil {
		return err
	}

	// 启动增量更新
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done():
			watcher.Close()
			return
		case resp := <-watchChan:
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		addr := string(event.Kv.Value)
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.clients[addr]; !exists {
				p.addNode(addr)
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			if client, exists := p.clients[addr]; exists {
				client.Close()
				p.remove(addr)
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("获取服务节点失败:%w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, v := range resp.Kvs {
		addr := string(v.Value)
		if addr != "" && addr != p.selfAddr {
			p.addNode(addr)
		}
	}

	return nil
}

func (c *ClientPicker) addNode(addr string) error {
	if client, err := NewClient(addr, c.svcName); err == nil {
		c.consHash.Add(addr)
		c.clients[addr] = client
		return nil
	} else {
		return err
	}
}

// 不需要额外加锁
func (c *ClientPicker) remove(addr string) {
	delete(c.clients, addr)
	c.consHash.Remove(addr)
}

func (c *ClientPicker) Register(stopCh <-chan struct{}) error {
	localIP, err := getLocalIP()
	if err != nil {
		return fmt.Errorf("本地 IP 地址获取失败")
	}
	if c.selfAddr[0] == ':' {
		c.selfAddr = fmt.Sprintf("%s%s", localIP, c.selfAddr)
	}

	// 创建租约
	lease, err := c.etcdCli.Grant(context.Background(), 10)
	if err != nil {
		c.etcdCli.Close()
		return fmt.Errorf("etcd 创建租约失败")
	}

	// 注册服务，使用完整的key路径
	key := fmt.Sprintf("/services/%s/%s", c.svcName, c.selfAddr)
	_, err = c.etcdCli.Put(context.Background(), key, c.selfAddr, clientv3.WithLease(lease.ID))
	if err != nil {
		c.etcdCli.Close()
		return fmt.Errorf("向 etcd 推送失败: %v", err)
	}

	// 保持租约
	keepAliveCh, err := c.etcdCli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		c.etcdCli.Close()
		return fmt.Errorf("续约失败: %v", err)
	}

	// 处理租约续期和服务注销
	go func() {
		defer c.etcdCli.Close()
		for {
			select {
			case <-stopCh:
				// 服务注销，撤销租约
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				c.etcdCli.Revoke(ctx, lease.ID)
				cancel()
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	return nil

}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}
