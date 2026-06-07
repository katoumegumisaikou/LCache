package lcache

import (
	"context"
	"fmt"
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
	Get(ctx context.Context,group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context,group string, key string) (bool, error)
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
	if err:=picker{

	}

	return picker
}

func (p*ClientPicker)startServer()error{

}

func (p*ClientPicker)fetchAllServices()error{
	ctx,cancel:=context.WithTimeout(p.ctx,3*time.Second)
	defer cancel()

	resp,err:=p.etcdCli.Get(ctx,"/services/"+p.svcName,clientv3.WithPrefix())
	if err!=nil{
		return fmt.Errorf("获取服务节点失败:%w",err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _,v:=range resp.Kvs{
		addr:=string(v.Value)
		if addr!=""&&addr!=p.selfAddr{
			
		}
	}
	
	return nil
}

func (c *ClientPicker) Register(stopCh <-chan struct{}) error {

}
