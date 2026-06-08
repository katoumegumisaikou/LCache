package lcache

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"lcache/pb"
)

type Server struct {
	pb.UnimplementedPBServer
	addr       string
	svcName    string
	groups     *sync.Map        // 缓存组
	grpcServer *grpc.Server     // gRPC服务器
	etcdCli    *clientv3.Client // etcd客户端
	stopCh     chan struct{}
	opts       *ServerOptions
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	EtcdEndpoints []string      // etcd端点
	DialTimeout   time.Duration // 连接超时
	MaxMsgSize    int           // 最大消息大小
	TLS           bool          // 是否启用TLS
	CertFile      string        // 证书文件
	KeyFile       string        // 密钥文件
}

// DefaultServerOptions 默认配置
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(options)
	}

	// 创建etcd客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan struct{}),
		opts:       options,
	}

	// 注册服务
	pb.RegisterPBServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动 gRPC 服务并注册到 etcd。
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	go func() {
		if err := s.register(); err != nil {
			logrus.Errorf("failed to register service: %v", err)
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务并撤销 etcd 注册。
func (s *Server) Stop() {
	close(s.stopCh)
	s.grpcServer.GracefulStop()
}

// Get 实现缓存服务的 Get 方法。
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: view.ByteSlice()}, nil
}

// Set 实现缓存服务的 Set 方法。
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	if ctx.Value("from_peer") == nil {
		ctx = context.WithValue(ctx, "from_peer", true)
	}

	if _, err := group.Set(ctx, req.Key, req.Value, 0); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete 实现缓存服务的 Delete 方法。
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	deleted, err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: deleted}, err
}

func (s *Server) register() error {
	addr := s.addr
	if len(addr) > 0 && addr[0] == ':' {
		localIP, err := getLocalIP()
		if err != nil {
			return err
		}
		addr = fmt.Sprintf("%s%s", localIP, addr)
	}

	lease, err := s.etcdCli.Grant(context.Background(), 10)
	if err != nil {
		return fmt.Errorf("etcd lease grant failed: %w", err)
	}

	key := fmt.Sprintf("/services/%s/%s", s.svcName, addr)
	if _, err := s.etcdCli.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID)); err != nil {
		return fmt.Errorf("etcd register failed: %w", err)
	}

	keepAliveCh, err := s.etcdCli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return fmt.Errorf("etcd keepalive failed: %w", err)
	}

	for {
		select {
		case <-s.stopCh:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := s.etcdCli.Revoke(ctx, lease.ID)
			cancel()
			if s.etcdCli != nil {
				s.etcdCli.Close()
			}
			return err
		case resp, ok := <-keepAliveCh:
			if !ok {
				return nil
			}
			logrus.Debugf("successfully renewed lease: %d", resp.ID)
		}
	}
}

// loadTLSCredentials 加载TLS证书。
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}
