package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"lcache"
)

func main() {
	addr := flag.String("addr", ":8001", "grpc listen address")
	service := flag.String("service", "lcache", "service name registered in etcd")
	group := flag.String("group", "default", "cache group name")
	cacheBytes := flag.Int64("cache-bytes", 64<<20, "max cache size in bytes")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "comma-separated etcd endpoints")
	dialTimeout := flag.Duration("dial-timeout", 5*time.Second, "etcd dial timeout")
	expiration := flag.Duration("expiration", 0, "cache item expiration, 0 means no expiration")
	flag.Parse()

	picker := lcache.NewClientPicker(*addr, lcache.WithServiceName(*service))
	if picker == nil {
		logrus.Warn("failed to create peer picker, running in standalone mode (no peer discovery)")
	}

	lcache.NewGroup(
		*group,
		*cacheBytes,
		lcache.GetterFunc(loadFromSource),
		lcache.WithEpiration(*expiration),
		lcache.WithPeers(picker),
	)

	server, err := lcache.NewServer(
		*addr,
		*service,
		lcache.WithEtcdEndpoints(splitEndpoints(*etcdEndpoints)),
		lcache.WithDialTimeout(*dialTimeout),
	)
	if err != nil {
		logrus.Fatalf("create server failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	logrus.Infof("lcache server is running at %s, group=%s", *addr, *group)

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stopCh:
		logrus.Infof("received signal %s, stopping server", sig)
		server.Stop()
	case err := <-errCh:
		if err != nil {
			logrus.Fatalf("server stopped with error: %v", err)
		}
	}
}

func splitEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		endpoint := strings.TrimSpace(part)
		if endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		return []string{"localhost:2379"}
	}
	return endpoints
}

func loadFromSource(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("load cancelled: %w", err)
	}
	return []byte("value:" + strconv.Quote(key)), nil
}
