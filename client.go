package lcache

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"lcache/pb"
)

type Client struct {
	addr    string
	svcName string
	conn    *grpc.ClientConn
	grpcCli pb.PBClient
}

var _ Peer = (*Client)(nil)

func NewClient(addr string, svcName string) (*Client, error) {
	var err error

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	grpcClient := pb.NewPBClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

func (c *Client) Get(ctx context.Context, group, key string) ([]byte, error) {

	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("Get 远程调用失败")
	}

	return resp.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {

	_, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("Set 远程调用失败")
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, group, key string) (bool, error) {
	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("Delete 远程调用失败")
	}

	return resp.Value, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
