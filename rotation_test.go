package nntppool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func TestClientConnectionRotation(t *testing.T) {
	// Mock server that stays open
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	var connectionCount int
	var mu sync.Mutex

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connectionCount++
			mu.Unlock()

			go func(c net.Conn) {
				defer c.Close()
				c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	client := NewClient(1)
	defer client.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	// Provider with very short lifetime
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:         l.Addr().String(),
		MaxConnections:  1,
		MaxConnLifetime: 200 * time.Millisecond,
		ConnFactory:     dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	client.AddProvider(p, ProviderPrimary)

	// Wait enough time for at least 2 rotations
	time.Sleep(600 * time.Millisecond)

	// Trigger a send to force a new connection if the old one died
	// (Though lazy replacement happens in Send, we also have eager replacement logic in the background?)
	// Actually provider.go: Send() triggers addConnection if count < max.
	// When a connection closes (due to lifetime), connCount decreases.
	// So the next Send() should trigger a new connection.

	// Let's force some activity to trigger replacements
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		_ = client.Date(ctx)
		time.Sleep(150 * time.Millisecond)
	}

	mu.Lock()
	count := connectionCount
	mu.Unlock()

	// Initial connect + at least 1-2 rotations
	if count < 2 {
		t.Errorf("expected > 1 connections (rotation), got %d", count)
	}
}
