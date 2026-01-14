package nntppool

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// mockSOCKSProxy implements a minimal SOCKS5 proxy server for testing
type mockSOCKSProxy struct {
	listener   net.Listener
	mu         sync.Mutex
	targetAddr string
	auth       *struct {
		username string
		password string
	}
	dialFunc func(network, address string) (net.Conn, error)
}

func newMockSOCKSProxy(targetAddr string) (*mockSOCKSProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	proxy := &mockSOCKSProxy{
		listener:   listener,
		targetAddr: targetAddr,
		dialFunc:   net.Dial,
	}

	go proxy.serve()

	return proxy, nil
}

func (p *mockSOCKSProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *mockSOCKSProxy) Close() error {
	return p.listener.Close()
}

func (p *mockSOCKSProxy) SetAuth(username, password string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.auth = &struct {
		username string
		password string
	}{username: username, password: password}
}

func (p *mockSOCKSProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handleConnection(conn)
	}
}

func (p *mockSOCKSProxy) handleConnection(client net.Conn) {
	defer func() {
		_ = client.Close()
	}()

	buf := make([]byte, 512)

	// Read SOCKS5 greeting
	n, err := client.Read(buf)
	if err != nil || n < 2 {
		return
	}

	version := buf[0]
	if version != 5 {
		return
	}

	// Check if authentication is required
	p.mu.Lock()
	requireAuth := p.auth != nil
	p.mu.Unlock()

	if requireAuth {
		// Respond: version 5, auth method 2 (username/password)
		if _, err := client.Write([]byte{5, 2}); err != nil {
			return
		}

		// Read auth request
		n, err = client.Read(buf)
		if err != nil || n < 2 {
			return
		}

		authVersion := buf[0]
		if authVersion != 1 {
			return
		}

		usernameLen := int(buf[1])
		if n < 2+usernameLen+1 {
			return
		}

		username := string(buf[2 : 2+usernameLen])
		passwordLen := int(buf[2+usernameLen])
		if n < 2+usernameLen+1+passwordLen {
			return
		}

		password := string(buf[2+usernameLen+1 : 2+usernameLen+1+passwordLen])

		p.mu.Lock()
		validAuth := p.auth.username == username && p.auth.password == password
		p.mu.Unlock()

		if !validAuth {
			// Auth failed
			_, _ = client.Write([]byte{1, 1})
			return
		}

		// Auth success
		if _, err := client.Write([]byte{1, 0}); err != nil {
			return
		}
	} else {
		// Respond: version 5, no auth required
		if _, err := client.Write([]byte{5, 0}); err != nil {
			return
		}
	}

	// Read connection request
	n, err = client.Read(buf)
	if err != nil || n < 10 {
		return
	}

	// Parse request
	if buf[0] != 5 || buf[1] != 1 { // version 5, CONNECT command
		_, _ = client.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}) // command not supported
		return
	}

	// Connect to target
	target, err := p.dialFunc("tcp", p.targetAddr)
	if err != nil {
		_, _ = client.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0}) // connection refused
		return
	}
	defer func() {
		_ = target.Close()
	}()

	// Send success response
	if _, err := client.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	// Relay data between client and target
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(target, client)
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, target)
	}()

	wg.Wait()
}

func TestProviderWithSOCKSProxy(t *testing.T) {
	// Create a mock NNTP server using testutil
	mockHandler := func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "DATE") {
			return "111 20260114120000\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			return "222 0 <test@example.com>\r\nTest body content\r\n.\r\n", nil
		}
		return "500 Unknown command\r\n", nil
	}

	nntpServer, stopServer := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: mockHandler,
	})
	defer stopServer()

	// Create SOCKS proxy
	proxy, err := newMockSOCKSProxy(nntpServer.Addr())
	if err != nil {
		t.Fatalf("failed to create SOCKS proxy: %v", err)
	}
	defer func() {
		if err := proxy.Close(); err != nil {
			t.Errorf("failed to close proxy: %v", err)
		}
	}()

	// Test 1: Connect through proxy without authentication
	t.Run("NoAuth", func(t *testing.T) {
		proxyURL := fmt.Sprintf("socks5://%s", proxy.Addr())

		provider, err := NewProvider(context.Background(), ProviderConfig{
			Address:               nntpServer.Addr(),
			MaxConnections:        1,
			InitialConnections:    1,
			InflightPerConnection: 1,
			ProxyURL:              proxyURL,
		})
		if err != nil {
			t.Fatalf("failed to create provider with proxy: %v", err)
		}
		defer func() {
			if err := provider.Close(); err != nil {
				t.Errorf("failed to close provider: %v", err)
			}
		}()

		// Test DATE command through proxy
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = provider.Date(ctx)
		if err != nil {
			t.Errorf("DATE command failed through proxy: %v", err)
		}
	})

	// Test 2: Connect through proxy with authentication
	t.Run("WithAuth", func(t *testing.T) {
		// Create a new proxy with auth enabled
		authProxy, err := newMockSOCKSProxy(nntpServer.Addr())
		if err != nil {
			t.Fatalf("failed to create SOCKS proxy: %v", err)
		}
		defer func() {
			if err := authProxy.Close(); err != nil {
				t.Errorf("failed to close authProxy: %v", err)
			}
		}()
		authProxy.SetAuth("testuser", "testpass")

		proxyURL := fmt.Sprintf("socks5://testuser:testpass@%s", authProxy.Addr())

		provider, err := NewProvider(context.Background(), ProviderConfig{
			Address:               nntpServer.Addr(),
			MaxConnections:        1,
			InitialConnections:    1,
			InflightPerConnection: 1,
			ProxyURL:              proxyURL,
		})
		if err != nil {
			t.Fatalf("failed to create provider with auth proxy: %v", err)
		}
		defer func() {
			if err := provider.Close(); err != nil {
				t.Errorf("failed to close provider: %v", err)
			}
		}()

		// Test DATE command through proxy with auth
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = provider.Date(ctx)
		if err != nil {
			t.Errorf("DATE command failed through auth proxy: %v", err)
		}
	})

	// Test 3: Invalid proxy URL should return error
	t.Run("InvalidProxyURL", func(t *testing.T) {
		_, err := NewProvider(context.Background(), ProviderConfig{
			Address:               nntpServer.Addr(),
			MaxConnections:        1,
			InitialConnections:    1,
			InflightPerConnection: 1,
			ProxyURL:              "invalid://proxy",
		})
		if err == nil {
			t.Error("expected error for invalid proxy URL")
		}
		if !strings.Contains(err.Error(), "unsupported proxy scheme") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	// Test 4: Test multiple commands through proxy
	t.Run("MultipleCommands", func(t *testing.T) {
		proxyURL := fmt.Sprintf("socks5://%s", proxy.Addr())

		provider, err := NewProvider(context.Background(), ProviderConfig{
			Address:               nntpServer.Addr(),
			MaxConnections:        1,
			InitialConnections:    1,
			InflightPerConnection: 1,
			ProxyURL:              proxyURL,
		})
		if err != nil {
			t.Fatalf("failed to create provider with proxy: %v", err)
		}
		defer func() {
			if err := provider.Close(); err != nil {
				t.Errorf("failed to close provider: %v", err)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Test multiple DATE commands through the same proxy connection
		for i := 0; i < 3; i++ {
			err = provider.Date(ctx)
			if err != nil {
				t.Errorf("DATE command %d failed through proxy: %v", i+1, err)
			}
		}
	})
}

func TestProviderProxyAndConnFactoryConflict(t *testing.T) {
	// Test that ConnFactory takes precedence over ProxyURL
	called := false
	customFactory := func(ctx context.Context) (net.Conn, error) {
		called = true
		return nil, fmt.Errorf("custom factory called")
	}

	_, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "example.com:119",
		MaxConnections:        1,
		InitialConnections:    1,
		InflightPerConnection: 1,
		ProxyURL:              "socks5://proxy:1080",
		ConnFactory:           customFactory,
	})

	// Should fail because custom factory returns error, but it should have been called
	if err == nil {
		t.Error("expected error from custom factory")
	}

	if !called {
		t.Error("custom ConnFactory should take precedence over ProxyURL")
	}
}
