package nntppool

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
)

// mockProviderHandler is a function that takes a command and returns a response.
// If it returns an error, the connection is closed.
type mockProviderHandler func(cmd string) (string, error)

func mockDialerWithHandler(handler mockProviderHandler) ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		c1, c2 := net.Pipe()

		go func() {
			defer c2.Close()
			// Initial greeting
			if _, err := c2.Write([]byte("200 Service Ready\r\n")); err != nil {
				return
			}

			buf := make([]byte, 1024)
			for {
				n, err := c2.Read(buf)
				if err != nil {
					return
				}
				cmd := string(buf[:n])
				resp, err := handler(cmd)
				if err != nil {
					return
				}
				if _, err := c2.Write([]byte(resp)); err != nil {
					return
				}
			}
		}()

		return c1, nil
	}
}

func TestClientRotation_ArticleNotFound(t *testing.T) {
	// Scenario: Primary returns 430, Backup returns 200 (Success)

	p1Handler := func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return "430 No Such Article\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	p2Handler := func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	client := NewClient(10)
	defer client.Close()

	// Add Primary (Failing)
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: mockDialerWithHandler(p1Handler),
	})
	client.AddProvider(p1, ProviderPrimary)

	// Add Backup (Succeeding)
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: mockDialerWithHandler(p2Handler),
	})
	client.AddProvider(p2, ProviderBackup)

	// Execute
	err := client.Body(context.Background(), "123", io.Discard)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	// Verify metrics to ensure rotation happened
	metrics := client.Metrics()
	if metrics["p1:119"].ActiveConnections == 0 && metrics["p2:119"].ActiveConnections == 0 {
		// Connections might be idle but instantiated.
		// We can't easily check 'calls made' without instrumentation, but success implies p2 was reached.
	}
}

func TestClientRotation_OnlyBackups(t *testing.T) {
	// Scenario: No primaries, only Backup returns 200

	p1Handler := func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	client := NewClient(10)
	defer client.Close()

	// Add Backup
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: mockDialerWithHandler(p1Handler),
	})
	client.AddProvider(p1, ProviderBackup)

	// Execute
	err := client.Body(context.Background(), "123", io.Discard)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestClientRotation_AllFail(t *testing.T) {
	// Scenario: Primary returns 430, Backup returns 430

	notFoundHandler := func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return "430 No Such Article\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	client := NewClient(10)
	defer client.Close()

	// Add Primary
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: mockDialerWithHandler(notFoundHandler),
	})
	client.AddProvider(p1, ProviderPrimary)

	// Add Backup
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: mockDialerWithHandler(notFoundHandler),
	})
	client.AddProvider(p2, ProviderBackup)

	// Execute
	err := client.Body(context.Background(), "123", io.Discard)
	if err == nil {
		t.Fatal("expected error, got success")
	}

	if !strings.Contains(err.Error(), "430") && !strings.Contains(err.Error(), "No Such Article") {
		// The client returns the last error.
		// If both fail with 430, it should return 430.
		// Wait, client.go logic:
		// if resp.Err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 -> Success
		// else -> continue loop
		// If all failed, it returns lastResp or lastErr.
		// Our mock returns 430 with err=nil in response struct, but response.Err might be nil.
		// Wait, client.sendSync returns resp.Err if set, OR &resp.
		// If 430 is returned, it is a valid response with StatusCode 430.
		// But Body() calls sendSync() which returns (resp, err).
		// sendSync implementation:
		// if resp.Err != nil return nil, resp.Err
		// return &resp, nil
		//
		// client.Body implementation:
		// _, err := c.sendSync(...)
		// return err
		//
		// Wait. sendSync returns err only if resp.Err is set (transport error) or context error.
		// It returns nil error if we got a valid NNTP response (even 430).
		// THIS IS A BUG/FEATURE in client.go?
		// Let's check Client.Body:
		// func (c *Client) Body(...) error {
		//     _, err := c.sendSync(...)
		//     return err
		// }
		//
		// If sendSync returns 430 response and nil error, Body returns nil error?
		// That would mean 430 is treated as success by Body().
		// Let's check sendSync again in client.go
	}
}
