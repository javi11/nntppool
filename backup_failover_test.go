package nntppool

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// articleProviderFactory returns a ConnFactory for a healthy server that has
// every article: STAT answers 223 after statDelay, BODY answers at once with
// a small yEnc body. bodyCalls counts the BODY commands it received.
func articleProviderFactory(statDelay time.Duration, bodyCalls *atomic.Int32) ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			var wmu sync.Mutex
			buf := make([]byte, 4096)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				cmd := strings.TrimRight(string(buf[:n]), "\r\n")
				switch {
				case strings.HasPrefix(cmd, "DATE"):
					wmu.Lock()
					_, _ = server.Write([]byte("111 20240315120000\r\n"))
					wmu.Unlock()
				case strings.HasPrefix(cmd, "STAT"):
					go func() {
						time.Sleep(statDelay)
						wmu.Lock()
						defer wmu.Unlock()
						_, _ = server.Write([]byte("223 0 <test@host> article exists\r\n"))
					}()
				case strings.HasPrefix(cmd, "BODY"):
					bodyCalls.Add(1)
					wmu.Lock()
					_, _ = server.Write(yencSinglePart([]byte("real article data"), "f.bin"))
					wmu.Unlock()
				default:
					wmu.Lock()
					_, _ = fmt.Fprintf(server, "500 unknown\r\n")
					wmu.Unlock()
				}
			}
		}()
		return client, nil
	}
}

// A backup that lacks the article says nothing about the next backup: with
// sequential failover the second backup must still be asked.
func TestSequentialFailoverAsksNextBackupAfterMiss(t *testing.T) {
	var mu sync.Mutex
	var mainCmds, backup1Cmds []string
	var backup2Body atomic.Int32
	missing := map[string]string{"BODY": "430 no such article", "STAT": "430 no such article"}
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &mainCmds, missing), Connections: 1},
		{Factory: makeStatProbeFactory(t, &mu, &backup1Cmds, missing), Connections: 1, Backup: true},
		{Factory: articleProviderFactory(0, &backup2Body), Connections: 1, Backup: true},
	}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	resp := <-c.Send(context.Background(), SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 || backup2Body.Load() != 1 {
		t.Fatalf("status = %d (err %v), BODY calls on the second backup = %d; want 222 from the second backup after the first backup's 430",
			resp.StatusCode, resp.Err, backup2Body.Load())
	}
}

// When every provider lacks the article the request still ends in a prompt 430.
func TestSequentialFailoverAllMissingIsA430(t *testing.T) {
	var mu sync.Mutex
	var mainCmds, backup1Cmds, backup2Cmds []string
	missing := map[string]string{"BODY": "430 no such article", "STAT": "430 no such article"}
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &mainCmds, missing), Connections: 1},
		{Factory: makeStatProbeFactory(t, &mu, &backup1Cmds, missing), Connections: 1, Backup: true},
		{Factory: makeStatProbeFactory(t, &mu, &backup2Cmds, missing), Connections: 1, Backup: true},
	}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	resp := <-c.Send(context.Background(), SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 430 {
		t.Fatalf("status = %d (err %v), want 430 when no provider has the article", resp.StatusCode, resp.Err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a miss on every provider took %s, want under 1s", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(backup2Cmds) == 0 {
		t.Fatalf("second backup was never asked; commands: main=%v backup1=%v", mainCmds, backup1Cmds)
	}
}
