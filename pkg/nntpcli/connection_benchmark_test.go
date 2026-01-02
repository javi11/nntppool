package nntpcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2/pkg/nntpcli/test"
	"github.com/mnightingale/rapidyenc"
)

const (
	largeFixturePath = "test/fixtures/large.yenc"
	largeFixtureSize = 100 * 1024 // 100KB decoded size
)

// generateLargeFixture creates a large yenc file for benchmarking.
// It generates 100KB of pattern data and encodes it as yenc.
func generateLargeFixture() error {
	// Check if fixture already exists
	if _, err := os.Stat(largeFixturePath); err == nil {
		return nil // Already exists
	}

	// Generate pattern data (100KB)
	data := make([]byte, largeFixtureSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Encode with rapidyenc using NewEncoder (handles headers/footers automatically)
	var buf bytes.Buffer
	meta := rapidyenc.Meta{
		FileName:   "benchmark_data",
		FileSize:   largeFixtureSize,
		PartNumber: 1,
		TotalParts: 1,
		PartSize:   largeFixtureSize,
	}

	enc, err := rapidyenc.NewEncoder(&buf, meta)
	if err != nil {
		return fmt.Errorf("create encoder failed: %w", err)
	}

	if _, err := enc.Write(data); err != nil {
		return fmt.Errorf("encode failed: %w", err)
	}

	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder failed: %w", err)
	}

	// Write to file
	if err := os.WriteFile(largeFixturePath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("write file failed: %w", err)
	}

	return nil
}

// benchmarkSetup creates a server and connection for benchmarking.
// It returns a cleanup function that must be called when done.
func benchmarkSetup(b *testing.B, fixtureFile string) (Connection, func()) {
	b.Helper()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	s, err := test.NewServer()
	if err != nil {
		b.Fatalf("failed to create server: %v", err)
	}

	port := s.Port()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Serve(ctx)
	}()

	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		cancel()
		s.Close()
		wg.Wait()
		b.Fatalf("failed to dial: %v", err)
	}

	conn, err := newConnection(netConn, time.Now().Add(time.Hour), configDefault.OperationTimeout)
	if err != nil {
		cancel()
		s.Close()
		wg.Wait()
		b.Fatalf("failed to create connection: %v", err)
	}

	if err := conn.JoinGroup("misc.test"); err != nil {
		conn.Close()
		cancel()
		s.Close()
		wg.Wait()
		b.Fatalf("failed to join group: %v", err)
	}

	// Post the test article
	encoded, err := os.ReadFile(fixtureFile)
	if err != nil {
		conn.Close()
		cancel()
		s.Close()
		wg.Wait()
		b.Fatalf("failed to read fixture: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(examplepost)
	buf.Write(encoded)

	if _, err := conn.Post(buf); err != nil {
		conn.Close()
		cancel()
		s.Close()
		wg.Wait()
		b.Fatalf("failed to post article: %v", err)
	}

	cleanup := func() {
		conn.Close()
		cancel()
		s.Close()
		wg.Wait()
	}

	return conn, cleanup
}

func BenchmarkBodyDecoded_Small(b *testing.B) {
	conn, cleanup := benchmarkSetup(b, "test/fixtures/test.yenc")
	defer cleanup()

	b.SetBytes(9) // decoded size
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(nil)
		n, err := conn.BodyDecoded("1234", buf, 0)
		if err != nil {
			b.Fatalf("BodyDecoded failed: %v", err)
		}
		if n != 9 {
			b.Fatalf("expected 9 bytes, got %d", n)
		}
	}
}

func BenchmarkBodyDecoded_Large(b *testing.B) {
	// Generate fixture if it doesn't exist
	if err := generateLargeFixture(); err != nil {
		b.Fatalf("failed to generate fixture: %v", err)
	}

	conn, cleanup := benchmarkSetup(b, largeFixturePath)
	defer cleanup()

	b.SetBytes(largeFixtureSize) // decoded size
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(nil)
		n, err := conn.BodyDecoded("1234", buf, 0)
		if err != nil {
			b.Fatalf("BodyDecoded failed: %v", err)
		}
		if n != largeFixtureSize {
			b.Fatalf("expected %d bytes, got %d", largeFixtureSize, n)
		}
	}
}

func BenchmarkBodyDecoded_Large_WithDiscard(b *testing.B) {
	// Generate fixture if it doesn't exist
	if err := generateLargeFixture(); err != nil {
		b.Fatalf("failed to generate fixture: %v", err)
	}

	conn, cleanup := benchmarkSetup(b, largeFixturePath)
	defer cleanup()

	discardBytes := int64(1024) // Discard first 1KB
	expectedBytes := int64(largeFixtureSize) - discardBytes

	b.SetBytes(expectedBytes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(nil)
		n, err := conn.BodyDecoded("1234", buf, discardBytes)
		if err != nil {
			b.Fatalf("BodyDecoded failed: %v", err)
		}
		if n != expectedBytes {
			b.Fatalf("expected %d bytes, got %d", expectedBytes, n)
		}
	}
}

func BenchmarkBodyReader_Large(b *testing.B) {
	// Generate fixture if it doesn't exist
	if err := generateLargeFixture(); err != nil {
		b.Fatalf("failed to generate fixture: %v", err)
	}

	conn, cleanup := benchmarkSetup(b, largeFixturePath)
	defer cleanup()

	b.SetBytes(largeFixtureSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := conn.BodyReader("1234")
		if err != nil {
			b.Fatalf("BodyReader failed: %v", err)
		}

		n, err := io.Copy(io.Discard, reader)
		if err != nil {
			reader.Close()
			b.Fatalf("Copy failed: %v", err)
		}
		reader.Close()

		if n != largeFixtureSize {
			b.Fatalf("expected %d bytes, got %d", largeFixtureSize, n)
		}
	}
}
