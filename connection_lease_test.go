package nntppool

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/puddle/v2"
)

func TestConnectionLeaseExpiration(t *testing.T) {
	// Create a mock internal connection
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	// Create connection with short lease
	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(50 * time.Millisecond),
		markedForReplacement: false,
	}

	// Create a mock puddle resource
	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	// Initially not expired
	if pooledConn.IsLeaseExpired() {
		t.Error("connection should not be expired initially")
	}

	// Wait for lease to expire
	time.Sleep(100 * time.Millisecond)

	if !pooledConn.IsLeaseExpired() {
		t.Error("connection should be expired after lease time")
	}
}

func TestConnectionLeaseExtension(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(50 * time.Millisecond),
		markedForReplacement: false,
	}

	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	// Extend lease before it expires
	pooledConn.ExtendLease(200 * time.Millisecond)

	// Wait for original lease time to pass
	time.Sleep(100 * time.Millisecond)

	// Should not be expired due to extension
	if pooledConn.IsLeaseExpired() {
		t.Error("connection should not be expired after lease extension")
	}

	// Wait for extended lease to expire
	time.Sleep(150 * time.Millisecond)

	if !pooledConn.IsLeaseExpired() {
		t.Error("connection should be expired after extended lease time")
	}
}

func TestConnectionMarkForReplacement(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(1 * time.Hour),
		markedForReplacement: false,
	}

	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	// Initially not marked for replacement
	if pooledConn.IsMarkedForReplacement() {
		t.Error("connection should not be marked for replacement initially")
	}

	// Mark for replacement
	pooledConn.MarkForReplacement()

	if !pooledConn.IsMarkedForReplacement() {
		t.Error("connection should be marked for replacement")
	}
}

func TestConnectionProviderInfo(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:                           "test.example.com",
		Username:                       "testuser",
		MaxConnections:                 10,
		MaxConnectionIdleTimeInSeconds: 300,
	}

	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(1 * time.Hour),
		markedForReplacement: false,
	}

	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	info := pooledConn.Provider()

	if info.Host != "test.example.com" {
		t.Errorf("expected host test.example.com, got %s", info.Host)
	}
	if info.Username != "testuser" {
		t.Errorf("expected username testuser, got %s", info.Username)
	}
	if info.MaxConnections != 10 {
		t.Errorf("expected max connections 10, got %d", info.MaxConnections)
	}
	if info.MaxConnectionIdleTimeout != 300*time.Second {
		t.Errorf("expected idle timeout 300s, got %v", info.MaxConnectionIdleTimeout)
	}
	if info.State != ProviderStateActive {
		t.Errorf("expected state active, got %s", info.State)
	}
}

func TestConnectionCreatedAt(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(1 * time.Hour),
		markedForReplacement: false,
	}

	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	beforeAcquire := time.Now()
	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()
	afterAcquire := time.Now()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	createdAt := pooledConn.CreatedAt()

	if createdAt.Before(beforeAcquire) || createdAt.After(afterAcquire) {
		t.Errorf("created time %v should be between %v and %v", createdAt, beforeAcquire, afterAcquire)
	}
}

func TestConnectionNNTPAccess(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	conn := &internalConnection{
		nntp:                 nil, // No actual NNTP connection for this test
		provider:             provider,
		leaseExpiry:          time.Now().Add(1 * time.Hour),
		markedForReplacement: false,
	}

	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return conn, nil
		},
		Destructor: func(value *internalConnection) {},
		MaxSize:    1,
	})
	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}
	defer puddlePool.Close()

	resource, err := puddlePool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire resource: %v", err)
	}
	defer resource.Release()

	pooledConn := pooledConnection{
		resource: resource,
		log:      slog.Default(),
	}

	// Test that we can access the underlying NNTP connection
	nntpConn := pooledConn.Connection()
	if nntpConn != nil {
		t.Error("expected nil NNTP connection for this test")
	}
}

func TestInternalConnectionInitialization(t *testing.T) {
	// Test that new internal connections are properly initialized
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	defaultLease := 10 * time.Minute

	// Simulate what happens in getPoolsWithLease
	conn := &internalConnection{
		provider:             provider,
		leaseExpiry:          time.Now().Add(defaultLease),
		markedForReplacement: false,
	}

	// Verify initialization
	if conn.provider.Host != "test.example.com" {
		t.Errorf("expected host test.example.com, got %s", conn.provider.Host)
	}

	if conn.markedForReplacement {
		t.Error("new connection should not be marked for replacement")
	}

	if time.Until(conn.leaseExpiry) <= 0 {
		t.Error("lease should be in the future")
	}

	if time.Until(conn.leaseExpiry) > defaultLease+time.Second {
		t.Error("lease should be approximately the default lease duration")
	}
}

func TestConnectionLeaseSystem(t *testing.T) {
	// Integration test for the complete lease system
	provider := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	// Test pools creation with lease
	provider.MaxConnections = 1 // Ensure minimum pool size
	pools, err := getPoolsWithLease(
		[]UsenetProviderConfig{provider},
		nil, // nntpCli - nil for this test
		slog.Default(),
		100*time.Millisecond, // Very short lease for testing
		nil, // metrics - nil for this test
	)

	if err != nil {
		t.Fatalf("failed to create pools: %v", err)
	}

	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}

	defer pools[0].connectionPool.Close()

	// The pool should be properly initialized
	if pools[0].provider.Host != "test.example.com" {
		t.Errorf("expected host test.example.com, got %s", pools[0].provider.Host)
	}

	if pools[0].GetState() != ProviderStateActive {
		t.Errorf("expected state active, got %s", pools[0].GetState())
	}
}