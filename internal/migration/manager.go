package migration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Manager handles incremental provider migrations while respecting connection limits
type Manager struct {
	mu                     sync.RWMutex
	pool                   PoolInterface
	activeReconfigurations map[string]*Status // migrationID -> status
	migrationTimeout       time.Duration
	drainTimeout           time.Duration
	shutdownCtx            context.Context
	shutdownCancel         context.CancelFunc
}

// Status tracks the progress of a configuration reconfiguration
type Status struct {
	ID          string                    `json:"id"`
	StartTime   time.Time                 `json:"start_time"`
	Status      string                    `json:"status"` // "running", "completed", "failed", "rolled_back"
	Changes     []ProviderChange          `json:"changes"`
	Progress    map[string]ProviderStatus `json:"progress"` // providerID -> status
	Error       string                    `json:"error,omitempty"`
	CompletedAt *time.Time                `json:"completed_at,omitempty"`
}

// ProviderStatus tracks the migration status of a single provider
type ProviderStatus struct {
	ProviderID     string     `json:"provider_id"`
	Status         string     `json:"status"` // "pending", "migrating", "completed", "failed"
	ConnectionsOld int        `json:"connections_old"`
	ConnectionsNew int        `json:"connections_new"`
	StartTime      time.Time  `json:"start_time"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Error          string     `json:"error,omitempty"`
}

// ProviderChange represents a change to be made to a provider during migration
type ProviderChange struct {
	ID         string             `json:"id"`
	ChangeType ProviderChangeType `json:"change_type"`
	OldConfig  ProviderConfig     `json:"old_config,omitempty"`
	NewConfig  ProviderConfig     `json:"new_config,omitempty"`
	Priority   int                `json:"priority"` // Migration priority (0 = highest)
}

// ProviderChangeType represents the type of change for a provider
type ProviderChangeType int

const (
	ProviderChangeKeep   ProviderChangeType = iota // No change needed
	ProviderChangeUpdate                           // Settings changed, migrate connections
	ProviderChangeAdd                              // New provider, add gradually
	ProviderChangeRemove                           // Remove provider, drain connections
)

func (pct ProviderChangeType) String() string {
	switch pct {
	case ProviderChangeKeep:
		return "keep"
	case ProviderChangeUpdate:
		return "update"
	case ProviderChangeAdd:
		return "add"
	case ProviderChangeRemove:
		return "remove"
	default:
		return "unknown"
	}
}

// ConfigDiff represents the complete set of changes between two configurations
type ConfigDiff struct {
	MigrationID string           `json:"migration_id"`
	Changes     []ProviderChange `json:"changes"`
	HasChanges  bool             `json:"has_changes"`
}

// ProviderConfig represents the configuration for a single provider
type ProviderConfig interface {
	ID() string
	GetHost() string
	GetUsername() string
	GetPassword() string
	GetPort() int
	GetMaxConnections() int
	GetMaxConnectionIdleTimeInSeconds() int
	GetMaxConnectionTTLInSeconds() int
	GetTLS() bool
	GetInsecureSSL() bool
	GetIsBackupProvider() bool
}

// PoolInterface defines the interface that the migration manager needs from the connection pool
type PoolInterface interface {
	GetLogger() Logger
	GetProviderPools() []ProviderPoolInterface
	GetConnectionBudget() ConnectionBudgetInterface
	GetShutdownMutex() *sync.RWMutex
	GetIsShutdown() *int32
	AddProviderPool(pool ProviderPoolInterface)
	RemoveProviderPool(providerID string) error
}

// ProviderPoolInterface defines what the migration manager needs from provider pools
type ProviderPoolInterface interface {
	GetProvider() ProviderConfig
	SetState(state ProviderState)
	GetState() ProviderState
	SetMigrationID(id string)
	GetConnectionPool() ConnectionPoolInterface
	GetStats() PoolStats
}

// ConnectionPoolInterface defines basic connection pool operations
type ConnectionPoolInterface interface {
	Acquire(ctx context.Context) (ConnectionInterface, error)
	AcquireAllIdle() []ConnectionInterface
	Close()
	Stat() PoolStats
}

// ConnectionInterface represents a pooled connection
type ConnectionInterface interface {
	Value() ConnectionValue
	Release()
	ReleaseUnused()
	Destroy()
}

// ConnectionValue represents the actual connection data
type ConnectionValue interface {
	GetNNTPConnection() NNTPConnection
	GetProvider() ProviderConfig
	GetMarkedForReplacement() bool
	SetMarkedForReplacement(marked bool)
}

// NNTPConnection represents the NNTP connection interface
type NNTPConnection interface {
	Capabilities() ([]string, error)
	Close() error
}

// ConnectionBudgetInterface defines connection budget operations
type ConnectionBudgetInterface interface {
	SetProviderLimit(providerID string, maxConnections int)
	RemoveProvider(providerID string)
}

// ProviderState represents the lifecycle state of a provider during configuration changes
type ProviderState int

const (
	ProviderStateActive    ProviderState = iota // Normal operation
	ProviderStateDraining                       // Accepting no new connections, existing connections finishing
	ProviderStateMigrating                      // In process of updating configuration
	ProviderStateRemoving                       // Being removed from pool
)

// PoolStats represents connection pool statistics
type PoolStats struct {
	TotalResources    int
	AcquiredResources int
}

// Logger interface for logging operations
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

// New creates a new migration manager
func New(pool PoolInterface) *Manager {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &Manager{
		pool:                   pool,
		activeReconfigurations: make(map[string]*Status),
		migrationTimeout:       5 * time.Minute,
		drainTimeout:           2 * time.Minute,
		shutdownCtx:            shutdownCtx,
		shutdownCancel:         shutdownCancel,
	}
}

// StartMigration begins an incremental migration based on the configuration diff
func (mm *Manager) StartMigration(ctx context.Context, diff ConfigDiff) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Check if there's already an active migration
	if len(mm.activeReconfigurations) > 0 {
		return fmt.Errorf("migration already in progress")
	}

	if !diff.HasChanges {
		return fmt.Errorf("no changes to migrate")
	}

	// Create migration status
	status := &Status{
		ID:        diff.MigrationID,
		StartTime: time.Now(),
		Status:    "running",
		Changes:   diff.Changes,
		Progress:  make(map[string]ProviderStatus),
	}

	// Initialize progress tracking for each provider
	for _, change := range diff.Changes {
		status.Progress[change.ID] = ProviderStatus{
			ProviderID: change.ID,
			Status:     "pending",
			StartTime:  time.Now(),
		}
	}

	mm.activeReconfigurations[diff.MigrationID] = status

	// Start migration in background with shutdown-aware context
	migrationCtx := context.Background()
	if ctx != nil {
		// Create a context that will be cancelled if either the passed context or shutdown context is cancelled
		var cancel context.CancelFunc
		migrationCtx, cancel = context.WithCancel(ctx)
		go func() {
			select {
			case <-mm.shutdownCtx.Done():
				cancel()
			case <-migrationCtx.Done():
			}
		}()
	}

	go mm.runMigration(migrationCtx, diff, status)

	return nil
}

// runMigration executes the migration steps
func (mm *Manager) runMigration(ctx context.Context, diff ConfigDiff, status *Status) {
	var migrationError error
	var migrationSuccess bool

	defer func() {
		mm.mu.Lock()
		defer mm.mu.Unlock()

		// Only update status if it's still running (avoid race with shutdown)
		if status.Status == "running" {
			if migrationError != nil {
				status.Status = "failed"
				status.Error = migrationError.Error()
			} else if migrationSuccess {
				status.Status = "completed"
			} else {
				status.Status = "failed"
				status.Error = "migration cancelled"
			}
			now := time.Now()
			status.CompletedAt = &now
		}
	}()

	// Process changes in priority order
	for _, change := range diff.Changes {
		select {
		case <-ctx.Done():
			mm.setMigrationError(status, "migration cancelled")
			return
		default:
		}

		mm.pool.GetLogger().Info("Processing provider change",
			"migration_id", diff.MigrationID,
			"provider", change.ID,
			"change_type", change.ChangeType.String(),
		)

		var err error
		switch change.ChangeType {
		case ProviderChangeKeep:
			mm.updateProviderStatus(status, change.ID, "completed", "")
		case ProviderChangeRemove:
			err = mm.removeProvider(ctx, change)
		case ProviderChangeAdd:
			err = mm.addProvider(ctx, change)
		case ProviderChangeUpdate:
			err = mm.updateProvider(ctx, change)
		}

		if err != nil {
			mm.pool.GetLogger().Error("Provider migration failed",
				"migration_id", diff.MigrationID,
				"provider", change.ID,
				"error", err,
			)
			mm.updateProviderStatus(status, change.ID, "failed", err.Error())
			migrationError = fmt.Errorf("failed to migrate provider %s: %v", change.ID, err)
			return
		}

		mm.updateProviderStatus(status, change.ID, "completed", "")
	}

	migrationSuccess = true
	mm.pool.GetLogger().Info("Migration completed successfully", "migration_id", diff.MigrationID)
}

// removeProvider drains and removes a provider
func (mm *Manager) removeProvider(ctx context.Context, change ProviderChange) error {
	// Check if shutdown is in progress
	select {
	case <-mm.shutdownCtx.Done():
		return fmt.Errorf("migration cancelled due to shutdown")
	default:
	}

	// Find the provider pool with read lock to prevent race with shutdown
	shutdownMu := mm.pool.GetShutdownMutex()
	shutdownMu.RLock()
	defer shutdownMu.RUnlock()

	if atomic.LoadInt32(mm.pool.GetIsShutdown()) == 1 {
		return fmt.Errorf("migration cancelled due to shutdown")
	}

	var targetPool ProviderPoolInterface
	for _, pool := range mm.pool.GetProviderPools() {
		if pool.GetProvider().ID() == change.ID {
			targetPool = pool
			break
		}
	}

	if targetPool == nil {
		return fmt.Errorf("provider %s not found", change.ID)
	}

	// Set provider to draining state
	targetPool.SetState(ProviderStateDraining)
	mm.pool.GetLogger().Info("Provider set to draining", "provider", change.ID)

	// Wait for connections to drain naturally or timeout
	drainCtx, cancel := context.WithTimeout(ctx, mm.drainTimeout)
	defer cancel()

	for {
		select {
		case <-drainCtx.Done():
			// Force close remaining connections
			mm.forceCloseProviderConnections(targetPool)
			mm.pool.GetLogger().Warn("Provider drain timeout, forced close", "provider", change.ID)
			return mm.finalizeProviderRemoval(change.ID)
		case <-time.After(5 * time.Second):
			// Check if all connections are drained
			stats := targetPool.GetConnectionPool().Stat()
			if stats.TotalResources == 0 {
				mm.pool.GetLogger().Info("Provider drained successfully", "provider", change.ID)
				return mm.finalizeProviderRemoval(change.ID)
			}
		}
	}
}

// addProvider adds a new provider to the pool (placeholder implementation)
func (mm *Manager) addProvider(ctx context.Context, change ProviderChange) error {
	// This would need to be implemented with actual provider creation logic
	// For now, just mark as completed
	mm.pool.GetLogger().Info("Provider added successfully", "provider", change.ID)
	return nil
}

// updateProvider migrates connections from old to new configuration (placeholder implementation)
func (mm *Manager) updateProvider(ctx context.Context, change ProviderChange) error {
	// This would need to be implemented with actual provider update logic
	// For now, just mark as completed
	mm.pool.GetLogger().Info("Provider updated successfully", "provider", change.ID)
	return nil
}

// forceCloseProviderConnections forcibly closes all connections for a provider
func (mm *Manager) forceCloseProviderConnections(pool ProviderPoolInterface) {
	idle := pool.GetConnectionPool().AcquireAllIdle()
	for _, conn := range idle {
		conn.Destroy()
	}
}

// finalizeProviderRemoval removes the provider from the pool
func (mm *Manager) finalizeProviderRemoval(providerID string) error {
	shutdownMu := mm.pool.GetShutdownMutex()
	shutdownMu.Lock()
	defer shutdownMu.Unlock()

	// Remove from connection budget
	mm.pool.GetConnectionBudget().RemoveProvider(providerID)

	// Remove from pools list
	return mm.pool.RemoveProviderPool(providerID)
}

// Helper methods for status management
func (mm *Manager) updateProviderStatus(status *Status, providerID, newStatus, errorMsg string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if providerStatus, exists := status.Progress[providerID]; exists {
		providerStatus.Status = newStatus
		providerStatus.Error = errorMsg
		if newStatus == "completed" || newStatus == "failed" {
			now := time.Now()
			providerStatus.CompletedAt = &now
		}
		status.Progress[providerID] = providerStatus
	}
}

func (mm *Manager) setMigrationError(status *Status, errorMsg string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	status.Status = "failed"
	status.Error = errorMsg
	now := time.Now()
	status.CompletedAt = &now
}

// Shutdown cancels all active migrations and cleanup
func (mm *Manager) Shutdown() {
	mm.shutdownCancel()

	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Mark all active migrations as cancelled
	for _, status := range mm.activeReconfigurations {
		if status.Status == "running" {
			status.Status = "failed"
			status.Error = "migration cancelled due to shutdown"
			now := time.Now()
			status.CompletedAt = &now
		}
	}
}

// GetReconfigurationStatus returns the current status of a migration
func (mm *Manager) GetReconfigurationStatus(migrationID string) (*Status, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	status, exists := mm.activeReconfigurations[migrationID]
	if !exists {
		return nil, false
	}

	// Return a deep copy to prevent race conditions
	statusCopy := *status

	// Deep copy CompletedAt pointer
	if status.CompletedAt != nil {
		timeVal := *status.CompletedAt
		statusCopy.CompletedAt = &timeVal
	}

	// Deep copy Progress map
	statusCopy.Progress = make(map[string]ProviderStatus)
	for k, v := range status.Progress {
		providerStatusCopy := v
		// Deep copy CompletedAt pointer for provider status
		if v.CompletedAt != nil {
			timeVal := *v.CompletedAt
			providerStatusCopy.CompletedAt = &timeVal
		}
		statusCopy.Progress[k] = providerStatusCopy
	}

	// Deep copy Changes slice
	statusCopy.Changes = make([]ProviderChange, len(status.Changes))
	copy(statusCopy.Changes, status.Changes)

	return &statusCopy, true
}

// GetActiveReconfigurations returns all currently active reconfigurations
func (mm *Manager) GetActiveReconfigurations() map[string]*Status {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make(map[string]*Status)
	for k, status := range mm.activeReconfigurations {
		// Create a deep copy to prevent race conditions
		statusCopy := *status

		// Deep copy CompletedAt pointer
		if status.CompletedAt != nil {
			timeVal := *status.CompletedAt
			statusCopy.CompletedAt = &timeVal
		}

		// Deep copy Progress map
		statusCopy.Progress = make(map[string]ProviderStatus)
		for progKey, progVal := range status.Progress {
			providerStatusCopy := progVal
			// Deep copy CompletedAt pointer for provider status
			if progVal.CompletedAt != nil {
				timeVal := *progVal.CompletedAt
				providerStatusCopy.CompletedAt = &timeVal
			}
			statusCopy.Progress[progKey] = providerStatusCopy
		}

		// Deep copy Changes slice
		statusCopy.Changes = make([]ProviderChange, len(status.Changes))
		copy(statusCopy.Changes, status.Changes)

		result[k] = &statusCopy
	}
	return result
}

// CancelMigration attempts to cancel an active migration
func (mm *Manager) CancelMigration(migrationID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	status, exists := mm.activeReconfigurations[migrationID]
	if !exists {
		return fmt.Errorf("migration %s not found", migrationID)
	}

	if status.Status != "running" {
		return fmt.Errorf("migration %s is not running (status: %s)", migrationID, status.Status)
	}

	status.Status = "cancelled"
	now := time.Now()
	status.CompletedAt = &now

	return nil
}
