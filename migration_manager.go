package nntppool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MigrationManager handles incremental provider migrations while respecting connection limits
type MigrationManager struct {
	mu                sync.RWMutex
	pool              *connectionPool
	activeMigrations  map[string]*MigrationStatus // migrationID -> status
	migrationTimeout  time.Duration
	drainTimeout      time.Duration
	shutdownCtx       context.Context
	shutdownCancel    context.CancelFunc
}

// MigrationStatus tracks the progress of a configuration migration
type MigrationStatus struct {
	ID          string                    `json:"id"`
	StartTime   time.Time                 `json:"start_time"`
	Status      string                    `json:"status"` // "running", "completed", "failed", "rolled_back"
	Changes     []ProviderDiff            `json:"changes"`
	Progress    map[string]ProviderStatus `json:"progress"` // providerID -> status
	Error       string                    `json:"error,omitempty"`
	CompletedAt *time.Time                `json:"completed_at,omitempty"`
}

// ProviderStatus tracks the migration status of a single provider
type ProviderStatus struct {
	ProviderID      string    `json:"provider_id"`
	Status          string    `json:"status"` // "pending", "migrating", "completed", "failed"
	ConnectionsOld  int       `json:"connections_old"`
	ConnectionsNew  int       `json:"connections_new"`
	StartTime       time.Time `json:"start_time"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	Error           string    `json:"error,omitempty"`
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(pool *connectionPool) *MigrationManager {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &MigrationManager{
		pool:             pool,
		activeMigrations: make(map[string]*MigrationStatus),
		migrationTimeout: 5 * time.Minute,
		drainTimeout:     2 * time.Minute,
		shutdownCtx:      shutdownCtx,
		shutdownCancel:   shutdownCancel,
	}
}

// StartMigration begins an incremental migration based on the configuration diff
func (mm *MigrationManager) StartMigration(ctx context.Context, diff ConfigDiff, newConfig Config) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Check if there's already an active migration
	if len(mm.activeMigrations) > 0 {
		return fmt.Errorf("migration already in progress")
	}

	if !diff.HasChanges {
		return fmt.Errorf("no changes to migrate")
	}

	// Create migration status
	status := &MigrationStatus{
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

	mm.activeMigrations[diff.MigrationID] = status

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
	
	go mm.runMigration(migrationCtx, diff, newConfig, status)

	return nil
}

// runMigration executes the migration steps
func (mm *MigrationManager) runMigration(ctx context.Context, diff ConfigDiff, newConfig Config, status *MigrationStatus) {
	defer func() {
		mm.mu.Lock()
		if status.Status == "running" {
			status.Status = "completed"
			now := time.Now()
			status.CompletedAt = &now
		}
		mm.mu.Unlock()
	}()

	// Process changes in priority order
	for _, change := range diff.Changes {
		select {
		case <-ctx.Done():
			mm.setMigrationError(status, "migration cancelled")
			return
		default:
		}

		mm.pool.log.Info("Processing provider change",
			"migration_id", diff.MigrationID,
			"provider", change.ID,
			"change_type", change.ChangeType.String(),
		)

		var err error
		switch change.ChangeType {
		case ProviderChangeKeep:
			// No action needed
			mm.updateProviderStatus(status, change.ID, "completed", "")
		case ProviderChangeRemove:
			err = mm.removeProvider(ctx, change)
		case ProviderChangeAdd:
			err = mm.addProvider(ctx, change, newConfig)
		case ProviderChangeUpdate:
			err = mm.updateProvider(ctx, change, newConfig)
		}

		if err != nil {
			mm.pool.log.Error("Provider migration failed",
				"migration_id", diff.MigrationID,
				"provider", change.ID,
				"error", err,
			)
			mm.updateProviderStatus(status, change.ID, "failed", err.Error())
			mm.setMigrationError(status, fmt.Sprintf("failed to migrate provider %s: %v", change.ID, err))
			return
		}

		mm.updateProviderStatus(status, change.ID, "completed", "")
	}

	mm.pool.log.Info("Migration completed successfully", "migration_id", diff.MigrationID)
}

// removeProvider drains and removes a provider
func (mm *MigrationManager) removeProvider(ctx context.Context, change ProviderDiff) error {
	// Check if shutdown is in progress
	select {
	case <-mm.shutdownCtx.Done():
		return fmt.Errorf("migration cancelled due to shutdown")
	default:
	}
	
	// Find the provider pool with read lock to prevent race with shutdown
	mm.pool.shutdownMu.RLock()
	defer mm.pool.shutdownMu.RUnlock()
	
	if atomic.LoadInt32(&mm.pool.isShutdown) == 1 {
		return fmt.Errorf("migration cancelled due to shutdown")
	}
	
	var targetPool *providerPool
	for _, pool := range mm.pool.connPools {
		if pool.provider.ID() == change.ID {
			targetPool = pool
			break
		}
	}

	if targetPool == nil {
		return fmt.Errorf("provider %s not found", change.ID)
	}

	// Set provider to draining state
	targetPool.SetState(ProviderStateDraining)
	mm.pool.log.Info("Provider set to draining", "provider", change.ID)

	// Wait for connections to drain naturally or timeout
	drainCtx, cancel := context.WithTimeout(ctx, mm.drainTimeout)
	defer cancel()

	for {
		select {
		case <-drainCtx.Done():
			// Force close remaining connections
			mm.forceCloseProviderConnections(targetPool)
			mm.pool.log.Warn("Provider drain timeout, forced close", "provider", change.ID)
		case <-time.After(5 * time.Second):
			// Check if all connections are drained
			stats := targetPool.connectionPool.Stat()
			if stats.TotalResources() == 0 {
				mm.pool.log.Info("Provider drained successfully", "provider", change.ID)
				return mm.finalizeProviderRemoval(change.ID)
			}
		}
	}
}

// addProvider adds a new provider to the pool
func (mm *MigrationManager) addProvider(ctx context.Context, change ProviderDiff, newConfig Config) error {
	if change.NewConfig == nil {
		return fmt.Errorf("new config is nil for provider %s", change.ID)
	}

	// Create new provider pool
	newPools, err := getPoolsWithLease([]UsenetProviderConfig{*change.NewConfig}, newConfig.NntpCli, mm.pool.log, newConfig.DefaultConnectionLease)
	if err != nil {
		return fmt.Errorf("failed to create pool for provider %s: %w", change.ID, err)
	}

	if len(newPools) != 1 {
		return fmt.Errorf("expected 1 pool, got %d", len(newPools))
	}

	newPool := newPools[0]

	// Verify the new provider works
	if err := mm.verifyProvider(ctx, newPool); err != nil {
		return fmt.Errorf("provider verification failed for %s: %w", change.ID, err)
	}

	// Add to connection budget
	mm.pool.connectionBudget.SetProviderLimit(change.ID, change.NewConfig.MaxConnections)

	// Add to pool list
	mm.pool.shutdownMu.Lock()
	mm.pool.connPools = append(mm.pool.connPools, newPool)
	mm.pool.shutdownMu.Unlock()

	mm.pool.log.Info("Provider added successfully", "provider", change.ID)
	return nil
}

// updateProvider migrates connections from old to new configuration
func (mm *MigrationManager) updateProvider(ctx context.Context, change ProviderDiff, newConfig Config) error {
	// Check if shutdown is in progress
	select {
	case <-mm.shutdownCtx.Done():
		return fmt.Errorf("migration cancelled due to shutdown")
	default:
	}
	
	if change.OldConfig == nil || change.NewConfig == nil {
		return fmt.Errorf("missing old or new config for provider %s", change.ID)
	}

	// Find existing provider pool with read lock to prevent race with shutdown
	mm.pool.shutdownMu.RLock()
	defer mm.pool.shutdownMu.RUnlock()
	
	if atomic.LoadInt32(&mm.pool.isShutdown) == 1 {
		return fmt.Errorf("migration cancelled due to shutdown")
	}
	
	var existingPool *providerPool
	for _, pool := range mm.pool.connPools {
		if pool.provider.ID() == change.ID {
			existingPool = pool
			break
		}
	}

	if existingPool == nil {
		return fmt.Errorf("existing provider %s not found", change.ID)
	}

	// Set to migrating state
	existingPool.SetState(ProviderStateMigrating)
	existingPool.SetMigrationID(change.ID)

	// Mark existing connections for replacement
	mm.markConnectionsForReplacement(existingPool)

	// Update provider configuration in place
	existingPool.provider = *change.NewConfig

	// Update connection budget if max connections changed
	if change.OldConfig.MaxConnections != change.NewConfig.MaxConnections {
		mm.pool.connectionBudget.SetProviderLimit(change.ID, change.NewConfig.MaxConnections)
	}

	// Gradually replace connections as they become idle
	return mm.performGradualReplacement(ctx, existingPool, newConfig)
}

// verifyProvider tests that a new provider is working
func (mm *MigrationManager) verifyProvider(ctx context.Context, pool *providerPool) error {
	testCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := pool.connectionPool.Acquire(testCtx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Test basic functionality
	_, err = conn.Value().nntp.Capabilities()
	return err
}

// markConnectionsForReplacement marks all connections in a pool for replacement
func (mm *MigrationManager) markConnectionsForReplacement(pool *providerPool) {
	idle := pool.connectionPool.AcquireAllIdle()
	for _, conn := range idle {
		conn.Value().markedForReplacement = true
		conn.ReleaseUnused()
	}
}

// performGradualReplacement gradually replaces connections as they become available
func (mm *MigrationManager) performGradualReplacement(ctx context.Context, pool *providerPool, config Config) error {
	replacementCtx, cancel := context.WithTimeout(ctx, mm.migrationTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-replacementCtx.Done():
			// Migration timeout - mark as complete even if some connections weren't replaced
			// They'll be replaced naturally as they expire
			pool.SetState(ProviderStateActive)
			return nil
		case <-ticker.C:
			// Check if all connections have been replaced
			if mm.checkReplacementComplete(pool) {
				pool.SetState(ProviderStateActive)
				mm.pool.log.Info("Provider migration completed", "provider", pool.provider.ID())
				return nil
			}
		}
	}
}

// checkReplacementComplete checks if all marked connections have been replaced
func (mm *MigrationManager) checkReplacementComplete(pool *providerPool) bool {
	idle := pool.connectionPool.AcquireAllIdle()
	defer func() {
		for _, conn := range idle {
			conn.ReleaseUnused()
		}
	}()

	for _, conn := range idle {
		if conn.Value().markedForReplacement {
			return false
		}
	}
	return true
}

// forceCloseProviderConnections forcibly closes all connections for a provider
func (mm *MigrationManager) forceCloseProviderConnections(pool *providerPool) {
	idle := pool.connectionPool.AcquireAllIdle()
	for _, conn := range idle {
		conn.Destroy()
	}
}

// finalizeProviderRemoval removes the provider from the pool
func (mm *MigrationManager) finalizeProviderRemoval(providerID string) error {
	mm.pool.shutdownMu.Lock()
	defer mm.pool.shutdownMu.Unlock()

	// Remove from connection budget
	mm.pool.connectionBudget.RemoveProvider(providerID)

	// Remove from pools list
	for i, pool := range mm.pool.connPools {
		if pool.provider.ID() == providerID {
			// Close the pool
			pool.connectionPool.Close()
			
			// Remove from slice
			mm.pool.connPools = append(mm.pool.connPools[:i], mm.pool.connPools[i+1:]...)
			break
		}
	}

	return nil
}

// Helper methods for status management
func (mm *MigrationManager) updateProviderStatus(status *MigrationStatus, providerID, newStatus, errorMsg string) {
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

func (mm *MigrationManager) setMigrationError(status *MigrationStatus, errorMsg string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	status.Status = "failed"
	status.Error = errorMsg
	now := time.Now()
	status.CompletedAt = &now
}

// Shutdown cancels all active migrations and cleanup
func (mm *MigrationManager) Shutdown() {
	mm.shutdownCancel()
	
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	// Mark all active migrations as cancelled
	for _, status := range mm.activeMigrations {
		if status.Status == "running" {
			status.Status = "failed"
			status.Error = "migration cancelled due to shutdown"
			now := time.Now()
			status.CompletedAt = &now
		}
	}
}

// GetMigrationStatus returns the current status of a migration
func (mm *MigrationManager) GetMigrationStatus(migrationID string) (*MigrationStatus, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	status, exists := mm.activeMigrations[migrationID]
	if !exists {
		return nil, false
	}
	
	// Return a copy to prevent race conditions
	statusCopy := *status
	statusCopy.Progress = make(map[string]ProviderStatus)
	for k, v := range status.Progress {
		statusCopy.Progress[k] = v
	}
	statusCopy.Changes = make([]ProviderDiff, len(status.Changes))
	copy(statusCopy.Changes, status.Changes)
	return &statusCopy, true
}

// GetActiveMigrations returns all currently active migrations
func (mm *MigrationManager) GetActiveMigrations() map[string]*MigrationStatus {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make(map[string]*MigrationStatus)
	for k, v := range mm.activeMigrations {
		result[k] = v
	}
	return result
}

// CancelMigration attempts to cancel an active migration
func (mm *MigrationManager) CancelMigration(migrationID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	status, exists := mm.activeMigrations[migrationID]
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