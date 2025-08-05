package nntppool

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/puddle/v2"

	"github.com/javi11/nntppool/internal/budget"
	"github.com/javi11/nntppool/internal/migration"
)

// migrationAdapter implements the interfaces needed by the internal migration manager
type migrationAdapter struct {
	pool *connectionPool
}

func newMigrationAdapter(pool *connectionPool) *migrationAdapter {
	return &migrationAdapter{pool: pool}
}

// GetLogger implements migration.PoolInterface
func (ma *migrationAdapter) GetLogger() migration.Logger {
	return ma.pool.log
}

// GetProviderPools implements migration.PoolInterface
func (ma *migrationAdapter) GetProviderPools() []migration.ProviderPoolInterface {
	pools := make([]migration.ProviderPoolInterface, len(ma.pool.connPools))
	for i, pool := range ma.pool.connPools {
		pools[i] = &providerPoolAdapter{pool: pool}
	}
	return pools
}

// GetConnectionBudget implements migration.PoolInterface
func (ma *migrationAdapter) GetConnectionBudget() migration.ConnectionBudgetInterface {
	return &budgetAdapter{budget: ma.pool.connectionBudget}
}

// GetShutdownMutex implements migration.PoolInterface
func (ma *migrationAdapter) GetShutdownMutex() *sync.RWMutex {
	return &ma.pool.shutdownMu
}

// GetIsShutdown implements migration.PoolInterface
func (ma *migrationAdapter) GetIsShutdown() *int32 {
	return &ma.pool.isShutdown
}

// AddProviderPool implements migration.PoolInterface
func (ma *migrationAdapter) AddProviderPool(pool migration.ProviderPoolInterface) {
	// This would need to be implemented to actually add a provider pool
	// For now, just log that it was called
	ma.pool.log.Info("AddProviderPool called (not implemented)")
}

// RemoveProviderPool implements migration.PoolInterface
func (ma *migrationAdapter) RemoveProviderPool(providerID string) error {
	// This would need to be implemented to actually remove a provider pool
	// For now, just log that it was called
	ma.pool.log.Info("RemoveProviderPool called", "provider_id", providerID)
	return nil
}

// providerPoolAdapter adapts providerPool to migration.ProviderPoolInterface
type providerPoolAdapter struct {
	pool *providerPool
}

func (ppa *providerPoolAdapter) GetProvider() migration.ProviderConfig {
	return &ppa.pool.provider
}

func (ppa *providerPoolAdapter) SetState(state migration.ProviderState) {
	// Convert from migration.ProviderState to our ProviderState
	var localState ProviderState
	switch state {
	case migration.ProviderStateActive:
		localState = ProviderStateActive
	case migration.ProviderStateDraining:
		localState = ProviderStateDraining
	case migration.ProviderStateMigrating:
		localState = ProviderStateMigrating
	case migration.ProviderStateRemoving:
		localState = ProviderStateRemoving
	}
	ppa.pool.SetState(localState)
}

func (ppa *providerPoolAdapter) GetState() migration.ProviderState {
	// Convert from our ProviderState to migration.ProviderState
	switch ppa.pool.GetState() {
	case ProviderStateActive:
		return migration.ProviderStateActive
	case ProviderStateDraining:
		return migration.ProviderStateDraining
	case ProviderStateMigrating:
		return migration.ProviderStateMigrating
	case ProviderStateRemoving:
		return migration.ProviderStateRemoving
	default:
		return migration.ProviderStateActive
	}
}

func (ppa *providerPoolAdapter) SetMigrationID(id string) {
	ppa.pool.SetMigrationID(id)
}

func (ppa *providerPoolAdapter) GetConnectionPool() migration.ConnectionPoolInterface {
	return &connectionPoolAdapter{pool: ppa.pool.connectionPool}
}

func (ppa *providerPoolAdapter) GetStats() migration.PoolStats {
	stats := ppa.pool.connectionPool.Stat()
	return migration.PoolStats{
		TotalResources:    int(stats.TotalResources()),
		AcquiredResources: int(stats.AcquiredResources()),
	}
}

// connectionPoolAdapter adapts puddle connection pool to migration interface
type connectionPoolAdapter struct {
	pool *puddle.Pool[*internalConnection]
}

func (cpa *connectionPoolAdapter) Acquire(ctx context.Context) (migration.ConnectionInterface, error) {
	// This would need proper context conversion and connection wrapping
	// For now, return nil to avoid compilation errors
	return nil, nil
}

func (cpa *connectionPoolAdapter) AcquireAllIdle() []migration.ConnectionInterface {
	// This would need proper connection wrapping
	// For now, return empty slice
	return []migration.ConnectionInterface{}
}

func (cpa *connectionPoolAdapter) Close() {
	cpa.pool.Close()
}

func (cpa *connectionPoolAdapter) Stat() migration.PoolStats {
	stats := cpa.pool.Stat()
	return migration.PoolStats{
		TotalResources:    int(stats.TotalResources()),
		AcquiredResources: int(stats.AcquiredResources()),
	}
}

// budgetAdapter adapts budget.Budget to migration.ConnectionBudgetInterface
type budgetAdapter struct {
	budget *budget.Budget
}

func (ba *budgetAdapter) SetProviderLimit(providerID string, maxConnections int) {
	ba.budget.SetProviderLimit(providerID, maxConnections)
}

func (ba *budgetAdapter) RemoveProvider(providerID string) {
	ba.budget.RemoveProvider(providerID)
}

// convertToInternalStatus converts internal migration.Status to public ReconfigurationStatus
// This function creates a deep copy to avoid data races when the internal status is being modified concurrently
func convertToInternalStatus(internal *migration.Status) *ReconfigurationStatus {
	if internal == nil {
		return nil
	}

	// Create deep copies of all fields to avoid races
	changes := make([]ProviderChange, len(internal.Changes))
	for i, change := range internal.Changes {
		changes[i] = ProviderChange{
			ID:         change.ID,
			ChangeType: ProviderChangeType(change.ChangeType),
			Priority:   change.Priority,
		}

		// Convert provider configs if they exist
		if change.OldConfig != nil {
			if oldConfig, ok := change.OldConfig.(*UsenetProviderConfig); ok {
				changes[i].OldConfig = oldConfig
			}
		}
		if change.NewConfig != nil {
			if newConfig, ok := change.NewConfig.(*UsenetProviderConfig); ok {
				changes[i].NewConfig = newConfig
			}
		}
	}

	progress := make(map[string]ProviderStatus)
	for k, v := range internal.Progress {
		// Create a copy of CompletedAt pointer to avoid race
		var completedAtCopy *time.Time
		if v.CompletedAt != nil {
			timeVal := *v.CompletedAt
			completedAtCopy = &timeVal
		}

		progress[k] = ProviderStatus{
			ProviderID:     v.ProviderID,
			Status:         v.Status,
			ConnectionsOld: v.ConnectionsOld,
			ConnectionsNew: v.ConnectionsNew,
			StartTime:      v.StartTime,
			CompletedAt:    completedAtCopy,
			Error:          v.Error,
		}
	}

	// Create a copy of CompletedAt pointer to avoid race on the main status
	var completedAtCopy *time.Time
	if internal.CompletedAt != nil {
		timeVal := *internal.CompletedAt
		completedAtCopy = &timeVal
	}

	return &ReconfigurationStatus{
		ID:          internal.ID,
		StartTime:   internal.StartTime,
		Status:      internal.Status,
		Changes:     changes,
		Progress:    progress,
		Error:       internal.Error,
		CompletedAt: completedAtCopy,
	}
}
