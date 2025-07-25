package nntppool

import (
	"fmt"
	"sync"
)

// ConnectionBudget manages connection limits per provider to prevent exceeding NNTP server quotas
type ConnectionBudget struct {
	mu            sync.RWMutex
	providerLimits map[string]int // providerID -> max connections
	providerUsage  map[string]int // providerID -> current connections
}

// NewConnectionBudget creates a new connection budget manager
func NewConnectionBudget() *ConnectionBudget {
	return &ConnectionBudget{
		providerLimits: make(map[string]int),
		providerUsage:  make(map[string]int),
	}
}

// SetProviderLimit sets the maximum connections allowed for a provider
func (cb *ConnectionBudget) SetProviderLimit(providerID string, maxConnections int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.providerLimits[providerID] = maxConnections
	
	// Initialize usage if not exists
	if _, exists := cb.providerUsage[providerID]; !exists {
		cb.providerUsage[providerID] = 0
	}
}

// CanAcquireConnection checks if a new connection can be acquired for the provider
func (cb *ConnectionBudget) CanAcquireConnection(providerID string) bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	limit, hasLimit := cb.providerLimits[providerID]
	if !hasLimit {
		return false // No limit set, can't acquire
	}
	
	usage := cb.providerUsage[providerID]
	return usage < limit
}

// AcquireConnection attempts to acquire a connection slot for the provider
func (cb *ConnectionBudget) AcquireConnection(providerID string) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	limit, hasLimit := cb.providerLimits[providerID]
	if !hasLimit {
		return fmt.Errorf("no connection limit set for provider %s", providerID)
	}
	
	usage := cb.providerUsage[providerID]
	if usage >= limit {
		return fmt.Errorf("connection limit exceeded for provider %s (%d/%d)", providerID, usage, limit)
	}
	
	cb.providerUsage[providerID]++
	return nil
}

// ReleaseConnection releases a connection slot for the provider
func (cb *ConnectionBudget) ReleaseConnection(providerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if usage := cb.providerUsage[providerID]; usage > 0 {
		cb.providerUsage[providerID]--
	}
}

// GetProviderUsage returns current usage for a provider
func (cb *ConnectionBudget) GetProviderUsage(providerID string) (current, limit int, exists bool) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	limit, hasLimit := cb.providerLimits[providerID]
	if !hasLimit {
		return 0, 0, false
	}
	
	current = cb.providerUsage[providerID]
	return current, limit, true
}

// GetAvailableSlots returns how many more connections can be created for the provider
func (cb *ConnectionBudget) GetAvailableSlots(providerID string) int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	limit, hasLimit := cb.providerLimits[providerID]
	if !hasLimit {
		return 0
	}
	
	usage := cb.providerUsage[providerID]
	available := limit - usage
	if available < 0 {
		return 0
	}
	return available
}

// GetTotalBudget returns the total connection budget across all providers
func (cb *ConnectionBudget) GetTotalBudget() (totalUsed, totalLimit int) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	for providerID, limit := range cb.providerLimits {
		totalLimit += limit
		totalUsed += cb.providerUsage[providerID]
	}
	
	return totalUsed, totalLimit
}

// RemoveProvider removes a provider from budget tracking
func (cb *ConnectionBudget) RemoveProvider(providerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	delete(cb.providerLimits, providerID)
	delete(cb.providerUsage, providerID)
}

// CanMigrateConnection checks if we can create a new connection during migration
// This considers both current usage and temporary migration overhead
func (cb *ConnectionBudget) CanMigrateConnection(providerID string) bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	limit, hasLimit := cb.providerLimits[providerID]
	if !hasLimit {
		return false
	}
	
	usage := cb.providerUsage[providerID]
	
	// During migration, we might need extra slots temporarily
	// Reserve at least 1 slot for migration if possible
	migrationBuffer := 1
	if limit <= migrationBuffer {
		migrationBuffer = 0
	}
	
	return usage < (limit - migrationBuffer)
}

// GetProviderStats returns detailed statistics for all providers
func (cb *ConnectionBudget) GetProviderStats() map[string]ProviderBudgetStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	stats := make(map[string]ProviderBudgetStats)
	
	for providerID, limit := range cb.providerLimits {
		usage := cb.providerUsage[providerID]
		available := limit - usage
		if available < 0 {
			available = 0
		}
		
		stats[providerID] = ProviderBudgetStats{
			ProviderID:         providerID,
			CurrentConnections: usage,
			MaxConnections:     limit,
			AvailableSlots:     available,
			UtilizationPercent: float64(usage) / float64(limit) * 100,
		}
	}
	
	return stats
}

// ProviderBudgetStats contains statistics for a provider's connection budget
type ProviderBudgetStats struct {
	ProviderID         string  `json:"provider_id"`
	CurrentConnections int     `json:"current_connections"`
	MaxConnections     int     `json:"max_connections"`
	AvailableSlots     int     `json:"available_slots"`
	UtilizationPercent float64 `json:"utilization_percent"`
}