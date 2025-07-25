package nntppool

import (
	"testing"
)

func TestConnectionBudget(t *testing.T) {
	budget := NewConnectionBudget()

	// Test setting provider limits
	budget.SetProviderLimit("provider1", 10)
	budget.SetProviderLimit("provider2", 5)

	// Test initial state
	current, limit, exists := budget.GetProviderUsage("provider1")
	if !exists {
		t.Error("provider1 should exist")
	}
	if current != 0 {
		t.Errorf("expected current usage 0, got %d", current)
	}
	if limit != 10 {
		t.Errorf("expected limit 10, got %d", limit)
	}

	// Test connection acquisition
	if !budget.CanAcquireConnection("provider1") {
		t.Error("should be able to acquire connection for provider1")
	}

	err := budget.AcquireConnection("provider1")
	if err != nil {
		t.Errorf("failed to acquire connection: %v", err)
	}

	current, _, _ = budget.GetProviderUsage("provider1")
	if current != 1 {
		t.Errorf("expected current usage 1, got %d", current)
	}

	// Test available slots
	available := budget.GetAvailableSlots("provider1")
	if available != 9 {
		t.Errorf("expected 9 available slots, got %d", available)
	}

	// Test connection release
	budget.ReleaseConnection("provider1")
	current, _, _ = budget.GetProviderUsage("provider1")
	if current != 0 {
		t.Errorf("expected current usage 0 after release, got %d", current)
	}
}

func TestConnectionBudgetLimits(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 2)

	// Fill up to limit
	for i := 0; i < 2; i++ {
		err := budget.AcquireConnection("provider1")
		if err != nil {
			t.Errorf("failed to acquire connection %d: %v", i+1, err)
		}
	}

	// Try to exceed limit
	err := budget.AcquireConnection("provider1")
	if err == nil {
		t.Error("should not be able to exceed connection limit")
	}

	if budget.CanAcquireConnection("provider1") {
		t.Error("CanAcquireConnection should return false when at limit")
	}

	// Release one and try again
	budget.ReleaseConnection("provider1")
	if !budget.CanAcquireConnection("provider1") {
		t.Error("should be able to acquire connection after release")
	}
}

func TestConnectionBudgetNonExistentProvider(t *testing.T) {
	budget := NewConnectionBudget()

	// Test operations on non-existent provider
	if budget.CanAcquireConnection("nonexistent") {
		t.Error("should not be able to acquire connection for non-existent provider")
	}

	err := budget.AcquireConnection("nonexistent")
	if err == nil {
		t.Error("should return error for non-existent provider")
	}

	_, _, exists := budget.GetProviderUsage("nonexistent")
	if exists {
		t.Error("non-existent provider should not exist")
	}

	available := budget.GetAvailableSlots("nonexistent")
	if available != 0 {
		t.Errorf("expected 0 available slots for non-existent provider, got %d", available)
	}
}

func TestConnectionBudgetTotalBudget(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 10)
	budget.SetProviderLimit("provider2", 5)

	// Initial state
	totalUsed, totalLimit := budget.GetTotalBudget()
	if totalUsed != 0 {
		t.Errorf("expected total used 0, got %d", totalUsed)
	}
	if totalLimit != 15 {
		t.Errorf("expected total limit 15, got %d", totalLimit)
	}

	// Acquire some connections
	_ = budget.AcquireConnection("provider1")
	_ = budget.AcquireConnection("provider1")
	_ = budget.AcquireConnection("provider2")

	totalUsed, totalLimit = budget.GetTotalBudget()
	if totalUsed != 3 {
		t.Errorf("expected total used 3, got %d", totalUsed)
	}
	if totalLimit != 15 {
		t.Errorf("expected total limit 15, got %d", totalLimit)
	}
}

func TestConnectionBudgetRemoveProvider(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 10)
	_ = budget.AcquireConnection("provider1")

	// Verify provider exists
	current, limit, exists := budget.GetProviderUsage("provider1")
	if !exists || current != 1 || limit != 10 {
		t.Error("provider should exist with 1 connection used")
	}

	// Remove provider
	budget.RemoveProvider("provider1")

	// Verify provider is gone
	_, _, exists = budget.GetProviderUsage("provider1")
	if exists {
		t.Error("provider should be removed")
	}

	// Total budget should be updated
	totalUsed, totalLimit := budget.GetTotalBudget()
	if totalUsed != 0 || totalLimit != 0 {
		t.Errorf("expected total budget 0/0, got %d/%d", totalUsed, totalLimit)
	}
}

func TestConnectionBudgetCanMigrateConnection(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 5)

	// Should be able to migrate when there's buffer
	if !budget.CanMigrateConnection("provider1") {
		t.Error("should be able to migrate when buffer available")
	}

	// Fill up leaving 2 slots (for migration buffer)
	for i := 0; i < 3; i++ {
		_ = budget.AcquireConnection("provider1")
	}

	// Should still be able to migrate (2 slots available, 1 reserved for migration buffer)
	if !budget.CanMigrateConnection("provider1") {
		t.Error("should be able to migrate with 2 slots available")
	}

	// Fill leaving only 1 slot
	_ = budget.AcquireConnection("provider1")

	// Should not be able to migrate when only migration buffer remains
	if budget.CanMigrateConnection("provider1") {
		t.Error("should not be able to migrate when only buffer slot remains")
	}

	// Fill to capacity
	_ = budget.AcquireConnection("provider1")

	// Should not be able to migrate when at capacity
	if budget.CanMigrateConnection("provider1") {
		t.Error("should not be able to migrate when at capacity")
	}
}

func TestConnectionBudgetProviderStats(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 10)
	budget.SetProviderLimit("provider2", 5)

	// Acquire some connections
	_ = budget.AcquireConnection("provider1")
	_ = budget.AcquireConnection("provider1")
	_ = budget.AcquireConnection("provider2")

	stats := budget.GetProviderStats()

	if len(stats) != 2 {
		t.Errorf("expected 2 provider stats, got %d", len(stats))
	}

	provider1Stats := stats["provider1_user1"]
	if provider1Stats.ProviderID == "" {
		// Fallback to checking stats by host since ID format might differ
		for _, stat := range stats {
			if stat.CurrentConnections == 2 && stat.MaxConnections == 10 {
				provider1Stats = stat
				break
			}
		}
	}

	if provider1Stats.CurrentConnections != 2 {
		t.Errorf("expected provider1 current connections 2, got %d", provider1Stats.CurrentConnections)
	}
	if provider1Stats.MaxConnections != 10 {
		t.Errorf("expected provider1 max connections 10, got %d", provider1Stats.MaxConnections)
	}
	if provider1Stats.AvailableSlots != 8 {
		t.Errorf("expected provider1 available slots 8, got %d", provider1Stats.AvailableSlots)
	}
	if provider1Stats.UtilizationPercent != 20.0 {
		t.Errorf("expected provider1 utilization 20%%, got %.1f%%", provider1Stats.UtilizationPercent)
	}
}

func TestConnectionBudgetReleaseNonExistentConnection(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 10)

	// Release connection when none are acquired (should not panic)
	budget.ReleaseConnection("provider1")

	current, _, _ := budget.GetProviderUsage("provider1")
	if current != 0 {
		t.Errorf("expected current usage 0, got %d", current)
	}

	// Release from non-existent provider (should not panic)
	budget.ReleaseConnection("nonexistent")
}

func TestConnectionBudgetConcurrentAccess(t *testing.T) {
	budget := NewConnectionBudget()
	budget.SetProviderLimit("provider1", 100)

	// Test concurrent access
	done := make(chan bool, 100)

	// Start 50 goroutines acquiring connections
	for i := 0; i < 50; i++ {
		go func() {
			_ = budget.AcquireConnection("provider1")
			done <- true
		}()
	}

	// Start 50 goroutines checking status
	for i := 0; i < 50; i++ {
		go func() {
			budget.GetProviderUsage("provider1")
			budget.CanAcquireConnection("provider1")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify final state
	current, limit, exists := budget.GetProviderUsage("provider1")
	if !exists {
		t.Error("provider should exist")
	}
	if current != 50 {
		t.Errorf("expected 50 connections acquired, got %d", current)
	}
	if limit != 100 {
		t.Errorf("expected limit 100, got %d", limit)
	}
}
