package config

import (
	"crypto/sha256"
	"fmt"

	"github.com/javi11/nntppool/internal/migration"
)

// ProviderConfig represents a provider configuration that can be compared
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

// Config represents a configuration with providers
type Config interface {
	GetProviders() []ProviderConfig
}

// AnalyzeChanges compares old and new configurations and returns a diff
func AnalyzeChanges(oldConfig, newConfig Config) migration.ConfigDiff {
	migrationID := generateMigrationID(oldConfig, newConfig)

	oldProviders := make(map[string]ProviderConfig)
	newProviders := make(map[string]ProviderConfig)

	// Build maps of providers by ID
	for _, p := range oldConfig.GetProviders() {
		oldProviders[p.ID()] = p
	}

	for _, p := range newConfig.GetProviders() {
		newProviders[p.ID()] = p
	}

	var changes []migration.ProviderChange
	allProviderIDs := make(map[string]bool)

	// Collect all unique provider IDs
	for id := range oldProviders {
		allProviderIDs[id] = true
	}
	for id := range newProviders {
		allProviderIDs[id] = true
	}

	// Analyze changes for each provider
	for id := range allProviderIDs {
		oldProvider, existsInOld := oldProviders[id]
		newProvider, existsInNew := newProviders[id]

		var change migration.ProviderChange
		change.ID = id

		switch {
		case existsInOld && existsInNew:
			// Provider exists in both - check if changed
			if providersEqual(oldProvider, newProvider) {
				change.ChangeType = migration.ProviderChangeKeep
			} else {
				change.ChangeType = migration.ProviderChangeUpdate
				change.OldConfig = oldProvider
				change.NewConfig = newProvider
			}
		case existsInOld && !existsInNew:
			// Provider removed
			change.ChangeType = migration.ProviderChangeRemove
			change.OldConfig = oldProvider
		case !existsInOld && existsInNew:
			// Provider added
			change.ChangeType = migration.ProviderChangeAdd
			change.NewConfig = newProvider
		}

		// Assign migration priority
		change.Priority = getMigrationPriority(change.ChangeType, change.NewConfig)

		changes = append(changes, change)
	}

	// Sort changes by priority
	sortChangesByPriority(changes)

	hasChanges := false
	for _, change := range changes {
		if change.ChangeType != migration.ProviderChangeKeep {
			hasChanges = true
			break
		}
	}

	return migration.ConfigDiff{
		MigrationID: migrationID,
		Changes:     changes,
		HasChanges:  hasChanges,
	}
}

// providersEqual compares two provider configurations for equality
func providersEqual(a, b ProviderConfig) bool {
	return a.GetHost() == b.GetHost() &&
		a.GetUsername() == b.GetUsername() &&
		a.GetPassword() == b.GetPassword() &&
		a.GetPort() == b.GetPort() &&
		a.GetMaxConnections() == b.GetMaxConnections() &&
		a.GetMaxConnectionIdleTimeInSeconds() == b.GetMaxConnectionIdleTimeInSeconds() &&
		a.GetMaxConnectionTTLInSeconds() == b.GetMaxConnectionTTLInSeconds() &&
		a.GetTLS() == b.GetTLS() &&
		a.GetInsecureSSL() == b.GetInsecureSSL() &&
		a.GetIsBackupProvider() == b.GetIsBackupProvider()
}

// getMigrationPriority assigns priority to changes (0 = highest priority)
func getMigrationPriority(changeType migration.ProviderChangeType, config ProviderConfig) int {
	switch changeType {
	case migration.ProviderChangeKeep:
		return 1000 // Lowest priority, no action needed
	case migration.ProviderChangeRemove:
		return 0 // Highest priority, free up connections first
	case migration.ProviderChangeAdd:
		if config != nil && config.GetIsBackupProvider() {
			return 3 // Lower priority for backup providers
		}
		return 2 // Higher priority for primary providers
	case migration.ProviderChangeUpdate:
		return 1 // High priority, existing provider needs updating
	default:
		return 999
	}
}

// sortChangesByPriority sorts changes by priority (0 = highest)
func sortChangesByPriority(changes []migration.ProviderChange) {
	for i := 0; i < len(changes)-1; i++ {
		for j := i + 1; j < len(changes); j++ {
			if changes[i].Priority > changes[j].Priority {
				changes[i], changes[j] = changes[j], changes[i]
			}
		}
	}
}

// generateMigrationID creates a unique ID for this migration
func generateMigrationID(oldConfig, newConfig Config) string {
	h := sha256.New()

	// Hash old config
	for _, p := range oldConfig.GetProviders() {
		_, _ = fmt.Fprintf(h, "%s:%s:%d", p.GetHost(), p.GetUsername(), p.GetMaxConnections())
	}

	// Hash new config
	for _, p := range newConfig.GetProviders() {
		_, _ = fmt.Fprintf(h, "%s:%s:%d", p.GetHost(), p.GetUsername(), p.GetMaxConnections())
	}

	return fmt.Sprintf("migration_%x", h.Sum(nil)[:8])
}