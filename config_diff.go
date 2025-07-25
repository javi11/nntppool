package nntppool

import (
	"crypto/sha256"
	"fmt"
)

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

// ProviderDiff represents a change to be made to a provider
type ProviderDiff struct {
	ID         string                `json:"id"`
	ChangeType ProviderChangeType    `json:"change_type"`
	OldConfig  *UsenetProviderConfig `json:"old_config,omitempty"`
	NewConfig  *UsenetProviderConfig `json:"new_config,omitempty"`
	Priority   int                   `json:"priority"` // Migration priority (0 = highest)
}

// ConfigDiff represents the complete set of changes between two configurations
type ConfigDiff struct {
	MigrationID string         `json:"migration_id"`
	Changes     []ProviderDiff `json:"changes"`
	HasChanges  bool           `json:"has_changes"`
}

// AnalyzeConfigurationChanges compares old and new configurations and returns a diff
func AnalyzeConfigurationChanges(oldConfig, newConfig Config) ConfigDiff {
	migrationID := generateMigrationID(oldConfig, newConfig)

	oldProviders := make(map[string]UsenetProviderConfig)
	newProviders := make(map[string]UsenetProviderConfig)

	// Build maps of providers by ID
	for _, p := range oldConfig.Providers {
		oldProviders[p.ID()] = p
	}

	for _, p := range newConfig.Providers {
		newProviders[p.ID()] = p
	}

	var changes []ProviderDiff
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

		var change ProviderDiff
		change.ID = id

		switch {
		case existsInOld && existsInNew:
			// Provider exists in both - check if changed
			if providersEqual(oldProvider, newProvider) {
				change.ChangeType = ProviderChangeKeep
			} else {
				change.ChangeType = ProviderChangeUpdate
				change.OldConfig = &oldProvider
				change.NewConfig = &newProvider
			}
		case existsInOld && !existsInNew:
			// Provider removed
			change.ChangeType = ProviderChangeRemove
			change.OldConfig = &oldProvider
		case !existsInOld && existsInNew:
			// Provider added
			change.ChangeType = ProviderChangeAdd
			change.NewConfig = &newProvider
		}

		// Assign migration priority
		change.Priority = getMigrationPriority(change.ChangeType, change.NewConfig)

		changes = append(changes, change)
	}

	// Sort changes by priority
	sortChangesByPriority(changes)

	hasChanges := false
	for _, change := range changes {
		if change.ChangeType != ProviderChangeKeep {
			hasChanges = true
			break
		}
	}

	return ConfigDiff{
		MigrationID: migrationID,
		Changes:     changes,
		HasChanges:  hasChanges,
	}
}

// providersEqual compares two provider configurations for equality
func providersEqual(a, b UsenetProviderConfig) bool {
	return a.Host == b.Host &&
		a.Username == b.Username &&
		a.Password == b.Password &&
		a.Port == b.Port &&
		a.MaxConnections == b.MaxConnections &&
		a.MaxConnectionIdleTimeInSeconds == b.MaxConnectionIdleTimeInSeconds &&
		a.MaxConnectionTTLInSeconds == b.MaxConnectionTTLInSeconds &&
		a.TLS == b.TLS &&
		a.InsecureSSL == b.InsecureSSL &&
		a.IsBackupProvider == b.IsBackupProvider
}

// getMigrationPriority assigns priority to changes (0 = highest priority)
func getMigrationPriority(changeType ProviderChangeType, config *UsenetProviderConfig) int {
	switch changeType {
	case ProviderChangeKeep:
		return 1000 // Lowest priority, no action needed
	case ProviderChangeRemove:
		return 0 // Highest priority, free up connections first
	case ProviderChangeAdd:
		if config != nil && config.IsBackupProvider {
			return 3 // Lower priority for backup providers
		}
		return 2 // Higher priority for primary providers
	case ProviderChangeUpdate:
		return 1 // High priority, existing provider needs updating
	default:
		return 999
	}
}

// sortChangesByPriority sorts changes by priority (0 = highest)
func sortChangesByPriority(changes []ProviderDiff) {
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
	for _, p := range oldConfig.Providers {
		_, _ = fmt.Fprintf(h, "%s:%s:%d", p.Host, p.Username, p.MaxConnections)
	}

	// Hash new config
	for _, p := range newConfig.Providers {
		_, _ = fmt.Fprintf(h, "%s:%s:%d", p.Host, p.Username, p.MaxConnections)
	}

	return fmt.Sprintf("migration_%x", h.Sum(nil)[:8])
}
