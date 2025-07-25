package nntppool

import (
	"testing"
)

func TestAnalyzeConfigurationChanges(t *testing.T) {
	tests := []struct {
		name           string
		oldConfig      Config
		newConfig      Config
		expectedChanges int
		expectedTypes  []ProviderChangeType
		hasChanges     bool
	}{
		{
			name: "no changes",
			oldConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
				},
			},
			newConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
				},
			},
			expectedChanges: 1,
			expectedTypes:  []ProviderChangeType{ProviderChangeKeep},
			hasChanges:     false,
		},
		{
			name: "add new provider",
			oldConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
				},
			},
			newConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
					{Host: "server2.example.com", Username: "user2", MaxConnections: 15},
				},
			},
			expectedChanges: 2,
			expectedTypes:  []ProviderChangeType{ProviderChangeKeep, ProviderChangeAdd},
			hasChanges:     true,
		},
		{
			name: "remove provider",
			oldConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
					{Host: "server2.example.com", Username: "user2", MaxConnections: 15},
				},
			},
			newConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
				},
			},
			expectedChanges: 2,
			expectedTypes:  []ProviderChangeType{ProviderChangeRemove, ProviderChangeKeep},
			hasChanges:     true,
		},
		{
			name: "update provider",
			oldConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
				},
			},
			newConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 20},
				},
			},
			expectedChanges: 1,
			expectedTypes:  []ProviderChangeType{ProviderChangeUpdate},
			hasChanges:     true,
		},
		{
			name: "complex changes",
			oldConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 10},
					{Host: "server2.example.com", Username: "user2", MaxConnections: 15},
					{Host: "server3.example.com", Username: "user3", MaxConnections: 8},
				},
			},
			newConfig: Config{
				Providers: []UsenetProviderConfig{
					{Host: "server1.example.com", Username: "user1", MaxConnections: 20}, // Update
					{Host: "server3.example.com", Username: "user3", MaxConnections: 8},  // Keep
					{Host: "server4.example.com", Username: "user4", MaxConnections: 12}, // Add
				},
			},
			expectedChanges: 4,
			expectedTypes:  []ProviderChangeType{ProviderChangeRemove, ProviderChangeUpdate, ProviderChangeKeep, ProviderChangeAdd},
			hasChanges:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff := AnalyzeConfigurationChanges(tt.oldConfig, tt.newConfig)

			if len(diff.Changes) != tt.expectedChanges {
				t.Errorf("expected %d changes, got %d", tt.expectedChanges, len(diff.Changes))
			}

			if diff.HasChanges != tt.hasChanges {
				t.Errorf("expected hasChanges=%v, got %v", tt.hasChanges, diff.HasChanges)
			}

			if len(diff.Changes) == len(tt.expectedTypes) {
				// Verify change types (note: order might vary due to sorting)
				changeTypeCounts := make(map[ProviderChangeType]int)
				expectedTypeCounts := make(map[ProviderChangeType]int)

				for _, change := range diff.Changes {
					changeTypeCounts[change.ChangeType]++
				}

				for _, expectedType := range tt.expectedTypes {
					expectedTypeCounts[expectedType]++
				}

				for changeType, expectedCount := range expectedTypeCounts {
					if actualCount := changeTypeCounts[changeType]; actualCount != expectedCount {
						t.Errorf("expected %d changes of type %s, got %d", expectedCount, changeType, actualCount)
					}
				}
			}

			// Verify migration ID is generated
			if diff.MigrationID == "" {
				t.Error("migration ID should not be empty")
			}
		})
	}
}

func TestProvidersEqual(t *testing.T) {
	provider1 := UsenetProviderConfig{
		Host:                           "server1.example.com",
		Username:                       "user1",
		Password:                       "pass1",
		Port:                           563,
		MaxConnections:                 10,
		MaxConnectionIdleTimeInSeconds: 300,
		MaxConnectionTTLInSeconds:      600,
		TLS:                            true,
		InsecureSSL:                    false,
		IsBackupProvider:               false,
	}

	provider2 := provider1 // Same
	provider3 := provider1
	provider3.MaxConnections = 20 // Different

	if !providersEqual(provider1, provider2) {
		t.Error("identical providers should be equal")
	}

	if providersEqual(provider1, provider3) {
		t.Error("different providers should not be equal")
	}
}

func TestGetMigrationPriority(t *testing.T) {
	tests := []struct {
		name         string
		changeType   ProviderChangeType
		config       *UsenetProviderConfig
		expectedPriority int
	}{
		{
			name:       "keep has lowest priority",
			changeType: ProviderChangeKeep,
			config:     nil,
			expectedPriority: 1000,
		},
		{
			name:       "remove has highest priority",
			changeType: ProviderChangeRemove,
			config:     nil,
			expectedPriority: 0,
		},
		{
			name:       "add primary has medium priority",
			changeType: ProviderChangeAdd,
			config:     &UsenetProviderConfig{IsBackupProvider: false},
			expectedPriority: 2,
		},
		{
			name:       "add backup has lower priority",
			changeType: ProviderChangeAdd,
			config:     &UsenetProviderConfig{IsBackupProvider: true},
			expectedPriority: 3,
		},
		{
			name:       "update has high priority",
			changeType: ProviderChangeUpdate,
			config:     nil,
			expectedPriority: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			priority := getMigrationPriority(tt.changeType, tt.config)
			if priority != tt.expectedPriority {
				t.Errorf("expected priority %d, got %d", tt.expectedPriority, priority)
			}
		})
	}
}

func TestProviderChangeTypeString(t *testing.T) {
	tests := []struct {
		changeType ProviderChangeType
		expected   string
	}{
		{ProviderChangeKeep, "keep"},
		{ProviderChangeUpdate, "update"},
		{ProviderChangeAdd, "add"},
		{ProviderChangeRemove, "remove"},
		{ProviderChangeType(999), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.changeType.String(); got != tt.expected {
			t.Errorf("ProviderChangeType(%d).String() = %q, want %q", tt.changeType, got, tt.expected)
		}
	}
}

func TestSortChangesByPriority(t *testing.T) {
	changes := []ProviderDiff{
		{ChangeType: ProviderChangeKeep, Priority: 1000},
		{ChangeType: ProviderChangeAdd, Priority: 2},
		{ChangeType: ProviderChangeRemove, Priority: 0},
		{ChangeType: ProviderChangeUpdate, Priority: 1},
	}

	sortChangesByPriority(changes)

	expectedOrder := []ProviderChangeType{
		ProviderChangeRemove,  // Priority 0
		ProviderChangeUpdate,  // Priority 1
		ProviderChangeAdd,     // Priority 2
		ProviderChangeKeep,    // Priority 1000
	}

	for i, expectedType := range expectedOrder {
		if changes[i].ChangeType != expectedType {
			t.Errorf("position %d: expected %s, got %s", i, expectedType, changes[i].ChangeType)
		}
	}
}