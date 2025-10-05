package nntppool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/textproto"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2/pkg/nntpcli"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestStandaloneTestProviderConnectivity(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func(*gomock.Controller) nntpcli.Client
		config        UsenetProviderConfig
		expectedError bool
		errorContains string
		description   string
	}{
		{
			name: "successful connectivity test",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
				mockConn.EXPECT().Close().AnyTimes()

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:           "test.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 5,
			},
			expectedError: false,
			description:   "should succeed with working provider",
		},
		{
			name: "successful connectivity test with capabilities",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
				mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil)
				mockConn.EXPECT().Close().AnyTimes()

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:               "test.example.com",
				Port:               119,
				Username:           "testuser",
				Password:           "testpass",
				MaxConnections:     5,
				VerifyCapabilities: []string{"READER"},
			},
			expectedError: false,
			description:   "should succeed with working provider",
		},
		{
			name: "connection failure",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockClient.EXPECT().Dial(gomock.Any(), "broken.example.com", 119, gomock.Any()).
					Return(nil, errors.New("connection refused"))

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:               "broken.example.com",
				Port:               119,
				Username:           "testuser",
				Password:           "testpass",
				MaxConnections:     5,
				VerifyCapabilities: []string{"READER", "POST"},
			},
			expectedError: true,
			errorContains: "failed to connect to provider broken.example.com",
			description:   "should fail with connection error",
		},
		{
			name: "authentication failure",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().Dial(gomock.Any(), "auth-fail.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("user", "wrongpass").
					Return(&textproto.Error{Code: AuthenticationFailedCode, Msg: "Authentication failed"})
				mockConn.EXPECT().Close().AnyTimes()

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:               "auth-fail.example.com",
				Port:               119,
				Username:           "user",
				Password:           "wrongpass",
				MaxConnections:     5,
				VerifyCapabilities: []string{"READER", "POST"},
			},
			expectedError: true,
			errorContains: "authentication failed for provider auth-fail.example.com",
			description:   "should fail with authentication error",
		},
		{
			name: "capabilities failure",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().Dial(gomock.Any(), "caps-fail.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
				mockConn.EXPECT().Capabilities().Return(nil, errors.New("capabilities failed"))
				mockConn.EXPECT().Close().AnyTimes()

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:               "caps-fail.example.com",
				Port:               119,
				Username:           "testuser",
				Password:           "testpass",
				MaxConnections:     5,
				VerifyCapabilities: []string{"READER", "POST"},
			},
			expectedError: true,
			errorContains: "failed to get capabilities from provider caps-fail.example.com",
			description:   "should fail with capabilities error",
		},
		{
			name: "capabilities authentication failure",
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().Dial(gomock.Any(), "caps-auth-fail.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
				mockConn.EXPECT().Capabilities().
					Return(nil, &textproto.Error{Code: AuthenticationFailedCode, Msg: "Authentication required for capabilities"})
				mockConn.EXPECT().Close().AnyTimes()

				return mockClient
			},
			config: UsenetProviderConfig{
				Host:               "caps-auth-fail.example.com",
				Port:               119,
				Username:           "testuser",
				Password:           "testpass",
				MaxConnections:     5,
				VerifyCapabilities: []string{"READER", "POST"},
			},
			expectedError: true,
			errorContains: "authentication failed during capabilities check for provider caps-auth-fail.example.com",
			description:   "should fail with capabilities authentication error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := tt.setupMocks(ctrl)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			ctx := context.Background()
			err := TestProviderConnectivity(ctx, tt.config, logger, mockClient)

			if tt.expectedError {
				assert.Error(t, err, tt.description)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

func TestStandaloneTestProviderConnectivityWithCapabilityRequirements(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup mock to return capabilities without required one
	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		Return(mockConn, nil)
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil) // Missing "POST"
	mockConn.EXPECT().Close().AnyTimes()

	config := UsenetProviderConfig{
		Host:               "test.example.com",
		Port:               119,
		Username:           "testuser",
		Password:           "testpass",
		MaxConnections:     5,
		VerifyCapabilities: []string{"READER", "POST"}, // Require POST capability
	}

	ctx := context.Background()
	err := TestProviderConnectivity(ctx, config, logger, mockClient)

	assert.Error(t, err, "should fail when required capability is missing")
	assert.Contains(t, err.Error(), "does not support required capability POST")
}

func TestStandaloneTestProviderConnectivityWithAllCapabilities(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup mock to return all required capabilities
	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		Return(mockConn, nil)
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
	mockConn.EXPECT().Capabilities().Return([]string{"READER", "POST", "IHAVE"}, nil)
	mockConn.EXPECT().Close().AnyTimes()

	config := UsenetProviderConfig{
		Host:               "test.example.com",
		Port:               119,
		Username:           "testuser",
		Password:           "testpass",
		MaxConnections:     5,
		VerifyCapabilities: []string{"READER", "POST"},
	}

	ctx := context.Background()
	err := TestProviderConnectivity(ctx, config, logger, mockClient)

	assert.NoError(t, err, "should succeed when all required capabilities are present")
}

func TestStandaloneTestProviderConnectivityNilClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := UsenetProviderConfig{
		Host:           "test.example.com",
		Port:           119,
		Username:       "testuser",
		Password:       "testpass",
		MaxConnections: 5,
	}

	ctx := context.Background()
	// This should now work by creating a default client, but will likely fail to connect
	// since we're not mocking the actual network calls for the default client
	err := TestProviderConnectivity(ctx, config, logger, nil)

	// Should fail with connection error (since it's trying to make a real connection)
	// but not with "client cannot be nil" error
	assert.Error(t, err, "should fail with connection error when using default client")
	assert.NotContains(t, err.Error(), "client cannot be nil", "should not fail due to nil client")
}

func TestStandaloneTestProviderConnectivityNilLogger(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)

	// Setup successful mock
	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		Return(mockConn, nil)
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil)
	mockConn.EXPECT().Close().AnyTimes()

	config := UsenetProviderConfig{
		Host:               "test.example.com",
		Port:               119,
		Username:           "testuser",
		Password:           "testpass",
		MaxConnections:     5,
		VerifyCapabilities: []string{"READER"},
	}

	ctx := context.Background()
	// Pass nil logger - should use default logger
	err := TestProviderConnectivity(ctx, config, nil, mockClient)

	assert.NoError(t, err, "should succeed with nil logger (uses default)")
}

func TestStandaloneTestProviderConnectivityTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Mock a slow connection that will timeout
	mockClient.EXPECT().Dial(gomock.Any(), "slow.example.com", 119, gomock.Any()).
		DoAndReturn(func(ctx context.Context, host string, port int, config nntpcli.DialConfig) (nntpcli.Connection, error) {
			select {
			case <-time.After(5 * time.Second): // Simulate slow connection
				return nil, errors.New("connection timeout")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})

	config := UsenetProviderConfig{
		Host:           "slow.example.com",
		Port:           119,
		Username:       "testuser",
		Password:       "testpass",
		MaxConnections: 5,
	}

	// Test with a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := TestProviderConnectivity(ctx, config, logger, mockClient)
	assert.Error(t, err, "should timeout")
	// Should be a connection error, not an authentication error
	assert.Contains(t, err.Error(), "failed to connect to provider slow.example.com")
}

func TestStandaloneTestProviderConnectivityNoAuth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup mock for connection without authentication
	mockClient.EXPECT().Dial(gomock.Any(), "noauth.example.com", 119, gomock.Any()).
		Return(mockConn, nil)
	// No authentication expectation since username/password are empty
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil)
	mockConn.EXPECT().Close().AnyTimes()

	config := UsenetProviderConfig{
		Host:               "noauth.example.com",
		Port:               119,
		Username:           "", // No username
		Password:           "", // No password
		MaxConnections:     5,
		VerifyCapabilities: []string{"READER"},
	}

	ctx := context.Background()
	err := TestProviderConnectivity(ctx, config, logger, mockClient)

	assert.NoError(t, err, "should succeed without authentication when no credentials provided")
}

func TestStandaloneTestProviderConnectivityTLS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup mock for TLS connection
	mockClient.EXPECT().DialTLS(gomock.Any(), "secure.example.com", 563, false, gomock.Any()).
		Return(mockConn, nil)
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil)
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil)
	mockConn.EXPECT().Close().AnyTimes()

	config := UsenetProviderConfig{
		Host:               "secure.example.com",
		Port:               563,
		Username:           "testuser",
		Password:           "testpass",
		MaxConnections:     5,
		TLS:                true,
		InsecureSSL:        false,
		VerifyCapabilities: []string{"READER"},
	}

	ctx := context.Background()
	err := TestProviderConnectivity(ctx, config, logger, mockClient)

	assert.NoError(t, err, "should succeed with TLS connection")
}

func TestStandaloneTestProviderConnectivityDefaultClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := UsenetProviderConfig{
		Host:           "nonexistent.example.com", // Use a non-existent host to ensure test fails quickly
		Port:           119,
		Username:       "testuser",
		Password:       "testpass",
		MaxConnections: 5,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Test calling without client parameter - should use default client
	err := TestProviderConnectivity(ctx, config, logger, nil)

	// Should fail with connection error since we're using a non-existent host
	assert.Error(t, err, "should fail with default client trying to connect to non-existent host")
	assert.Contains(t, err.Error(), "failed to connect to provider nonexistent.example.com")
}
