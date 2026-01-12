package nntppool

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
	"go.uber.org/mock/gomock"
)

func TestCreateProxyDialer(t *testing.T) {
	tests := []struct {
		name        string
		proxyURL    string
		wantNil     bool
		wantErr     bool
		errContains string
	}{
		{
			name:     "empty proxy URL returns nil",
			proxyURL: "",
			wantNil:  true,
			wantErr:  false,
		},
		{
			name:     "valid socks5 URL without auth",
			proxyURL: "socks5://proxy.example.com:1080",
			wantNil:  false,
			wantErr:  false,
		},
		{
			name:     "valid socks5 URL with auth",
			proxyURL: "socks5://user:pass@proxy.example.com:1080",
			wantNil:  false,
			wantErr:  false,
		},
		{
			name:     "valid socks5 URL with special chars in password",
			proxyURL: "socks5://user:p%40ss%23word@proxy.example.com:1080",
			wantNil:  false,
			wantErr:  false,
		},
		{
			name:        "unsupported http scheme",
			proxyURL:    "http://proxy.example.com:8080",
			wantNil:     true,
			wantErr:     true,
			errContains: "unsupported proxy scheme",
		},
		{
			name:        "unsupported https scheme",
			proxyURL:    "https://proxy.example.com:8080",
			wantNil:     true,
			wantErr:     true,
			errContains: "unsupported proxy scheme",
		},
		{
			name:        "invalid URL format",
			proxyURL:    "://invalid",
			wantNil:     true,
			wantErr:     true,
			errContains: "invalid proxy URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := createProxyDialer(tt.proxyURL)

			if tt.wantErr {
				if err == nil {
					t.Errorf("createProxyDialer() expected error, got nil")
					return
				}
				if tt.errContains != "" {
					if !contains(err.Error(), tt.errContains) {
						t.Errorf("createProxyDialer() error = %v, want error containing %q", err, tt.errContains)
					}
				}
				return
			}

			if err != nil {
				t.Errorf("createProxyDialer() unexpected error = %v", err)
				return
			}

			if tt.wantNil && dialer != nil {
				t.Errorf("createProxyDialer() = %v, want nil", dialer)
			}

			if !tt.wantNil && dialer == nil {
				t.Errorf("createProxyDialer() = nil, want non-nil dialer")
			}
		})
	}
}

func TestIsProxyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "socks error",
			err:  &net.OpError{Op: "socks connect", Err: net.ErrClosed},
			want: true,
		},
		{
			name: "proxy error in message",
			err:  net.UnknownNetworkError("proxy connection failed"),
			want: true,
		},
		{
			name: "regular connection error",
			err:  &net.OpError{Op: "dial", Err: net.ErrClosed},
			want: false,
		},
		{
			name: "regular timeout error",
			err:  context.DeadlineExceeded,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isProxyError(tt.err)
			if got != tt.want {
				t.Errorf("isProxyError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSocks5ContextDialer_DialContext_ContextCancellation(t *testing.T) {
	// Create a mock dialer that blocks
	mockDialer := &slowDialer{delay: 5 * time.Second}
	dialer := &socks5ContextDialer{dialer: mockDialer}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := dialer.DialContext(ctx, "tcp", "example.com:80")
	elapsed := time.Since(start)

	if err != context.DeadlineExceeded {
		t.Errorf("DialContext() error = %v, want %v", err, context.DeadlineExceeded)
	}

	// Should return quickly due to context cancellation, not wait for the slow dialer
	if elapsed > 500*time.Millisecond {
		t.Errorf("DialContext() took %v, expected to return quickly on context cancellation", elapsed)
	}
}

// slowDialer is a mock dialer that delays before returning
type slowDialer struct {
	delay time.Duration
}

func (s *slowDialer) Dial(network, address string) (net.Conn, error) {
	time.Sleep(s.delay)
	return nil, net.ErrClosed
}

// contains checks if s contains substr (case-sensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDialNNTP_WithProxy(t *testing.T) {
	tests := []struct {
		name        string
		config      UsenetProviderConfig
		setupMocks  func(*gomock.Controller) nntpcli.Client
		wantErr     bool
		errContains string
	}{
		{
			name: "successful connection without proxy",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "pass",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().
					Dial(gomock.Any(), "news.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("user", "pass").Return(nil)

				return mockClient
			},
			wantErr: false,
		},
		{
			name: "successful TLS connection without proxy",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      563,
				TLS:                       true,
				Username:                  "user",
				Password:                  "pass",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().
					DialTLS(gomock.Any(), "news.example.com", 563, false, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("user", "pass").Return(nil)

				return mockClient
			},
			wantErr: false,
		},
		{
			name: "connection with proxy passes dialer config",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "pass",
				ProxyURL:                  "socks5://proxy.example.com:1080",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				// Verify that a DialConfig with non-nil Dialer is passed
				mockClient.EXPECT().
					Dial(gomock.Any(), "news.example.com", 119, gomock.Any()).
					DoAndReturn(func(ctx context.Context, host string, port int, config ...nntpcli.DialConfig) (nntpcli.Connection, error) {
						if len(config) == 0 {
							t.Error("expected DialConfig to be passed")
							return nil, errors.New("no config")
						}
						if config[0].Dialer == nil {
							t.Error("expected Dialer to be set in DialConfig when ProxyURL is configured")
							return nil, errors.New("no dialer")
						}
						return mockConn, nil
					})
				mockConn.EXPECT().Authenticate("user", "pass").Return(nil)

				return mockClient
			},
			wantErr: false,
		},
		{
			name: "TLS connection with proxy passes dialer config",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      563,
				TLS:                       true,
				Username:                  "user",
				Password:                  "pass",
				ProxyURL:                  "socks5://user:pass@proxy.example.com:1080",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				// Verify that a DialConfig with non-nil Dialer is passed
				mockClient.EXPECT().
					DialTLS(gomock.Any(), "news.example.com", 563, false, gomock.Any()).
					DoAndReturn(func(ctx context.Context, host string, port int, insecureSSL bool, config ...nntpcli.DialConfig) (nntpcli.Connection, error) {
						if len(config) == 0 {
							t.Error("expected DialConfig to be passed")
							return nil, errors.New("no config")
						}
						if config[0].Dialer == nil {
							t.Error("expected Dialer to be set in DialConfig when ProxyURL is configured")
							return nil, errors.New("no dialer")
						}
						return mockConn, nil
					})
				mockConn.EXPECT().Authenticate("user", "pass").Return(nil)

				return mockClient
			},
			wantErr: false,
		},
		{
			name: "invalid proxy URL returns error",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "pass",
				ProxyURL:                  "http://proxy.example.com:8080", // unsupported scheme
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				// No dial should be attempted with invalid proxy
				return nntpcli.NewMockClient(ctrl)
			},
			wantErr:     true,
			errContains: "unsupported proxy scheme",
		},
		{
			name: "malformed proxy URL returns error",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "pass",
				ProxyURL:                  "://invalid",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				return nntpcli.NewMockClient(ctrl)
			},
			wantErr:     true,
			errContains: "invalid proxy URL",
		},
		{
			name: "connection without auth succeeds",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().
					Dial(gomock.Any(), "news.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				// No Authenticate call expected

				return mockClient
			},
			wantErr: false,
		},
		{
			name: "dial failure returns error",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "pass",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)

				mockClient.EXPECT().
					Dial(gomock.Any(), "news.example.com", 119, gomock.Any()).
					Return(nil, errors.New("connection refused"))

				return mockClient
			},
			wantErr:     true,
			errContains: "error dialing",
		},
		{
			name: "authentication failure returns error",
			config: UsenetProviderConfig{
				Host:                      "news.example.com",
				Port:                      119,
				Username:                  "user",
				Password:                  "wrongpass",
				MaxConnectionTTLInSeconds: 3600,
			},
			setupMocks: func(ctrl *gomock.Controller) nntpcli.Client {
				mockClient := nntpcli.NewMockClient(ctrl)
				mockConn := nntpcli.NewMockConnection(ctrl)

				mockClient.EXPECT().
					Dial(gomock.Any(), "news.example.com", 119, gomock.Any()).
					Return(mockConn, nil)
				mockConn.EXPECT().Authenticate("user", "wrongpass").
					Return(errors.New("authentication failed"))

				return mockClient
			},
			wantErr:     true,
			errContains: "error authenticating",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := tt.setupMocks(ctrl)
			logger := slog.Default()

			conn, err := dialNNTP(context.Background(), mockClient, tt.config, logger)

			if tt.wantErr {
				if err == nil {
					t.Errorf("dialNNTP() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("dialNNTP() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("dialNNTP() unexpected error = %v", err)
				return
			}

			if conn == nil {
				t.Error("dialNNTP() returned nil connection")
			}
		})
	}
}

func TestSocks5ContextDialer_DialContext_Success(t *testing.T) {
	// Create a mock dialer that succeeds
	mockConn := &mockNetConn{}
	mockDialer := &successDialer{conn: mockConn}
	dialer := &socks5ContextDialer{dialer: mockDialer}

	ctx := context.Background()
	conn, err := dialer.DialContext(ctx, "tcp", "example.com:80")

	if err != nil {
		t.Errorf("DialContext() unexpected error = %v", err)
	}

	if conn != mockConn {
		t.Errorf("DialContext() returned wrong connection")
	}
}

func TestSocks5ContextDialer_DialContext_Error(t *testing.T) {
	// Create a mock dialer that fails
	expectedErr := errors.New("connection refused")
	mockDialer := &errorDialer{err: expectedErr}
	dialer := &socks5ContextDialer{dialer: mockDialer}

	ctx := context.Background()
	conn, err := dialer.DialContext(ctx, "tcp", "example.com:80")

	if err == nil {
		t.Error("DialContext() expected error, got nil")
	}

	if conn != nil {
		t.Error("DialContext() expected nil connection on error")
	}
}

func TestCreateProxyDialer_URLParsing(t *testing.T) {
	tests := []struct {
		name     string
		proxyURL string
		wantErr  bool
	}{
		{
			name:     "socks5 with IPv4 address",
			proxyURL: "socks5://192.168.1.1:1080",
			wantErr:  false,
		},
		{
			name:     "socks5 with IPv6 address",
			proxyURL: "socks5://[::1]:1080",
			wantErr:  false,
		},
		{
			name:     "socks5 with hostname",
			proxyURL: "socks5://proxy.example.com:1080",
			wantErr:  false,
		},
		{
			name:     "socks5 with username only",
			proxyURL: "socks5://user@proxy.example.com:1080",
			wantErr:  false,
		},
		{
			name:     "socks5 with empty password",
			proxyURL: "socks5://user:@proxy.example.com:1080",
			wantErr:  false,
		},
		{
			name:     "socks5 with URL encoded credentials",
			proxyURL: "socks5://user%40domain:p%40ss%3Aword@proxy.example.com:1080",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := createProxyDialer(tt.proxyURL)

			if tt.wantErr {
				if err == nil {
					t.Errorf("createProxyDialer() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("createProxyDialer() unexpected error = %v", err)
				return
			}

			if dialer == nil {
				t.Error("createProxyDialer() returned nil dialer")
			}
		})
	}
}

func TestUsenetProviderConfig_GetProxyURL(t *testing.T) {
	tests := []struct {
		name     string
		config   UsenetProviderConfig
		expected string
	}{
		{
			name:     "empty proxy URL",
			config:   UsenetProviderConfig{Host: "news.example.com"},
			expected: "",
		},
		{
			name: "with proxy URL",
			config: UsenetProviderConfig{
				Host:     "news.example.com",
				ProxyURL: "socks5://proxy.example.com:1080",
			},
			expected: "socks5://proxy.example.com:1080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetProxyURL()
			if got != tt.expected {
				t.Errorf("GetProxyURL() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// successDialer is a mock dialer that returns a successful connection
type successDialer struct {
	conn net.Conn
}

func (s *successDialer) Dial(network, address string) (net.Conn, error) {
	return s.conn, nil
}

// errorDialer is a mock dialer that returns an error
type errorDialer struct {
	err error
}

func (e *errorDialer) Dial(network, address string) (net.Conn, error) {
	return nil, e.err
}

// mockNetConn is a minimal mock implementation of net.Conn
type mockNetConn struct{}

func (m *mockNetConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockNetConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockNetConn) Close() error                       { return nil }
func (m *mockNetConn) LocalAddr() net.Addr                { return nil }
func (m *mockNetConn) RemoteAddr() net.Addr               { return nil }
func (m *mockNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConn) SetWriteDeadline(t time.Time) error { return nil }
