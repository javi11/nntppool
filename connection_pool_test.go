package nntppool

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/textproto"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()

	tests := []struct {
		expectedError error
		setup         func()
		name          string
		msgID         string
		providers     []UsenetProviderConfig
		nntpGroups    []string
		expectedBytes int64
	}{
		{
			name: "successfully downloads article body",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(100), nil)
			},
			expectedBytes: 100,
			expectedError: nil,
		},
		{
			name: "handles connection timeout error and retries",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// First attempt fails with timeout
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(nil, &net.OpError{Op: "dial", Err: io.ErrClosedPipe})

				// Second attempt succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(100), nil)
			},
			expectedBytes: 100,
			expectedError: nil,
		},
		{
			name: "handles context cancellation",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(0), context.Canceled)
			},
			expectedBytes: 0,
			expectedError: context.Canceled,
		},
		{
			name: "handles group join error",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(ErrNoSuchCapability)
			},
			expectedBytes: 0,
			expectedError: ErrNoSuchCapability,
		},
		{
			name: "handles partial download with retry",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// First attempt - partial download
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(50), syscall.EPIPE)

				// Second attempt - completes download
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(50)).Return(int64(50), nil)
			},
			expectedBytes: 50,
			expectedError: nil,
		},
		{
			name: "retries on connection error and succeeds with backup provider",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// First attempt fails
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(0), &textproto.Error{
					Code: ArticleNotFoundErrCode,
				})

				// Second attempt with backup succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"backup.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(100), nil)
			},
			expectedBytes: 100,
			expectedError: nil,
		},
		{
			name: "returns error when article not found in any provider",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<missing@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().BodyDecoded("<missing@example.com>", gomock.Any(), int64(0)).Return(int64(0), &textproto.Error{
					Code: ArticleNotFoundErrCode,
				})
			},
			expectedBytes: 0,
			expectedError: ErrArticleNotFoundInProviders,
		},
	}

	for _, tt := range tests {
		tt.setup()

		pool, err := NewConnectionPool(Config{
			Providers:                           tt.providers,
			NntpCli:                             mockClient,
			Logger:                              logger,
			SkipProvidersVerificationOnCreation: true,
			HealthCheckInterval:                 time.Hour,
		})
		assert.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			bytes, err := pool.Body(context.Background(), tt.msgID, io.Discard, tt.nntpGroups)
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBytes, bytes)
		})

		pool.Quit()
	}
}

func TestGetConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.Default()
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()

	tests := []struct {
		expectedError      error
		setup              func()
		name               string
		providers          []UsenetProviderConfig
		skipProviders      []string
		useBackupProviders bool
	}{
		{
			name: "successfully gets connection from primary provider",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			skipProviders:      []string{},
			useBackupProviders: false,
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)
			},
			expectedError: nil,
		},
		{
			name: "skips specified provider and uses backup",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			skipProviders:      []string{"primary.example.com_"},
			useBackupProviders: true,
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"backup.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)
			},
			expectedError: nil,
		},
		{
			name: "returns error when no providers available",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			skipProviders:      []string{"primary.example.com_"},
			useBackupProviders: false,
			setup:              func() {},
			expectedError:      ErrArticleNotFoundInProviders,
		},
	}

	for _, tt := range tests {
		tt.setup()

		pool, err := NewConnectionPool(Config{
			Providers:                           tt.providers,
			NntpCli:                             mockClient,
			Logger:                              logger,
			SkipProvidersVerificationOnCreation: true,
			HealthCheckInterval:                 time.Hour,
		})
		assert.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			conn, err := pool.GetConnection(context.Background(), tt.skipProviders, tt.useBackupProviders)
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			conn.Free()

			assert.NoError(t, err)
			assert.NotNil(t, conn)
		})

		pool.Quit()
	}
}

func TestPost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()

	tests := []struct {
		articleData   io.Reader
		expectedError error
		setup         func()
		name          string
		providers     []UsenetProviderConfig
	}{
		{
			name: "successfully posts article",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "handles temporary error and retries successfully",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// First attempt fails with temporary error
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(&net.OpError{Op: "write", Err: syscall.EPIPE})

				// Second attempt succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "handles context cancellation",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(context.Canceled)
			},
			expectedError: context.Canceled,
		},
		{
			name: "retries with when there is a failure",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// Primary provider fails
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil).MaxTimes(2)

				mockConn.EXPECT().Post(gomock.Any()).Return(&net.OpError{Op: "write", Err: syscall.EPIPE})
				mockConn.EXPECT().Post(gomock.Any()).Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "rotates providers when segment already exists",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// Primary provider fails with segment already exists
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(&textproto.Error{
					Code: SegmentAlreadyExistsErrCode,
				})

				// Backup provider succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"backup.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "converts ErrArticleNotFoundInProviders to ErrFailedToPostInAllProviders",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			articleData: strings.NewReader("test article data"),
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// Primary provider fails with article not found
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Post(gomock.Any()).Return(&textproto.Error{
					Code: SegmentAlreadyExistsErrCode,
				})
			},
			expectedError: ErrFailedToPostInAllProviders,
		},
	}

	for _, tt := range tests {
		tt.setup()

		pool, err := NewConnectionPool(Config{
			Providers:                           tt.providers,
			NntpCli:                             mockClient,
			Logger:                              logger,
			SkipProvidersVerificationOnCreation: true,
			HealthCheckInterval:                 time.Hour,
		})
		assert.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			err := pool.Post(context.Background(), tt.articleData)
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			assert.NoError(t, err)
		})

		pool.Quit()
	}
}

func TestStat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()

	tests := []struct {
		expectedError error
		setup         func()
		name          string
		msgID         string
		providers     []UsenetProviderConfig
		nntpGroups    []string
	}{
		{
			name: "successfully checks article status",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<test@example.com>").Return(1, nil)
			},
			expectedError: nil,
		},
		{
			name: "handles connection timeout error and retries",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// First attempt fails with timeout
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(nil, &net.OpError{Op: "dial", Err: io.ErrClosedPipe})

				// Second attempt succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<test@example.com>").Return(1, nil)
			},
			expectedError: nil,
		},
		{
			name: "handles context cancellation",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<test@example.com>").Return(0, context.Canceled)
			},
			expectedError: context.Canceled,
		},
		{
			name: "retries with backup provider when primary fails",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// Primary provider fails
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<test@example.com>").Return(0, &net.OpError{Op: "write", Err: syscall.EPIPE})

				// Second attempt with primary succeeds
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<test@example.com>").Return(1, nil)
			},
			expectedError: nil,
		},
		{
			name: "returns article not found error when all providers fail",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
				{
					Host:                      "backup.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
					IsBackupProvider:          true,
				},
			},
			msgID:      "<missing@example.com>",
			nntpGroups: []string{},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				// Primary provider fails with article not found
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<missing@example.com>").Return(0, &textproto.Error{
					Code: ArticleNotFoundErrCode,
				})

				// Backup provider also fails with article not found
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"backup.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().Stat("<missing@example.com>").Return(0, &textproto.Error{
					Code: ArticleNotFoundErrCode,
				})
			},
			expectedError: ErrArticleNotFoundInProviders,
		},
		{
			name: "handles group join before stat",
			providers: []UsenetProviderConfig{
				{
					Host:                      "primary.example.com",
					Port:                      119,
					MaxConnections:            1,
					MaxConnectionTTLInSeconds: 2400,
				},
			},
			msgID:      "<test@example.com>",
			nntpGroups: []string{"alt.test"},
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil)

				mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
				mockConn.EXPECT().Stat("<test@example.com>").Return(1, nil)
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		tt.setup()

		pool, err := NewConnectionPool(Config{
			Providers:                           tt.providers,
			NntpCli:                             mockClient,
			Logger:                              logger,
			SkipProvidersVerificationOnCreation: true,
			HealthCheckInterval:                 time.Hour,
		})
		assert.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			_, err := pool.Stat(context.Background(), tt.msgID, tt.nntpGroups)
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			assert.NoError(t, err)
		})

		pool.Quit()
	}
}

func TestHotReload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()

	initialProviders := []UsenetProviderConfig{
		{
			Host:                      "primary.example.com",
			Port:                      119,
			MaxConnections:            1,
			MaxConnectionTTLInSeconds: 2400,
		},
	}

	newProviders := []UsenetProviderConfig{
		{
			Host:                      "new.example.com",
			Port:                      119,
			MaxConnections:            2,
			MaxConnectionTTLInSeconds: 3600,
		},
	}

	tests := []struct {
		expectedError error
		setup         func()
		name          string
		newConfig     Config
	}{
		{
			name: "successfully reloads configuration",
			setup: func() {
				ttl := time.Duration(2400) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"primary.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil).MaxTimes(2)

				mockConn.EXPECT().Capabilities().Return([]string{}, nil).MaxTimes(2)

				// Expect dial for new configuration
				ttl = time.Duration(3600) * time.Second
				mockClient.EXPECT().Dial(
					gomock.Any(),
					"new.example.com",
					119,
					nntpcli.DialConfig{
						KeepAliveTime: ttl,
					},
				).Return(mockConn, nil).MaxTimes(2)

				mockConn.EXPECT().Capabilities().Return([]string{}, nil).MaxTimes(2)
			},
			newConfig: Config{
				Providers:                           newProviders,
				NntpCli:                             mockClient,
				Logger:                              logger,
				SkipProvidersVerificationOnCreation: false,
				HealthCheckInterval:                 time.Hour,
			},
			expectedError: nil,
		},
		{
			name:  "handles empty providers list",
			setup: func() {},
			newConfig: Config{
				Providers:                           []UsenetProviderConfig{},
				NntpCli:                             mockClient,
				Logger:                              logger,
				SkipProvidersVerificationOnCreation: true,
				HealthCheckInterval:                 time.Hour,
			},
			expectedError: ErrNoProviderAvailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			pool, err := NewConnectionPool(Config{
				Providers:                           initialProviders,
				NntpCli:                             mockClient,
				Logger:                              logger,
				SkipProvidersVerificationOnCreation: true,
				HealthCheckInterval:                 time.Hour,
			})
			assert.NoError(t, err)

			err = pool.HotReload(tt.newConfig)
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
				return
			}

			assert.NoError(t, err)

			// Verify the new configuration is active
			providers := pool.GetProvidersInfo()
			assert.Equal(t, len(tt.newConfig.Providers), len(providers))

			if len(providers) > 0 {
				assert.Equal(t, tt.newConfig.Providers[0].Host, providers[0].Host)
				assert.Equal(t, tt.newConfig.Providers[0].MaxConnections, providers[0].MaxConnections)
			}

			pool.Quit()
		})
	}
}
