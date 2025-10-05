package nntpcli

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDial(t *testing.T) {
	// create a mock server
	mockServer, e := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, e)

	var mx sync.RWMutex

	closed := false

	defer func() {
		mx.Lock()

		closed = true

		_ = mockServer.Close()
		mx.Unlock()
	}()

	// create a goroutine to accept incoming connections
	go func() {
		for {
			mx.RLock()
			if closed {
				mx.RUnlock()
				return
			}
			mx.RUnlock()

			conn, err := mockServer.Accept()
			if err != nil {
				return
			}

			_, _ = conn.Write([]byte("200 mock server ready\r\n"))
			_ = conn.Close()
		}
	}()

	s := strings.Split(mockServer.Addr().String(), ":")
	host := s[0]

	port, err := strconv.Atoi(s[1])
	assert.NoError(t, err)

	// Test successful connection
	t.Run("Dial", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		config := DialConfig{
			KeepAliveTime: 10 * time.Second,
			DialTimeout:   5 * time.Second,
		}
		conn, err := c.Dial(context.Background(), host, port, config)
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		assert.True(t, conn.MaxAgeTime().After(time.Now()))
		assert.True(t, conn.MaxAgeTime().After(time.Now().Add(9*time.Second)))
	})

	// Test connection to non-existent server
	t.Run("DialFail", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		config := DialConfig{
			DialTimeout: 5 * time.Second,
		}
		_, err := c.Dial(context.Background(), "127.0.0.1", 12345, config)
		assert.Error(t, err)
	})

	// Test connection with nil timeout
	t.Run("DialWithNilTimeout", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		conn, err := c.Dial(context.Background(), host, port, DialConfig{})
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})

	// Test connection with zero timeout
	t.Run("DialWithZeroTimeout", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		config := DialConfig{
			DialTimeout: time.Duration(0),
		}
		conn, err := c.Dial(context.Background(), host, port, config)
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})

	// Test connection with canceled context
	t.Run("DialWithCanceledContext", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		config := DialConfig{
			DialTimeout: 5 * time.Second,
		}
		_, err := c.Dial(ctx, host, port, config)
		assert.Error(t, err)
	})
}

func TestDialTLS(t *testing.T) {
	// create a mock TLS server
	cert, err := tls.X509KeyPair([]byte(testCert), []byte(testKey))
	assert.NoError(t, err)

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	mockServer, err := tls.Listen("tcp", "127.0.0.1:0", config)
	assert.NoError(t, err)

	var mx sync.RWMutex

	closed := false

	defer func() {
		mx.Lock()

		closed = true

		_ = mockServer.Close()
		mx.Unlock()
	}()

	// create a goroutine to accept incoming connections
	go func() {
		for {
			mx.RLock()
			if closed {
				mx.RUnlock()
				return
			}
			mx.RUnlock()

			conn, e := mockServer.Accept()
			if e != nil {
				return
			}

			_, _ = conn.Write([]byte("200 mock server ready\r\n"))
			_ = conn.Close()
		}
	}()

	s := strings.Split(mockServer.Addr().String(), ":")
	host := s[0]

	port, err := strconv.Atoi(s[1])
	assert.NoError(t, err)

	// Test successful TLS connection with insecure SSL
	t.Run("DialTLS", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		config := DialConfig{
			KeepAliveTime: 10 * time.Second,
			DialTimeout:   5 * time.Second,
		}
		conn, err := c.DialTLS(context.Background(), host, port, true, config)
		assert.NoError(t, err)
		assert.NotNil(t, conn)
		assert.True(t, conn.MaxAgeTime().After(time.Now()))
	})

	// Test TLS connection with secure SSL (should fail with self-signed cert)
	t.Run("DialTLSSecure", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		config := DialConfig{
			DialTimeout: 5 * time.Second,
		}
		_, err := c.DialTLS(context.Background(), host, port, false, config)
		assert.Error(t, err)
	})

	// Test TLS connection with nil timeout
	t.Run("DialTLSWithNilTimeout", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}
		conn, err := c.DialTLS(context.Background(), host, port, true, DialConfig{})
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})

	// Test TLS connection with canceled context
	t.Run("DialTLSWithCanceledContext", func(t *testing.T) {
		c := &client{keepAliveTime: 5 * time.Second}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		config := DialConfig{
			DialTimeout: 5 * time.Second,
		}
		_, err := c.DialTLS(ctx, host, port, true, config)
		assert.Error(t, err)
	})
}

// Self-signed test certificate and key
const testCert = `-----BEGIN CERTIFICATE-----
MIIDzzCCAregAwIBAgIUVN8hNBbQkGt61hGioSVvFVfsKOcwDQYJKoZIhvcNAQEL
BQAwdjELMAkGA1UEBhMCZXMxDTALBgNVBAgMBHRlc3QxDTALBgNVBAcMBHRlc3Qx
DTALBgNVBAoMBHRlc3QxDTALBgNVBAsMBHRlc3QxDTALBgNVBAMMBHRlc3QxHDAa
BgkqhkiG9w0BCQEWDXRlc3RAdGVzdC5jb20wIBcNMjUwMTI0MDczNDQ1WhgPMjA1
MDA5MTUwNzM0NDVaMHYxCzAJBgNVBAYTAmVzMQ0wCwYDVQQIDAR0ZXN0MQ0wCwYD
VQQHDAR0ZXN0MQ0wCwYDVQQKDAR0ZXN0MQ0wCwYDVQQLDAR0ZXN0MQ0wCwYDVQQD
DAR0ZXN0MRwwGgYJKoZIhvcNAQkBFg10ZXN0QHRlc3QuY29tMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyKWSwJDdGKNrvi6IA9h3X7DLof5iKM8tOU5J
9ykkYCr+uEvtW90SJBSPbgJ0pI1AfVBIqteTDsuUpg1pmzLGh0iQZxziVxkNzBEQ
Guz1/6bfJVkXHdUHPR06lLuH8NHEVTEm9/6BJpSmsOxlVA1gZXKdTRuUqKerjcZ9
HMyCF/Y2UNZhsx/BmTuxZDyWkwTnjNg8/v+pIpb2ea3pPhO02tQ3GKEwH9FoyBIu
5z89e0fGxo1Fj0XtziWqn2jXbwSKpnwrbXofAF4mhL10hkoeMJ5ckW1z067PTTOv
OGhGDknSMqYsuUil5UZZtuTkbbFFKi8D1ofA/7mfzifEnpM1kwIDAQABo1MwUTAd
BgNVHQ4EFgQUJGLoWiLT31mqL20z9VsY8JnAlOswHwYDVR0jBBgwFoAUJGLoWiLT
31mqL20z9VsY8JnAlOswDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC
AQEAKoky4qxpt9kmnKHyNF6ZrBm7Cpw4ZhFyVUP0LpFenUv6ecGwW5mxR2ComMxP
xCjXA0Yq2mTETCwrqi81ZFNhXfvJEPamUrQ7bATvPZzDPviaKrxPdkek/SqBtU3s
VZppxe7Sc+MC4ds65HwevwopE+mPoe5fMGVs81G4tI9oCF1XPuAe4fiOl33YEcW2
4qAyCEaZ6SIi4WsAsOaTYhijE/5ud3vHvx4ZWC+nwRbCumm6MWdtLPLWZa6CbqZB
jHQNAe+ih8Br8Nrh1uyZOB2q2t0RbcfClGIunjR8cm9id9IuAvEaNVNzR3jyKRQH
wub2PjUzKSWOKr770AmAUb19Pg==
-----END CERTIFICATE-----
`

const testKey = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDIpZLAkN0Yo2u+
LogD2HdfsMuh/mIozy05Tkn3KSRgKv64S+1b3RIkFI9uAnSkjUB9UEiq15MOy5Sm
DWmbMsaHSJBnHOJXGQ3MERAa7PX/pt8lWRcd1Qc9HTqUu4fw0cRVMSb3/oEmlKaw
7GVUDWBlcp1NG5Sop6uNxn0czIIX9jZQ1mGzH8GZO7FkPJaTBOeM2Dz+/6kilvZ5
rek+E7Ta1DcYoTAf0WjIEi7nPz17R8bGjUWPRe3OJaqfaNdvBIqmfCtteh8AXiaE
vXSGSh4wnlyRbXPTrs9NM684aEYOSdIypiy5SKXlRlm25ORtsUUqLwPWh8D/uZ/O
J8SekzWTAgMBAAECggEABgR9NbxCfUItcYs4thDQZ7TILqgP7pRkEVNpQXng5udz
MzjHuhkTubRKJuz47ZR06i01uLX1aZyubRp639YygQ0qk4UYvq74LHYYiw4vRIcP
KzIUUOc6K9mMD7jeF1lbL4jlV8uwuOT9aNH2KgKqsPAyioT4vOQmb36T8wCpKCnD
DKwXA0AV40iBnDAoZlixBkwf8De5wcYC63zOGJ+i9th0ujovW8Ds+UuUb+fbpXrz
gq8Uk3zuvEhrczRwaK+bDaR6PW3g4jy96FcMVjtSJdigRG+UEGnZC3pvt8cGwvoH
iMFF8xQfF31OPPJNEt4Z5nVebuZbE9pc0vPAQdFXGQKBgQD1xC+P8abHcC2MXOU1
QI1ppxBHlBE58SZcpGxG6TG2rdiMvM0oq7VUuCnQ9GimoMuDAdD9YlfbRAb2VENH
KTrCiu8kk7/6dkbmMpwMCw/S1VkGHFaysYdvWY9PC+284DqkubmZsigaXdWYVsVr
S+lJ8tDXwrQzoHsa27T/NZtJ6QKBgQDRAGw/1cc8zHauepMm5+m8KC/+wDi+UXIS
1RjKdPVStUvz/UfOrIjesoOGlmPFHLQC9ADSrxxH6aK+gG6d/1KjzTQVYpndq4xC
VDhfpzsGwoVW1w32eHQpI/dRO44yshdyZqZHkbNPmeI8XFc3VdtrFylvtKS7QcBJ
g3fjfPLaGwKBgQCfDd7yO6SCQllYE+7LLgHXNKXWjT8wzp7TKh5hLh5cadpSCwaD
oczzDVUSxHrODBZprM1Cj1josPgIh7Qa49YBfcUTWQPP5qgv5uUS7j3JZwX8bG63
qylJqR6UO9YafMu3O/OgQqqtlbjcpJuTu0c58omyeXICT4Qcd8CFwn3DsQKBgGrg
1nqGbg6PWJm9IQciTYrk2jZiQiJBMB6lTropuVKEV8T73v63iH6pt0zaF0czeHKS
KOGUnte/iHP25ZpyeOY/B8Vv2NNc6Kr6uqFfuXWpf9p6uy8xReXL+KtX003leMwN
5jZvMc0hGmpXplor07sd6xiuvhbsdtKhImv494/FAoGASZYOdE7amobY5Kpm6lbE
g4jbgkZQvDOhCebwcWVcxL/F8tSZcHWdhmaX4Q9A5+8EF9W/0nZPImB54rwQJ6DN
e2MC+FKfM4FUxfPXzTz8DMkhrbyD8v7MfVnpLBZSOfuB9xvB7ZQu2qALmKm4OPiN
GcILtt7C97hQjjUJsURDFhA=
-----END PRIVATE KEY-----`
