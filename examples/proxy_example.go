package main

import (
	"context"
	"fmt"
	"log"

	"github.com/javi11/nntppool/v3"
)

func main() {
	// Example 1: Provider with SOCKS5 proxy (no authentication)
	provider1, err := nntppool.NewProvider(context.Background(), nntppool.ProviderConfig{
		Address:               "news.example.com:119",
		MaxConnections:        10,
		InitialConnections:    2,
		InflightPerConnection: 1,
		ProxyURL:              "socks5://proxy.example.com:1080",
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}
	defer func() {
		if err := provider1.Close(); err != nil {
			log.Printf("Failed to close provider1: %v", err)
		}
	}()

	// Example 2: Provider with SOCKS5 proxy with authentication
	provider2, err := nntppool.NewProvider(context.Background(), nntppool.ProviderConfig{
		Address:               "news.example.com:119",
		MaxConnections:        10,
		InitialConnections:    2,
		InflightPerConnection: 1,
		ProxyURL:              "socks5://username:password@proxy.example.com:1080",
		Auth: nntppool.Auth{
			Username: "newsuser",
			Password: "newspass",
		},
	})
	if err != nil {
		log.Fatalf("Failed to create provider with auth: %v", err)
	}
	defer func() {
		if err := provider2.Close(); err != nil {
			log.Printf("Failed to close provider2: %v", err)
		}
	}()

	// Example 3: Provider with SOCKS4 proxy
	provider3, err := nntppool.NewProvider(context.Background(), nntppool.ProviderConfig{
		Address:               "news.example.com:119",
		MaxConnections:        10,
		InitialConnections:    2,
		InflightPerConnection: 1,
		ProxyURL:              "socks4://proxy.example.com:1080",
	})
	if err != nil {
		log.Fatalf("Failed to create provider with SOCKS4: %v", err)
	}
	defer func() {
		if err := provider3.Close(); err != nil {
			log.Printf("Failed to close provider3: %v", err)
		}
	}()

	// Use the provider with a client
	client := nntppool.NewClient()
	defer client.Close()

	err = client.AddProvider(provider1, nntppool.ProviderPrimary)
	if err != nil {
		log.Fatalf("Failed to add provider: %v", err)
	}

	// Now all connections will go through the SOCKS proxy
	ctx := context.Background()
	resp, err := client.Head(ctx, "<article@example.com>")
	if err != nil {
		log.Fatalf("Failed to get article: %v", err)
	}

	fmt.Printf("Article status: %d %s\n", resp.StatusCode, resp.Status)
}
