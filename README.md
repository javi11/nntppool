# nntppool

A nntp pool connection with retry and provider rotation.

## Features

- Connection pooling
- Body download retry and yenc decode
- Post article with retry and yenc encode
- Stat article with retry
- TLS support
- Multiple providers with rotation. In case of failure for article not found the provider will be rotated.
- Backup providers. If all providers fail, the backup provider will be used for download. Useful for block accounts usage.

## Installation

To install the `nntppool` package, you can use `go get`:

```sh
go get github.com/javi11/nntppool
```

Since this package uses [Rapidyenc](github.com/mnightingale/rapidyenc), you will need to build it with **CGO enabled**

## Usage Example

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/javi11/nntppool"
)

func main() {
    // Configure the connection pool
    config := nntppool.Config{
        MinConnections: 5,
        MaxRetries:    3,
        Providers: []nntppool.Provider{
            {
                Host:                          "news.example.com",
                Port:                          119,
                Username:                      "user",
                Password:                      "pass",
                MaxConnections:                10,
                MaxConnectionIdleTimeInSeconds: 300,
                TLS:                           false,
            },
            {
                Host:                          "news-backup.example.com",
                Port:                          119,
                Username:                      "user",
                Password:                      "pass",
                MaxConnections:                5,
                MaxConnectionIdleTimeInSeconds: 300,
                TLS:                           true,
                IsBackupProvider:              true,
            },
        },
    }

    // Create a new connection pool
    pool, err := nntppool.NewConnectionPool(config)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Quit()

    // Example: Download an article
    ctx := context.Background()
    msgID := "<example-message-id@example.com>"
    file, err := os.Create("article.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    written, err := pool.Body(ctx, msgID, file, nil)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Downloaded %d bytes", written)

    // Example: Post an article
    article, err := os.Open("article.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer article.Close()

    err = pool.Post(ctx, article)
    if err != nil {
        log.Fatal(err)
    }

    // Example: Check if an article exists
    msgNum, err := pool.Stat(ctx, msgID, []string{"alt.binaries.test"})
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Article number: %d", msgNum)
}
```

## Development Setup

To set up the project for development, follow these steps:

1. Clone the repository:

```sh
git clone https://github.com/javi11/nntpcli.git
cd nntpcli
```

2. Install dependencies:

```sh
go mod download
```

3. Run tests:

```sh
make test
```

4. Lint the code:

```sh
make lint
```

5. Generate mocks and other code:

```sh
make generate
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request. See the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
