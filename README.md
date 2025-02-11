# nntpcli

A NNTP client with the fastest yenc support.

## Installation

To install the `nntpcli` package, you can use `go get`:

```sh
go get github.com/javi11/nntpcli
```

Since this package uses [Rapidyenc](github.com/mnightingale/rapidyenc), you will need to build it with **CGO enabled**

## Usage

Here is a simple example of how to use the `nntpcli` package:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "strings"
    "time"

    "github.com/javi11/nntpcli"
)

func main() {
    client := nntpcli.New()
    conn, err := client.Dial(context.Background(), "news.example.com", 119, time.Now().Add(5*time.Second))
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    err = conn.Authenticate("username", "password")
    if err != nil {
        log.Fatal(err)
    }

    err = conn.JoinGroup("misc.test")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Joined group:", conn.CurrentJoinedGroup())

    // Example of Body command
    body, err := os.Create("article_body.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer body.Close()

    _, err = conn.BodyDecoded("<message-id>", body, 0)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Article body saved to article_body.txt")

    // Example of Post command
    postContent := `From: <nobody@example.com>
Newsgroups: misc.test
Subject: Test Post
Message-Id: <1234>
Organization: nntpcli

This is a test post.
`
    err = conn.Post(strings.NewReader(postContent))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Article posted successfully")
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
