module github.com/javi11/nntppool/v2/cmd/nzb-downloader

go 1.25.1

require (
	github.com/javi11/nntppool/v2 v2.0.0
	github.com/javi11/nzbparser v0.5.4
	github.com/schollz/progressbar/v3 v3.19.0
	github.com/spf13/pflag v1.0.10
)

replace github.com/javi11/nntppool/v2 => ../..

require (
	github.com/avast/retry-go/v4 v4.6.1 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mnightingale/rapidyenc v0.0.0-20250628164132-aaf36ba945ef // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	go.uber.org/mock v0.5.2 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/term v0.38.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
