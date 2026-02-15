package nntppool

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// PostHeaders holds the structured headers for an NNTP POST command.
type PostHeaders struct {
	From       string   // required: "user@example.com" or "User Name <user@example.com>"
	Subject    string   // required
	Newsgroups []string // required: list of newsgroups
	MessageID  string   // recommended: "<unique@domain>"
	// Extra holds additional headers (e.g., Organization, X-No-Archive).
	Extra map[string][]string
}

// WriteTo formats the headers as RFC 5322 and writes them to w,
// including the blank line separator after headers.
func (h PostHeaders) WriteTo(w io.Writer) (int64, error) {
	var total int64
	write := func(s string) error {
		n, err := io.WriteString(w, s)
		total += int64(n)
		return err
	}

	if err := write(fmt.Sprintf("From: %s\r\n", h.From)); err != nil {
		return total, err
	}
	if err := write(fmt.Sprintf("Newsgroups: %s\r\n", strings.Join(h.Newsgroups, ","))); err != nil {
		return total, err
	}
	if err := write(fmt.Sprintf("Subject: %s\r\n", h.Subject)); err != nil {
		return total, err
	}
	if h.MessageID != "" {
		if err := write(fmt.Sprintf("Message-ID: %s\r\n", h.MessageID)); err != nil {
			return total, err
		}
	}

	// Write extra headers in sorted order for deterministic output.
	if len(h.Extra) > 0 {
		keys := make([]string, 0, len(h.Extra))
		for k := range h.Extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			for _, v := range h.Extra[k] {
				if err := write(fmt.Sprintf("%s: %s\r\n", k, v)); err != nil {
					return total, err
				}
			}
		}
	}

	// Blank line separating headers from body.
	if err := write("\r\n"); err != nil {
		return total, err
	}

	return total, nil
}

// PostResult holds the result of a POST command.
type PostResult struct {
	StatusCode int
	Status     string
}
