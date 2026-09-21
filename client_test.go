package nntppool

import (
	"context"
	"errors"
	"testing"

	"github.com/mnightingale/rapidyenc"
)

func TestMapFormat(t *testing.T) {
	tests := []struct {
		in   rapidyenc.Format
		want ArticleEncoding
	}{
		{rapidyenc.FormatYenc, EncodingYEnc},
		{rapidyenc.FormatUU, EncodingUU},
		{rapidyenc.FormatUnknown, EncodingUnknown},
		{rapidyenc.Format(99), EncodingUnknown}, // unknown value
	}
	for _, tt := range tests {
		if got := mapFormat(tt.in); got != tt.want {
			t.Errorf("mapFormat(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestParseHeaders(t *testing.T) {
	t.Run("simple headers", func(t *testing.T) {
		lines := []string{
			"Subject: test article",
			"From: user@example.com",
		}
		h := parseHeaders(lines)
		if got := h["Subject"]; len(got) != 1 || got[0] != "test article" {
			t.Errorf("Subject = %v", got)
		}
		if got := h["From"]; len(got) != 1 || got[0] != "user@example.com" {
			t.Errorf("From = %v", got)
		}
	})

	t.Run("folded continuation", func(t *testing.T) {
		lines := []string{
			"Subject: very long",
			" subject line",
			"From: user@example.com",
		}
		h := parseHeaders(lines)
		if got := h["Subject"]; len(got) != 1 || got[0] != "very long subject line" {
			t.Errorf("Subject = %v, want [very long subject line]", got)
		}
	})

	t.Run("tab continuation", func(t *testing.T) {
		lines := []string{
			"Subject: start",
			"\tcontinued",
		}
		h := parseHeaders(lines)
		if got := h["Subject"]; len(got) != 1 || got[0] != "start continued" {
			t.Errorf("Subject = %v", got)
		}
	})

	t.Run("multiple values", func(t *testing.T) {
		lines := []string{
			"Received: from a",
			"Received: from b",
		}
		h := parseHeaders(lines)
		if got := h["Received"]; len(got) != 2 || got[0] != "from a" || got[1] != "from b" {
			t.Errorf("Received = %v", got)
		}
	})

	t.Run("empty lines skipped", func(t *testing.T) {
		lines := []string{
			"Subject: test",
			"",
			"From: user@example.com",
		}
		h := parseHeaders(lines)
		if len(h) != 2 {
			t.Errorf("expected 2 headers, got %d", len(h))
		}
	})

	t.Run("no-colon lines skipped", func(t *testing.T) {
		lines := []string{
			"Subject: test",
			"not a header",
			"From: user@example.com",
		}
		h := parseHeaders(lines)
		if _, ok := h["not a header"]; ok {
			t.Error("should not parse line without colon as header")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		h := parseHeaders(nil)
		if len(h) != 0 {
			t.Errorf("expected empty map, got %v", h)
		}
	})

	t.Run("orphan continuation", func(t *testing.T) {
		lines := []string{
			" orphan continuation",
			"Subject: test",
		}
		h := parseHeaders(lines)
		// orphan continuation should be silently ignored (currentKey is "")
		if got := h["Subject"]; len(got) != 1 || got[0] != "test" {
			t.Errorf("Subject = %v", got)
		}
	})

	t.Run("whitespace trimming", func(t *testing.T) {
		lines := []string{
			"Subject:   padded value   ",
		}
		h := parseHeaders(lines)
		if got := h["Subject"]; len(got) != 1 || got[0] != "padded value" {
			t.Errorf("Subject = %q, want \"padded value\"", got)
		}
	})
}

// In v4 a nil writer was an error on the streaming entry points. In v5 it is
// the buffered mode, so it must reach the pool and fail for a transport
// reason (nothing is listening on localhost:119) rather than be rejected.
func TestFetchNilWriterMeansBuffered(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Host: "localhost:119", Connections: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	_, err = c.Fetch(context.Background(), Req{MessageID: "test@example.com"})
	if errors.Is(err, ErrNoMessageID) {
		t.Fatalf("a nil Writer must select buffered mode, not be rejected: %v", err)
	}
}
