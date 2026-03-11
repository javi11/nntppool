package nzb

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

type NZB struct {
	Files []File `xml:"file"`
}

type File struct {
	Poster   string    `xml:"poster,attr"`
	Date     string    `xml:"date,attr"`
	Subject  string    `xml:"subject,attr"`
	Groups   []string  `xml:"groups>group"`
	Segments []Segment `xml:"segments>segment"`
}

type Segment struct {
	Bytes     int    `xml:"bytes,attr"`
	Number    int    `xml:"number,attr"`
	MessageID string `xml:",chardata"`
}

func Parse(r io.Reader) (*NZB, error) {
	var n NZB
	if err := xml.NewDecoder(r).Decode(&n); err != nil {
		return nil, fmt.Errorf("nzb parse: %w", err)
	}
	return &n, nil
}

// FileName extracts the filename from the NZB subject line.
// It returns the first token enclosed in double quotes that precedes " yEnc",
// or an empty string if the subject does not match that pattern.
func (f *File) FileName() string {
	s := f.Subject
	start := strings.Index(s, `"`)
	if start == -1 {
		return ""
	}
	end := strings.Index(s[start+1:], `"`)
	if end == -1 {
		return ""
	}
	return s[start+1 : start+1+end]
}

// AllSegments returns a flat list of all segments across all files.
func (n *NZB) AllSegments() []Segment {
	var segs []Segment
	for _, f := range n.Files {
		segs = append(segs, f.Segments...)
	}
	return segs
}
