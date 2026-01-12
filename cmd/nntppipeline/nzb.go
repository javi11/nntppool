package main

import (
	"encoding/xml"
	"fmt"
	"os"
)

// NZB represents the root element of an NZB file.
type NZB struct {
	XMLName xml.Name  `xml:"nzb"`
	Files   []NZBFile `xml:"file"`
}

// NZBFile represents a file entry in the NZB.
type NZBFile struct {
	Subject  string       `xml:"subject,attr"`
	Poster   string       `xml:"poster,attr"`
	Date     int64        `xml:"date,attr"`
	Groups   []string     `xml:"groups>group"`
	Segments []NZBSegment `xml:"segments>segment"`
}

// NZBSegment represents a segment (article) in the NZB file.
type NZBSegment struct {
	Bytes     int64  `xml:"bytes,attr"`
	Number    int    `xml:"number,attr"`
	MessageID string `xml:",chardata"`
}

// ParseNZB parses an NZB file and returns all message IDs with their sizes.
func ParseNZB(path string) ([]SegmentInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read nzb file: %w", err)
	}

	var nzb NZB
	if err := xml.Unmarshal(data, &nzb); err != nil {
		return nil, fmt.Errorf("parse nzb xml: %w", err)
	}

	var segments []SegmentInfo
	for _, file := range nzb.Files {
		for _, seg := range file.Segments {
			segments = append(segments, SegmentInfo{
				MessageID: seg.MessageID,
				Bytes:     seg.Bytes,
				Groups:    file.Groups,
			})
		}
	}

	return segments, nil
}

// SegmentInfo holds information about a segment to download.
type SegmentInfo struct {
	MessageID string
	Bytes     int64
	Groups    []string
}

// TotalBytes returns the total size of all segments.
func TotalBytes(segments []SegmentInfo) int64 {
	var total int64
	for _, seg := range segments {
		total += seg.Bytes
	}
	return total
}
