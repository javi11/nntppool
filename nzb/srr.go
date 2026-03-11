package nzb

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

const srrStoredFileType = 0x6A
const srrFlagAddSize = uint16(0x8000)

// SRRFile returns the first File in the NZB whose FileName ends with ".srr"
// (case-insensitive), or nil if none.
func (n *NZB) SRRFile() *File {
	for i := range n.Files {
		if strings.HasSuffix(strings.ToLower(n.Files[i].FileName()), ".srr") {
			return &n.Files[i]
		}
	}
	return nil
}

// ParseSRRFilenames parses an SRR binary and returns all original release
// filenames it encodes. It collects the filename of every stored file block
// (NFO, SFV, SRS, …) plus every filename listed in any embedded .sfv file.
// The combined, deduplicated list gives the full correct file list.
func ParseSRRFilenames(r io.Reader) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("srr read: %w", err)
	}
	if len(data) < 3 || data[0] != 0x69 || data[1] != 0x69 || data[2] != 0x69 {
		return nil, fmt.Errorf("srr: invalid magic bytes")
	}

	seen := make(map[string]bool)
	var results []string
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			results = append(results, name)
		}
	}

	pos := 0
	for pos < len(data) {
		if pos+7 > len(data) {
			break
		}
		headType := data[pos+2]
		headFlags := binary.LittleEndian.Uint16(data[pos+3:])
		headSize := int(binary.LittleEndian.Uint16(data[pos+5:]))
		if headSize < 7 {
			break
		}

		var addSize uint32
		hasAddSize := headFlags&srrFlagAddSize != 0
		if hasAddSize {
			if pos+11 > len(data) {
				break
			}
			addSize = binary.LittleEndian.Uint32(data[pos+7:])
		}

		headerEnd := pos + headSize
		if headerEnd > len(data) {
			break
		}

		if headType == srrStoredFileType {
			bodyStart := pos + 7
			if hasAddSize {
				bodyStart = pos + 11
			}
			if bodyStart+2 <= headerEnd {
				fnameLen := int(binary.LittleEndian.Uint16(data[bodyStart:]))
				if bodyStart+2+fnameLen <= headerEnd {
					fname := string(data[bodyStart+2 : bodyStart+2+fnameLen])
					add(fname)
					if strings.HasSuffix(strings.ToLower(fname), ".sfv") && addSize > 0 {
						sfvEnd := headerEnd + int(addSize)
						if sfvEnd <= len(data) {
							for _, n := range parseSFV(data[headerEnd:sfvEnd]) {
								add(n)
							}
						}
					}
				}
			}
		}

		pos = headerEnd + int(addSize)
	}

	return results, nil
}

// parseSFV parses SFV file content and returns the filenames.
// Each non-comment line has the form: filename checksum
func parseSFV(data []byte) []string {
	var names []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}
		if fields := strings.Fields(line); len(fields) > 0 {
			names = append(names, fields[0])
		}
	}
	return names
}
