package shapefile

import (
	"archive/zip"
	"encoding/binary"
	"fmt"
	"io"
)

// An SHX is a .shx file.
type SHX struct {
	SHxHeader
	Records []SHXRecord
}

// An SHXRecord is a record in a SHX.
type SHXRecord struct {
	Offset        int
	ContentLength int
}

// ReadSHX reads a SHX from an io.Reader.
func ReadSHX(r io.Reader, size int64) (*SHX, error) {
	header, err := readSHxHeader(r, size)
	if err != nil {
		return nil, err
	}

	data := make([]byte, size-headerSize)
	if err := readFull(r, data); err != nil {
		return nil, err
	}

	n := int((size - headerSize) / 8)
	records := make([]SHXRecord, 0, n)
	for i := 0; i < n; i++ {
		record := ParseSHXRecord(data[8*i : 8*i+8])
		records = append(records, record)
	}

	return &SHX{
		SHxHeader: *header,
		Records:   records,
	}, nil
}

// ReadSHXZipFile reads a SHX from a *zip.File.
func ReadSHXZipFile(zipFile *zip.File) (*SHX, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	shx, err := ReadSHX(readCloser, int64(zipFile.UncompressedSize64))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", zipFile.Name, err)
	}
	return shx, nil
}

// ParseSHXRecord parses a SHXRecord from data.
func ParseSHXRecord(data []byte) SHXRecord {
	offset := 2 * int(binary.BigEndian.Uint32(data[:4]))
	contentLength := 2 * int(binary.BigEndian.Uint32(data[4:]))
	return SHXRecord{
		Offset:        offset,
		ContentLength: contentLength,
	}
}
