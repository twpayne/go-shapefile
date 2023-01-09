package shapefile

// FIXME document all exported types
// FIXME do more validation, especially against the length of the file

import (
	"archive/zip"
	"encoding/binary"
	"fmt"
	"io"
)

type SHX struct {
	SHxHeader
	Records []SHXRecord
}

type SHXRecord struct {
	Offset        int
	ContentLength int
}

func ReadSHX(r io.Reader, size int64) (*SHX, error) {
	header, err := ReadSHxHeader(r, size)
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

func ParseSHXRecord(data []byte) SHXRecord {
	offset := 2 * int(binary.BigEndian.Uint32(data[:4]))
	contentLength := 2 * int(binary.BigEndian.Uint32(data[4:]))
	return SHXRecord{
		Offset:        offset,
		ContentLength: contentLength,
	}
}
