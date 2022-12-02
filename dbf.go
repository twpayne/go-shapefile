package shapefile

// FIXME support dBase version 7 files if needed, see https://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
// FIXME work through https://www.clicketyclick.dk/databases/xbase/format/dbf.html and add any missing features
// FIXME add unmarshaller that unmarshals a record into a Go struct with `dbf:"..."` tags?s
// FIXME validate logical implementation
// FIXME add support for memos

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/charmap"
)

const (
	dbfHeaderLength        = 32
	dbfFieldDescriptorSize = 32
)

var (
	knownFieldTypes = map[byte]struct{}{
		'C': {},
		'D': {},
		'F': {},
		'L': {},
		'M': {},
		'N': {},
	}

	knownLogicalValues = map[byte]any{
		'?': nil,
		'F': false,
		'N': false,
		'T': true,
		'Y': true,
		'f': false,
		'n': false,
		't': true,
		'y': true,
	}

	iso8859_1Decoder = charmap.ISO8859_1.NewDecoder()

	errDBTFilesNotSupported  = errors.New(".DBT files are not supported")
	errInvalidDateField      = errors.New("invalid date field")
	errMemoFilesNotSupported = errors.New("memo files are not supported")
)

// A DBFHeader is a DBF header.
type DBFHeader struct {
	Version    int
	Memo       bool
	DBT        bool
	LastUpdate time.Time
	Records    int
	HeaderSize int
	RecordSize int
}

// A DBFFieldDescriptor describes a DBF field.
type DBFFieldDescriptor struct {
	Name         string
	Type         byte
	Length       int
	DecimalCount int
	WorkAreaID   byte
	SetFields    byte
}

// A DBF is a dBase III PLUS table.
//
// See http://web.archive.org/web/20150323061445/http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm.
// See https://www.clicketyclick.dk/databases/xbase/format/dbf.html.
type DBF struct {
	DBFHeader
	FieldDescriptors []*DBFFieldDescriptor
	Records          [][]any
}

// A DBFMemo is a DBF memo.
type DBFMemo string

// ReadDBF reads a DBF file from r.
func ReadDBF(r io.Reader, size int64) (*DBF, error) {
	headerData := make([]byte, dbfHeaderLength)
	if err := readFull(r, headerData); err != nil {
		return nil, err
	}
	header, err := ParseDBFHeader(headerData)
	if err != nil {
		return nil, err
	}
	if header.Version != 3 {
		return nil, fmt.Errorf("%d: unsupported version", header.Version)
	}

	var fieldDescriptors []*DBFFieldDescriptor
	for i := 0; ; i++ {
		fieldDescriptorData := make([]byte, dbfFieldDescriptorSize)
		if err := readFull(r, fieldDescriptorData[:1]); err != nil {
			return nil, err
		}
		if fieldDescriptorData[0] == '\x0d' {
			break
		}
		if err := readFull(r, fieldDescriptorData[1:]); err != nil {
			return nil, err
		}

		name := string(TrimTrailingZeros(fieldDescriptorData[:11]))
		fieldType := fieldDescriptorData[11]
		if _, ok := knownFieldTypes[fieldType]; !ok {
			return nil, fmt.Errorf("field %d: %d: invalid field type", i, fieldType)
		}
		length := int(fieldDescriptorData[16])
		workAreaID := fieldDescriptorData[20]
		setFields := fieldDescriptorData[23]

		fieldDescriptor := &DBFFieldDescriptor{
			Name:       name,
			Type:       fieldType,
			Length:     length,
			WorkAreaID: workAreaID,
			SetFields:  setFields,
		}
		fieldDescriptors = append(fieldDescriptors, fieldDescriptor)
	}

	records := make([][]any, 0, header.Records)
	for i := 0; i < header.Records; i++ {
		recordData := make([]byte, header.RecordSize)
		if err := readFull(r, recordData); err != nil {
			return nil, err
		}
		switch recordData[0] {
		case ' ':
			record := make([]any, 0, len(fieldDescriptors))
			offset := 1
			for _, fieldDescriptor := range fieldDescriptors {
				fieldData := recordData[offset : offset+fieldDescriptor.Length]
				offset += fieldDescriptor.Length
				field, err := fieldDescriptor.ParseRecord(fieldData)
				if err != nil {
					return nil, fmt.Errorf("field %s: %w", fieldDescriptor.Name, err)
				}
				record = append(record, field)
			}
			records = append(records, record)
		case '*':
			records = append(records, nil)
		default:
			return nil, fmt.Errorf("%d: invalid record flag", recordData[0])
		}
	}

	data := make([]byte, 1)
	if err := readFull(r, data); err != nil {
		return nil, err
	}
	if data[0] != '\x1a' {
		return nil, fmt.Errorf("%d: invalid end of file marker", data[0])
	}

	return &DBF{
		DBFHeader:        *header,
		FieldDescriptors: fieldDescriptors,
		Records:          records,
	}, nil
}

// ParseDBFHeader parses a DBFHeader from data.
func ParseDBFHeader(data []byte) (*DBFHeader, error) {
	if len(data) != dbfHeaderLength {
		return nil, errInvalidHeader
	}

	version := int(data[0]) & 0x7
	if version != 3 {
		return nil, fmt.Errorf("%d: unsupported version", version)
	}
	memo := int(data[0])&0x8 == 0x8
	if memo {
		return nil, errMemoFilesNotSupported
	}
	dbt := int(data[0])&0x80 == 0x80
	if dbt {
		return nil, errDBTFilesNotSupported
	}

	lastUpdateYear := int(data[1]) + 1900
	lastUpdateMonth := time.Month(int(data[2]))
	lastUpdateDay := int(data[3])
	lastUpdate := time.Date(lastUpdateYear, lastUpdateMonth, lastUpdateDay, 0, 0, 0, 0, time.UTC)

	records := int(binary.LittleEndian.Uint32(data[4:8]))
	headerSize := int(binary.LittleEndian.Uint16(data[8:10]))
	recordSize := int(binary.LittleEndian.Uint16(data[10:12]))

	return &DBFHeader{
		Version:    version,
		Memo:       memo,
		DBT:        dbt,
		LastUpdate: lastUpdate,
		Records:    records,
		HeaderSize: headerSize,
		RecordSize: recordSize,
	}, nil
}

// ReadDBFZipFile reads a DBF file from a *zip.File.
func ReadDBFZipFile(zipFile *zip.File) (*DBF, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	return ReadDBF(readCloser, int64(zipFile.UncompressedSize64))
}

// Record returns the ith record.
func (d *DBF) Record(i int) map[string]any {
	if d.Records[i] == nil {
		return nil
	}
	fields := make(map[string]any, len(d.FieldDescriptors))
	record := d.Records[i]
	for j, fieldDescriptor := range d.FieldDescriptors {
		fields[fieldDescriptor.Name] = record[j]
	}
	return fields
}

// ParseRecord parses a record from data.
func (d *DBFFieldDescriptor) ParseRecord(data []byte) (any, error) {
	switch d.Type {
	case 'C':
		return parseCharacter(data)
	case 'D':
		return parseDate(data)
	case 'F':
		return parseFloat(data)
	case 'L':
		return parseLogical(data)
	case 'M':
		return parseMemo(data), nil
	case 'N':
		return parseNumber(data)
	default:
		return nil, fmt.Errorf("%d: unsupported field type", d.Type)
	}
}

// TrimTrailingZeros trims any trailing zero bytes from data.
func TrimTrailingZeros(data []byte) []byte {
	for i := len(data) - 1; i >= 0; i-- {
		if data[i] != '\x00' {
			return data[:i+1]
		}
	}
	return nil
}

func parseCharacter(data []byte) (string, error) {
	return iso8859_1Decoder.String(string(bytes.TrimSpace(TrimTrailingZeros(data))))
}

func parseDate(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, errInvalidDateField
	}
	year, err := strconv.ParseInt(string(data[:4]), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: invalid year: %w", string(data[:4]), err)
	}
	month, err := strconv.ParseInt(string(data[4:6]), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: invalid month: %w", string(data[4:6]), err)
	}
	day, err := strconv.ParseInt(string(data[6:8]), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: invalid day: %w", string(data[6:8]), err)
	}
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC), nil
}

func parseFloat(data []byte) (any, error) {
	fieldStr := string(bytes.TrimSpace(TrimTrailingZeros(data)))
	if fieldStr == "" {
		return nil, nil
	}
	field, err := strconv.ParseFloat(fieldStr, 64)
	if err != nil {
		return nil, fmt.Errorf("%q: invalid numeric: %w", fieldStr, err)
	}
	return field, nil
}

func parseLogical(data []byte) (any, error) {
	if len(data) != 1 {
		return nil, fmt.Errorf("%q: invalid logical", string(data))
	}
	field, ok := knownLogicalValues[data[0]]
	if !ok {
		return nil, fmt.Errorf("%q: invalid logical", string(data))
	}
	return field, nil
}

func parseMemo(data []byte) DBFMemo {
	return DBFMemo(bytes.TrimSpace(TrimTrailingZeros(data)))
}

func parseNumber(data []byte) (any, error) {
	fieldStr := string(bytes.TrimSpace(TrimTrailingZeros(data)))
	if fieldStr == "" {
		return nil, nil
	}
	if strings.Contains(fieldStr, ".") {
		field, err := strconv.ParseFloat(fieldStr, 64)
		if err != nil {
			return nil, fmt.Errorf("%q: invalid numeric: %w", fieldStr, err)
		}
		return field, nil
	}
	field, err := strconv.ParseInt(fieldStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%q: invalid numeric: %w", fieldStr, err)
	}
	return int(field), nil
}
