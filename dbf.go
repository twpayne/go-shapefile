package shapefile

// FIXME support dBase version 7 files if needed, see https://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
// FIXME work through https://www.clicketyclick.dk/databases/xbase/format/dbf.html and add any missing features
// FIXME add unmarshaller that unmarshalls a record into a Go struct with `dbf:"..."` tags?s
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

	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

const (
	dbfHeaderLength        = 32
	dbfFieldDescriptorSize = 32
)

var (
	knownFieldTypes = map[byte]struct{}{
		'C': {}, // Character
		'D': {}, // Date
		'F': {}, // Floating point binary numeric
		'L': {}, // Binary coded decimal numeric
		'M': {}, // Memo
		'N': {}, // Numeric
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

// ReadDBFOptions are options to ReadDBF.
type ReadDBFOptions struct {
	MaxHeaderSize    int
	MaxRecordSize    int
	MaxRecords       int
	SkipBrokenFields bool
	Charset          string
}

// A DBFMemo is a DBF memo.
type DBFMemo string

// ReadDBF reads a DBF from an io.Reader.
func ReadDBF(r io.Reader, _ int64, options *ReadDBFOptions) (*DBF, error) {
	headerData := make([]byte, dbfHeaderLength)
	if err := readFull(r, headerData); err != nil {
		return nil, err
	}
	header, err := ParseDBFHeader(headerData, options)
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

	totalLength := 0
	for _, fieldDescriptor := range fieldDescriptors {
		totalLength += fieldDescriptor.Length
	}
	if totalLength+1 != header.RecordSize {
		return nil, errors.New("invalid total length of fields")
	}

	var decoder *encoding.Decoder
	if options != nil && options.Charset != "" {
		enc, _ := charset.Lookup(options.Charset)
		if enc == nil {
			return nil, fmt.Errorf("unknown charset '%s'", options.Charset)
		}
		decoder = enc.NewDecoder()
	} else {
		decoder = charmap.ISO8859_1.NewDecoder()
	}
	records := make([][]any, 0, header.Records)
	for range header.Records {
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
				field, err := fieldDescriptor.ParseRecord(fieldData, decoder)
				if err != nil && !options.SkipBrokenFields {
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
	switch err := readFull(r, data); {
	case errors.Is(err, io.EOF):
		// Ignore missing end of file marker.
	case err != nil:
		return nil, err
	case data[0] != '\x1a':
		return nil, fmt.Errorf("%d: invalid end of file marker", data[0])
	}

	return &DBF{
		DBFHeader:        *header,
		FieldDescriptors: fieldDescriptors,
		Records:          records,
	}, nil
}

// ParseDBFHeader parses a DBFHeader from data.
func ParseDBFHeader(data []byte, options *ReadDBFOptions) (*DBFHeader, error) {
	if len(data) != dbfHeaderLength {
		return nil, errors.New("invalid header length")
	}

	version := int(data[0]) & 0x7
	if version != 3 {
		return nil, fmt.Errorf("%d: unsupported version", version)
	}
	memo := int(data[0])&0x8 == 0x8
	if memo {
		return nil, errors.New("memo files not supported")
	}
	dbt := int(data[0])&0x80 == 0x80
	if dbt {
		return nil, errors.New(".DBT files are not supported")
	}

	lastUpdateYear := int(data[1]) + 1900
	lastUpdateMonth := time.Month(int(data[2]))
	lastUpdateDay := int(data[3])
	lastUpdate := time.Date(lastUpdateYear, lastUpdateMonth, lastUpdateDay, 0, 0, 0, 0, time.UTC)

	records := int(binary.LittleEndian.Uint32(data[4:8]))
	if options != nil && options.MaxRecords != 0 && records > options.MaxRecords {
		return nil, errors.New("too many records")
	}

	headerSize := int(binary.LittleEndian.Uint16(data[8:10]))
	if options != nil && options.MaxHeaderSize != 0 && headerSize > options.MaxHeaderSize {
		return nil, errors.New("header too large")
	}

	recordSize := int(binary.LittleEndian.Uint16(data[10:12]))
	if options != nil && options.MaxRecordSize != 0 && recordSize > options.MaxRecordSize {
		return nil, errors.New("records too large")
	}

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

// ReadDBFZipFile reads a DBF from a *zip.File.
func ReadDBFZipFile(zipFile *zip.File, options *ReadDBFOptions) (*DBF, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	dbf, err := ReadDBF(readCloser, int64(zipFile.UncompressedSize64), options)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", zipFile.Name, err)
	}
	return dbf, nil
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
func (d *DBFFieldDescriptor) ParseRecord(data []byte, decoder *encoding.Decoder) (any, error) {
	switch d.Type {
	case 'C':
		return parseCharacter(data, decoder)
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

func parseCharacter(data []byte, decoder *encoding.Decoder) (string, error) {
	if decoder == nil {
		return "", errors.New("decoder is nil")
	}
	return decoder.String(string(bytes.TrimSpace(TrimTrailingZeros(data))))
}

func parseDate(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, errors.New("invalid date field length")
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
