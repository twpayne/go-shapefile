// Package shapefile reads ESRI Shapefiles.
//
// See https://support.esri.com/en/white-paper/279.
package shapefile

// FIXME provide lazy, random access to individual records, using SHX

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"strings"

	"github.com/ettle/strcase"
	"github.com/twpayne/go-geom"
)

// bufioReadCloser ...
type bufioReadCloser = struct {
	*bufio.Reader
	io.Closer
}

type ScanShapefile struct {
	SHxHeader        *SHxHeader
	DBFHeader        *DBFHeader
	FieldDescriptors []*DBFFieldDescriptor
	Projection       *string
	NumRecords       int
	Records          []*ScanRecord

	fieldDescOrder map[int]string
}

func (s ScanShapefile) Record(i int) (map[string]any, geom.T) {
	if s.Records[i] == nil {
		return nil, nil
	}
	return s.Records[i].Properties(s.fieldDescOrder), s.Records[i].Geom()
}

// ScanExporter ...
type ScanExporter struct {
	FieldStruct map[int]string
	Type        reflect.Type
}

// NewExporter ...
func NewExporter(t reflect.Type, tag string, fieldDescriptors []*DBFFieldDescriptor) (*ScanExporter, error) {
	if t == nil || t.Kind() != reflect.Struct {
		return nil, errors.New("type t is nil or is not a struct")
	}
	structTags := make(map[string]string, t.NumField())
	for j := 0; j < t.NumField(); j++ {
		fieldType := t.Field(j)
		tagName := strings.Split(fieldType.Tag.Get(tag), ",")[0]
		structTags[tagName] = fieldType.Name
	}
	fieldStruct := make(map[int]string, len(fieldDescriptors))
	fieldStruct[-1] = structTags["geometry"]
	for i, fieldDescriptor := range fieldDescriptors {
		if name, ok := structTags[strcase.ToSnake(fieldDescriptor.Name)]; ok {
			fieldStruct[i] = name
		}
	}
	return &ScanExporter{
		FieldStruct: fieldStruct,
		Type:        t,
	}, nil
}

// ScanRecord ...
type ScanRecord struct {
	SPH *SHPRecord
	SHX *SHXRecord
	DBF *DBFRecord
}

func (s ScanRecord) Properties(order map[int]string) map[string]any {
	if s.DBF == nil {
		return nil
	}
	pMap := make(map[string]any)
	props := *s.DBF
	for i := 0; i < len(props); i++ {
		pMap[order[i]] = props[i]
	}
	return pMap
}

func (s ScanRecord) Geom() geom.T {
	if s.SPH == nil {
		return nil
	}
	return s.SPH.Geom
}

func (s ScanRecord) Export(exporter *ScanExporter) any {
	if exporter == nil {
		return nil
	}
	values := reflect.New(exporter.Type)
	if s.DBF != nil {
		props := *s.DBF
		for i := 0; i < len(props); i++ {
			val := values.Elem().FieldByName(exporter.FieldStruct[i])
			target := reflect.ValueOf(props[i])
			if val.IsValid() && target.CanConvert(val.Type()) {
				val.Set(target.Convert(val.Type()))
			}
		}
	}
	if s.SPH != nil {
		val := values.Elem().FieldByName(exporter.FieldStruct[-1])
		target := reflect.ValueOf(s.SPH.Geom)
		if val.IsValid() && target.CanConvert(val.Type()) {
			val.Set(target.Convert(val.Type()))
		}
	}

	return values.Elem().Interface()
}

// Scanner ...
type Scanner struct {
	SHP         *ScannerSHP
	DBF         *ScannerDBF
	SHX         *ScannerSHX
	PRJ         *PRJ
	scanRecords int
	err         error
}

func ReadScannerBasename(basename string, options *ReadShapefileOptions) (*ScanShapefile, error) {
	scanner, err := NewScannerFromBasename(basename, options)
	if err != nil {
		return nil, fmt.Errorf("NewScannerFromBasename: %w", err)
	}
	defer scanner.Close()
	sf, err := ReadScanner(scanner)
	if err != nil {
		return nil, fmt.Errorf("ReadScanner: %w", err)
	}
	return sf, err
}

func ReadScannerZipFile(name string, options *ReadShapefileOptions) (*ScanShapefile, error) {
	scanner, err := NewScannerFromZipFile(name, options)
	if err != nil {
		return nil, fmt.Errorf("NewScannerFromBasename: %w", err)
	}
	defer scanner.Close()
	sf, err := ReadScanner(scanner)
	if err != nil {
		return nil, fmt.Errorf("ReadScanner: %w", err)
	}
	return sf, err
}

func ReadScanner(scanner *Scanner) (*ScanShapefile, error) {
	var sf ScanShapefile
	for {
		if record, err := scanner.Scan(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("record %d: %w", scanner.scanRecords, err)
		} else {
			sf.Records = append(sf.Records, record)
		}
	}
	sf.NumRecords = scanner.scanRecords

	if scanner.SHX != nil {
		sf.SHxHeader = scanner.SHX.header
	} else if scanner.SHP != nil {
		sf.SHxHeader = scanner.SHP.header
	}
	if scanner.DBF != nil {
		sf.DBFHeader = scanner.DBF.header
		sf.FieldDescriptors = scanner.DBF.fieldDescriptors
		sf.fieldDescOrder = make(map[int]string, len(sf.FieldDescriptors))
		for i, field := range sf.FieldDescriptors {
			sf.fieldDescOrder[i] = field.Name
		}
	}
	if scanner.PRJ != nil {
		sf.Projection = &scanner.PRJ.Projection
	}
	return &sf, nil
}

func NewScannerFromBasename(basename string, options *ReadShapefileOptions) (*Scanner, error) {
	if options == nil {
		options = &ReadShapefileOptions{}
	}

	readers := make(map[string]io.ReadCloser)
	sizes := make(map[string]int64)

	dbfFile, dbfSize, err := openWithSize(basename + ".dbf")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.dbf: %w", basename, err)
	default:
		readers["dbf"] = dbfFile
		sizes["dbf"] = dbfSize
	}

	prjFile, prjSize, err := openWithSize(basename + ".prj")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.prj: %w", basename, err)
	default:
		readers["prj"] = prjFile
		sizes["prj"] = prjSize
	}

	shxFile, shxSize, err := openWithSize(basename + ".shx")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.shx: %w", basename, err)
	default:
		readers["shx"] = shxFile
		sizes["shx"] = shxSize
	}

	shpFile, shpSize, err := openWithSize(basename + ".shp")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.shp: %w", basename, err)
	default:
		readers["shp"] = shpFile
		sizes["shp"] = shpSize
	}

	scanner, err := NewScanner(readers, sizes, options)
	if err != nil {
		return nil, fmt.Errorf("NewScanner: %w", err)
	}
	return scanner, nil
}

// ReadZipFile reads a Shapefile from a .zip file.
func NewScannerFromZipFile(name string, options *ReadShapefileOptions) (*Scanner, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	// defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	zipReader, err := zip.NewReader(file, fileInfo.Size())
	if err != nil {
		return nil, err
	}

	scanner, err := NewScannerFromZipReader(zipReader, options)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return scanner, nil
}

// ReadZipReader reads a Shapefile from a *zip.Reader.
func NewScannerFromZipReader(zipReader *zip.Reader, options *ReadShapefileOptions) (*Scanner, error) {
	var dbfFiles []*zip.File
	var prjFiles []*zip.File
	var shxFiles []*zip.File
	var shpFiles []*zip.File
	for _, zipFile := range zipReader.File {
		switch strings.ToLower(path.Ext(zipFile.Name)) {
		case ".dbf":
			dbfFiles = append(dbfFiles, zipFile)
		case ".prj":
			prjFiles = append(prjFiles, zipFile)
		case ".shp":
			shpFiles = append(shpFiles, zipFile)
		case ".shx":
			shxFiles = append(shxFiles, zipFile)
		}
	}

	readers := make(map[string]io.ReadCloser)
	sizes := make(map[string]int64)

	switch len(dbfFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := dbfFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers["dbf"] = readCloser
		sizes["dbf"] = int64(dbfFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .dbf files")
	}

	switch len(prjFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := dbfFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers["prj"] = readCloser
		sizes["prj"] = int64(prjFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .prj files")
	}

	switch len(shpFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := shpFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers["shp"] = readCloser
		sizes["shp"] = int64(shpFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .shp files")
	}

	switch len(shxFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := shxFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers["shx"] = readCloser
		sizes["shx"] = int64(shxFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .shx files")
	}

	scanner, err := NewScanner(readers, sizes, options)
	if err != nil {
		return nil, fmt.Errorf("NewScanner: %w", err)
	}
	return scanner, nil
}

// NewScanner ...
func NewScanner(readers map[string]io.ReadCloser, sizes map[string]int64, options *ReadShapefileOptions) (*Scanner, error) {
	if options == nil {
		options = &ReadShapefileOptions{}
	}
	var scannerSF Scanner
	if reader, ok := readers["shp"]; ok {
		if scanner, err := NewScannerSHP(reader, sizes["shp"], options.SHP); err != nil {
			return nil, fmt.Errorf("NewScannerSHP: %w", err)
		} else {
			scannerSF.SHP = scanner
		}
	}
	if reader, ok := readers["dbf"]; ok {
		if scanner, err := NewScannerDBF(reader, options.DBF); err != nil {
			return nil, fmt.Errorf("NewScannerDBF: %w", err)
		} else {
			scannerSF.DBF = scanner
		}
	}
	if reader, ok := readers["shx"]; ok {
		if scanner, err := NewScannerSHX(reader, sizes["shx"]); err != nil {
			return nil, fmt.Errorf("NewScannerSHX: %w", err)
		} else {
			scannerSF.SHX = scanner
		}
	}
	if reader, ok := readers["prj"]; ok {
		if scanner, err := ReadPRJ(reader, sizes["prj"]); err != nil {
			return nil, fmt.Errorf("NewScannerSHP: %w", err)
		} else {
			scannerSF.PRJ = scanner
		}
	}
	return &scannerSF, nil
}

// Scan
func (s *Scanner) Scan() (*ScanRecord, error) {
	if s.err != nil {
		return nil, s.err
	}

	var scanRecord ScanRecord
	if s.SHP != nil {
		if record, err := s.SHP.Scan(); err != nil {
			s.err = errors.Join(s.err, fmt.Errorf("Scan SHP: %w", err))
			return nil, s.err
		} else {
			scanRecord.SPH = record
		}
	}
	if s.DBF != nil {
		if record, err := s.DBF.Scan(); err != nil {
			s.err = errors.Join(s.err, fmt.Errorf("Scan DBF: %w", err))
			return nil, s.err
		} else {
			scanRecord.DBF = &record
		}
	}
	if s.SHX != nil {
		if record, err := s.SHX.Scan(); err != nil {
			s.err = errors.Join(s.err, fmt.Errorf("Scan SHX: %w", err))
			return nil, s.err
		} else {
			scanRecord.SHX = record
		}
	}
	s.scanRecords++
	return &scanRecord, nil
}

// Discard ...
func (s *Scanner) Discard(n int) (int, error) {
	var nByte int
	if s.DBF != nil {
		nb, err := s.DBF.reader.Discard(n * s.DBF.header.RecordSize)
		if err != nil {
			return nb, err
		}
		nByte = nb
	}
	if s.SHX != nil {
		nb, err := s.SHX.reader.Discard(n * 8)
		if err != nil {
			return nb, err
		}
		nByte = n
		if s.SHP != nil {
			data, err := s.SHX.reader.Peek(8)
			if err != nil {
				return 0, err
			}
			record := ParseSHXRecord(data)
			nb, err := s.DBF.reader.Discard(record.Offset)
			if err != nil {
				return nb, err
			}
			nByte = nb
		}
	}
	return nByte, nil
}

func (s *Scanner) Close() error {
	var err error
	if s.DBF != nil {
		err = errors.Join(err, s.DBF.reader.Close())
	}
	if s.SHP != nil {
		err = errors.Join(err, s.SHP.reader.Close())
	}
	if s.SHX != nil {
		err = errors.Join(err, s.SHX.reader.Close())
	}
	return err
}

// SHP
type ScannerSHP struct {
	reader      bufioReadCloser
	options     *ReadSHPOptions
	header      *SHxHeader
	scanRecords int
	err         error
}

func NewScannerSHP(reader io.ReadCloser, size int64, options *ReadSHPOptions) (*ScannerSHP, error) {
	header, err := readSHxHeader(reader, size)
	if err != nil {
		return nil, err
	}
	return &ScannerSHP{
		reader:  bufioReadCloser{bufio.NewReader(reader), reader},
		header:  header,
		options: options,
	}, nil
}

// Scan
func (s *ScannerSHP) Scan() (*SHPRecord, error) {
	if s.err != nil {
		return nil, s.err
	}

	record, err := ReadSHPRecord(s.reader, s.options)
	switch {
	case errors.Is(err, io.EOF):
		s.err = io.EOF
		return nil, s.err
	case err != nil:
		s.err = fmt.Errorf("record %d: %w", s.scanRecords, err)
		return nil, s.err
	case record.Number != s.scanRecords+1:
		s.err = fmt.Errorf("record %d: invalid record number (expected %d)", s.scanRecords, record.Number)
		return nil, s.err
	default:
		s.scanRecords++
		return record, nil
	}
}

// SHX
type ScannerSHX struct {
	reader      bufioReadCloser
	header      *SHxHeader
	scanRecords int
	err         error
}

func NewScannerSHX(reader io.ReadCloser, size int64) (*ScannerSHX, error) {
	header, err := readSHxHeader(reader, size)
	if err != nil {
		return nil, err
	}
	return &ScannerSHX{
		reader: bufioReadCloser{bufio.NewReader(reader), reader},
		header: header,
	}, nil
}

func (s *ScannerSHX) Scan() (*SHXRecord, error) {
	if s.err != nil {
		return nil, s.err
	}

	data := make([]byte, 8)
	if err := readFull(s.reader, data); err != nil {
		return nil, err
	}
	record := ParseSHXRecord(data)
	s.scanRecords++
	return &record, nil
}

// DBF
type DBFRecord = []any

type ScannerDBF struct {
	reader           bufioReadCloser
	options          *ReadDBFOptions
	header           *DBFHeader
	fieldDescriptors []*DBFFieldDescriptor
	scanRecords      int
	err              error
}

func NewScannerDBF(reader io.ReadCloser, options *ReadDBFOptions) (*ScannerDBF, error) {

	headerData := make([]byte, dbfHeaderLength)
	if err := readFull(reader, headerData); err != nil {
		return nil, err
	}
	header, err := ParseDBFHeader(headerData, options)
	if err != nil {
		return nil, err
	}

	var fieldDescriptors []*DBFFieldDescriptor
	for i := 0; ; i++ {
		fieldDescriptorData := make([]byte, dbfFieldDescriptorSize)
		if err := readFull(reader, fieldDescriptorData[:1]); err != nil {
			return nil, err
		}
		if fieldDescriptorData[0] == '\x0d' {
			break
		}
		if err := readFull(reader, fieldDescriptorData[1:]); err != nil {
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
		return nil, fmt.Errorf("invalid total length of fields")
	}

	return &ScannerDBF{
		reader:           bufioReadCloser{bufio.NewReader(reader), reader},
		options:          options,
		header:           header,
		fieldDescriptors: fieldDescriptors,
	}, nil
}

// Scan
func (s *ScannerDBF) Scan() (DBFRecord, error) {

	if s.err != nil {
		return nil, s.err
	}

	recordData := make([]byte, s.header.RecordSize)
	if err := readFull(s.reader, recordData); err != nil {
		s.err = err
		return nil, s.err
	}
	switch recordData[0] {
	case ' ':
		record := make([]any, 0, len(s.fieldDescriptors))
		offset := 1
		for _, fieldDescriptor := range s.fieldDescriptors {
			fieldData := recordData[offset : offset+fieldDescriptor.Length]
			offset += fieldDescriptor.Length
			field, err := fieldDescriptor.ParseRecord(fieldData)
			if err != nil {
				s.err = fmt.Errorf("field %s: %w", fieldDescriptor.Name, err)
				return nil, s.err
			}
			record = append(record, field)
		}
		s.scanRecords++
		return record, nil
	case '*':
		return nil, nil
	default:
		s.err = fmt.Errorf("%d: invalid record flag", recordData[0])
		return nil, s.err
	}
}
