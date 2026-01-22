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
	"strings"
	"sync"

	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// bufioReadCloser ...
type bufioReadCloser = struct {
	*bufio.Reader
	io.Closer
}

// Scanner ...
type Scanner struct {
	scanSHP          *ScannerSHP
	scanDBF          *ScannerDBF
	scanSHX          *ScannerSHX
	filePRJ          *PRJ
	fileCPG          *CPG
	scanRecords      int64
	estimatedRecords int64
	err              error
}

// ReadScanner read a scanner and create a shapefile.
func ReadScanner(scanner *Scanner) (*Shapefile, error) {
	if scanner == nil {
		return nil, nil
	}
	var shp *SHP
	var shx *SHX
	var dbf *DBF
	var cpg *CPG
	var prj *PRJ

	if scanner.SHPHeader() != nil {
		shp = &SHP{SHxHeader: *scanner.SHPHeader()}
	}
	if scanner.SHxHeader() != nil {
		shx = &SHX{SHxHeader: *scanner.SHxHeader()}
	}

	if scanner.DBFHeader() != nil {
		dbf = &DBF{DBFHeader: *scanner.DBFHeader(), FieldDescriptors: scanner.DBFFieldDescriptors()}
	}

	if scanner.Projection() != "" {
		prj = &PRJ{Projection: scanner.Projection()}
	}

	if scanner.Charset() != "" {
		cpg = &CPG{Charset: scanner.Charset()}
	}

	for scanner.Next() {
		recSHP, recSHX, recDBF := scanner.Scan()
		if shp != nil && recSHP != nil {
			shp.Records = append(shp.Records, recSHP)
		}
		if dbf != nil && recDBF != nil {
			dbf.Records = append(dbf.Records, recDBF)
		}
		if shx != nil && recSHX != nil {
			shx.Records = append(shx.Records, *recSHX)
		}
	}

	if err := scanner.Error(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read scanner [%d]: %w", scanner.scanRecords, err)
	}

	return &Shapefile{
		SHP: shp,
		DBF: dbf,
		SHX: shx,
		PRJ: prj,
		CPG: cpg,
	}, nil
}

// NewScannerFromBasename reads files based of Basename and create a scanner.
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
		readers[".dbf"] = dbfFile
		sizes[".dbf"] = dbfSize
	}

	prjFile, prjSize, err := openWithSize(basename + ".prj")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.prj: %w", basename, err)
	default:
		readers[".prj"] = prjFile
		sizes[".prj"] = prjSize
	}

	cpgFile, cpgSize, err := openWithSize(basename + ".cpg")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.cpg: %w", basename, err)
	default:
		readers[".cpg"] = cpgFile
		sizes[".cpg"] = cpgSize
	}

	shxFile, shxSize, err := openWithSize(basename + ".shx")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.shx: %w", basename, err)
	default:
		readers[".shx"] = shxFile
		sizes[".shx"] = shxSize
	}

	shpFile, shpSize, err := openWithSize(basename + ".shp")
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, fmt.Errorf("%s.shp: %w", basename, err)
	default:
		readers[".shp"] = shpFile
		sizes[".shp"] = shpSize
	}

	scanner, err := NewScanner(readers, sizes, options)
	if err != nil {
		return nil, fmt.Errorf("NewScanner: %w", err)
	}
	return scanner, nil
}

// NewScannerFromZipFile reads a .zip file and create a scanner.
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

// NewScannerFromZipReader reads a *zip.Reader and create a scanner.
func NewScannerFromZipReader(zipReader *zip.Reader, options *ReadShapefileOptions) (*Scanner, error) {
	var dbfFiles []*zip.File
	var prjFiles []*zip.File
	var cpgFiles []*zip.File
	var shxFiles []*zip.File
	var shpFiles []*zip.File
	for _, zipFile := range zipReader.File {
		if isMacOSXPath(zipFile.Name) {
			continue
		}
		switch strings.ToLower(path.Ext(zipFile.Name)) {
		case ".dbf":
			dbfFiles = append(dbfFiles, zipFile)
		case ".prj":
			prjFiles = append(prjFiles, zipFile)
		case ".cpg":
			cpgFiles = append(cpgFiles, zipFile)
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
		readers[".dbf"] = readCloser
		sizes[".dbf"] = int64(dbfFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .dbf files")
	}

	switch len(prjFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := prjFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers[".prj"] = readCloser
		sizes[".prj"] = int64(prjFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .prj files")
	}

	switch len(cpgFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := cpgFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers[".cpg"] = readCloser
		sizes[".cpg"] = int64(cpgFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .cpg files")
	}

	switch len(shpFiles) {
	case 0:
		// Do nothing.
	case 1:
		readCloser, err := shpFiles[0].Open()
		if err != nil {
			return nil, err
		}
		readers[".shp"] = readCloser
		sizes[".shp"] = int64(shpFiles[0].UncompressedSize64)
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
		readers[".shx"] = readCloser
		sizes[".shx"] = int64(shxFiles[0].UncompressedSize64)
	default:
		return nil, errors.New("too many .shx files")
	}

	scanner, err := NewScanner(readers, sizes, options)
	if err != nil {
		return nil, fmt.Errorf("NewScanner: %w", err)
	}
	return scanner, nil
}

// NewScanner Create a new scanner.
func NewScanner(
	readers map[string]io.ReadCloser,
	sizes map[string]int64,
	options *ReadShapefileOptions,
) (*Scanner, error) {
	if options == nil {
		options = &ReadShapefileOptions{}
	}

	var cpg *CPG
	if reader, ok := readers[".cpg"]; ok {
		scanner, err := ReadCPG(reader, sizes[".cpg"])
		if err != nil {
			return nil, fmt.Errorf("ReadCPG: %w", err)
		}
		cpg = scanner
		switch {
		case options == nil:
			options = &ReadShapefileOptions{&ReadDBFOptions{Charset: scanner.Charset}, &ReadSHPOptions{}}
		case options.DBF == nil:
			options.DBF = &ReadDBFOptions{Charset: scanner.Charset}
		default:
			options.DBF.Charset = scanner.Charset
		}
	}

	var prj *PRJ
	if reader, ok := readers[".prj"]; ok {
		scanner, err := ReadPRJ(reader, sizes[".prj"])
		if err != nil {
			return nil, fmt.Errorf("ReadPRJ: %w", err)
		}
		prj = scanner
	}

	var wg sync.WaitGroup
	var scannerSHP *ScannerSHP
	var scannerSHX *ScannerSHX
	var scannerDBF *ScannerDBF
	var estimatedSHX, estimatedDBF int64
	var errSHP, errSHX, errDBF error

	wg.Add(3)
	go func() {
		defer wg.Done()
		if reader, ok := readers[".shp"]; ok {
			scanner, err := NewScannerSHP(reader, sizes[".shp"], options.SHP)
			if err != nil {
				errSHP = fmt.Errorf("NewScannerSHP: %w", err)
				return
			}
			scannerSHP = scanner
		}
	}()

	go func() {
		defer wg.Done()
		if reader, ok := readers[".dbf"]; ok {
			scanner, err := NewScannerDBF(reader, options.DBF)
			if err != nil {
				errDBF = fmt.Errorf("NewScannerDBF: %w", err)
				return
			}
			scannerDBF = scanner
			estimatedDBF = (sizes[".dbf"] - dbfHeaderLength) / int64(scanner.header.RecordSize)
		}
	}()

	go func() {
		defer wg.Done()
		if reader, ok := readers[".shx"]; ok {
			scanner, err := NewScannerSHX(reader, sizes[".shx"])
			if err != nil {
				errSHX = fmt.Errorf("NewScannerSHX: %w", err)
				return
			}
			scannerSHX = scanner
			estimatedSHX = (sizes[".shx"] - headerSize) / 8
		}
	}()

	wg.Wait()
	if err := errors.Join(errSHP, errDBF, errSHX); err != nil {
		return nil, err
	}

	return &Scanner{
		scanSHP:          scannerSHP,
		scanSHX:          scannerSHX,
		scanDBF:          scannerDBF,
		filePRJ:          prj,
		fileCPG:          cpg,
		estimatedRecords: max(estimatedDBF, estimatedSHX),
	}, nil
}

// Scan Scanner records.
func (s *Scanner) Scan() (recordSHP *SHPRecord, recordSHX *SHXRecord, recordDBF DBFRecord) {
	if s.err != nil {
		return nil, nil, nil
	}

	var wg sync.WaitGroup
	var errSHP, errSHX, errDBF error

	wg.Add(3)
	go func() {
		defer wg.Done()
		if s.scanSHP != nil {
			record, err := s.scanSHP.Scan()
			if err != nil {
				errSHP = fmt.Errorf("scanning SHP: %w", err)
			} else {
				recordSHP = record
			}
		}
	}()

	go func() {
		defer wg.Done()
		if s.scanDBF != nil {
			if record, err := s.scanDBF.Scan(); err != nil {
				errDBF = fmt.Errorf("scanning DBF: %w", err)
			} else {
				recordDBF = record
			}
		}
	}()

	go func() {
		defer wg.Done()
		if s.scanSHX != nil {
			if record, err := s.scanSHX.Scan(); err != nil {
				errSHX = fmt.Errorf("scanning SHX: %w", err)
			} else {
				recordSHX = record
			}
		}
	}()

	wg.Wait()
	if err := errors.Join(errSHP, errDBF, errSHX); err != nil {
		s.err = err
		return nil, nil, nil
	}

	s.scanRecords++
	return recordSHP, recordSHX, recordDBF
}

func (s *Scanner) Next() bool {
	return s.err == nil
}

// Discard Discards n records for concurrent scan.
func (s *Scanner) Discard(n int) (int, error) {
	var errSHP, errSHX, errDBF error
	var nSHP, nSHX, nDBF int
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if s.scanDBF != nil {
			nb, err := s.scanDBF.reader.Discard(n * s.scanDBF.header.RecordSize)
			if err != nil {
				errDBF = err
				nDBF = nb / s.scanDBF.header.RecordSize
				return
			}
			s.scanDBF.scanRecords += n
		}
	}()

	go func() {
		defer wg.Done()
		if s.scanSHX != nil {
			data, err := s.scanSHX.reader.Peek(8)
			if err != nil {
				errSHX = err
				return
			}
			record := ParseSHXRecord(data)
			offsetInit := record.Offset
			nb, err := s.scanSHX.reader.Discard(n * 8)
			if err != nil {
				nSHX = nb / 8
				errSHX = err
				return
			}
			s.scanSHX.scanRecords += n

			if s.scanSHP != nil {
				data, err := s.scanSHX.reader.Peek(8)
				if err != nil {
					errSHX = err
					return
				}
				record := ParseSHXRecord(data)
				offsetEnd := record.Offset
				nb, err := s.scanSHP.reader.Discard(offsetEnd - offsetInit)
				if err != nil {
					nSHP = nb / record.ContentLength
					errSHP = err
					return
				}
				s.scanSHP.scanRecords += n
			}
		} else if s.scanSHP != nil {
			errSHP = errors.New("can't discard .shp file without .shx file")
			return
		}
	}()

	wg.Wait()
	if err := errors.Join(errSHP, errDBF, errSHX); err != nil {
		s.err = err
		return max(nSHX, nDBF, nSHP), err
	}

	s.scanRecords += int64(n)
	return n, nil
}

func (s *Scanner) Close() error {
	var err error
	if s.scanDBF != nil {
		err = errors.Join(err, s.scanDBF.reader.Close())
	}
	if s.scanSHP != nil {
		err = errors.Join(err, s.scanSHP.reader.Close())
	}
	if s.scanSHX != nil {
		err = errors.Join(err, s.scanSHX.reader.Close())
	}
	return err
}

func (s *Scanner) ScannedRecords() int64 {
	return s.scanRecords
}

func (s *Scanner) EstimatedRecords() int64 {
	return s.estimatedRecords
}

func (s *Scanner) DBFHeader() *DBFHeader {
	if s.scanDBF != nil {
		return s.scanDBF.header
	}
	return nil
}

func (s *Scanner) DBFFieldDescriptors() []*DBFFieldDescriptor {
	if s.scanDBF != nil {
		return s.scanDBF.fieldDescriptors
	}
	return nil
}

func (s *Scanner) SHPHeader() *SHxHeader {
	if s.scanSHP != nil {
		return s.scanSHP.header
	}
	return nil
}

func (s *Scanner) SHxHeader() *SHxHeader {
	if s.scanSHX != nil {
		return s.scanSHX.header
	}
	return nil
}

func (s *Scanner) Charset() string {
	if s.fileCPG != nil {
		return s.fileCPG.Charset
	}
	return ""
}

func (s *Scanner) Projection() string {
	if s.filePRJ != nil {
		return s.filePRJ.Projection
	}
	return ""
}

func (s *Scanner) Error() error {
	return s.err
}

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
		s.err = err
		return nil, err
	}
	record := ParseSHXRecord(data)
	s.scanRecords++
	return &record, nil
}

type DBFRecord = []any

type ScannerDBF struct {
	reader           bufioReadCloser
	options          *ReadDBFOptions
	header           *DBFHeader
	fieldDescriptors []*DBFFieldDescriptor
	decoder          *encoding.Decoder
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

	return &ScannerDBF{
		reader:           bufioReadCloser{bufio.NewReader(reader), reader},
		options:          options,
		header:           header,
		fieldDescriptors: fieldDescriptors,
		decoder:          decoder,
	}, nil
}

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
			field, err := fieldDescriptor.ParseRecord(fieldData, s.decoder)
			if err != nil && !s.options.SkipBrokenFields {
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

func (s *ScannerDBF) FieldDescriptors() []*DBFFieldDescriptor {
	return s.fieldDescriptors
}
