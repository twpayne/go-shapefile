package shapefile

// FIXME document all exported types
// FIXME validate XYZ and XYZM code
// FIXME do more validation, especially against the length of the file
// FIXME use .shx indexes
// FIXME factor out ParseSHPRecord

import (
	"archive/zip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/twpayne/go-geom"
)

type SHPRecord struct {
	Number        int
	ContentLength int
	ShapeType     ShapeType
	Bounds        *geom.Bounds
	Geom          geom.T
}

type ReadSHPOptions struct {
	MaxParts      int
	MaxPoints     int
	MaxRecordSize int
}

type SHP struct {
	SHxHeader
	Records []*SHPRecord
}

func ReadSHP(r io.Reader, fileLength int64, options *ReadSHPOptions) (*SHP, error) {
	header, err := ReadSHxHeader(r, fileLength)
	if err != nil {
		return nil, err
	}
	var records []*SHPRecord
RECORD:
	for recordNumber := 1; ; recordNumber++ {
		switch record, err := ReadSHPRecord(r, options); {
		case errors.Is(err, io.EOF):
			break RECORD
		case err != nil:
			return nil, fmt.Errorf("record %d: %w", recordNumber, err)
		case record.Number != recordNumber:
			return nil, fmt.Errorf("record %d: invalid record number", recordNumber)
		default:
			records = append(records, record)
		}
	}
	return &SHP{
		SHxHeader: *header,
		Records:   records,
	}, nil
}

func ReadSHPRecord(r io.Reader, options *ReadSHPOptions) (*SHPRecord, error) {
	recordHeaderData := make([]byte, 8)
	if err := readFull(r, recordHeaderData); err != nil {
		return nil, err
	}
	recordNumber := int(binary.BigEndian.Uint32(recordHeaderData[:4]))
	contentLength := 2 * int(binary.BigEndian.Uint32(recordHeaderData[4:8]))
	if contentLength < 4 {
		return nil, errors.New("content length too short")
	}
	if options != nil && options.MaxRecordSize != 0 && contentLength > options.MaxRecordSize {
		return nil, errors.New("content length too large")
	}

	recordData := make([]byte, contentLength)
	if err := readFull(r, recordData); err != nil {
		return nil, err
	}

	byteSliceReader := newByteSliceReader(recordData)

	shapeType := ShapeType(byteSliceReader.readUint32())
	expectedContentLength := 4

	if shapeType == ShapeTypeNull {
		if contentLength != expectedContentLength {
			return nil, errors.New("invalid content length")
		}
		return &SHPRecord{
			Number:        recordNumber,
			ContentLength: contentLength,
			ShapeType:     ShapeTypeNull,
		}, nil
	}

	layout := geom.NoLayout
	switch shapeType {
	case ShapeTypeNull:
	case ShapeTypePoint, ShapeTypeMultiPoint, ShapeTypePolyLine, ShapeTypePolygon:
		layout = geom.XY
	case ShapeTypePointM, ShapeTypeMultiPointM, ShapeTypePolyLineM, ShapeTypePolygonM:
		layout = geom.XYM
	case ShapeTypePointZ, ShapeTypeMultiPointZ, ShapeTypePolyLineZ, ShapeTypePolygonZ:
		layout = geom.XYZM
	}

	switch shapeType {
	case ShapeTypePoint, ShapeTypePointM, ShapeTypePointZ:
		flatCoords := byteSliceReader.readFloat64s(layout.Stride())
		expectedContentLength += 8 * layout.Stride()
		if contentLength != expectedContentLength {
			return nil, errors.New("invalid content length")
		}
		return &SHPRecord{
			Number:        recordNumber,
			ContentLength: contentLength,
			ShapeType:     shapeType,
			Geom:          geom.NewPointFlat(layout, flatCoords),
		}, nil
	}

	minX, minY := byteSliceReader.readFloat64Pair()
	maxX, maxY := byteSliceReader.readFloat64Pair()
	expectedContentLength += 8 * 4

	var numParts int
	switch shapeType {
	case ShapeTypePolyLine, ShapeTypePolyLineM, ShapeTypePolyLineZ:
		fallthrough
	case ShapeTypePolygon, ShapeTypePolygonM, ShapeTypePolygonZ:
		numParts = byteSliceReader.readUint32()
		if numParts == 0 {
			return nil, errors.New("invalid number of parts")
		}
		if options != nil && options.MaxParts != 0 && numParts > options.MaxParts {
			return nil, errors.New("too many parts")
		}
		expectedContentLength += 4 + 4*numParts
	}

	numPoints := byteSliceReader.readUint32()
	if options != nil && options.MaxPoints != 0 && numPoints > options.MaxPoints {
		return nil, errors.New("too many points")
	}
	expectedContentLength += 4

	switch layout {
	case geom.XY:
		expectedContentLength += 8 * 2 * numPoints
	case geom.XYM:
		expectedContentLength += 8*2*numPoints + 8*2 + 8*numPoints
	case geom.XYZM:
		expectedContentLength += 8*2*numPoints + 8*2 + 8*numPoints + 8*2 + 8*numPoints
	}

	if contentLength != expectedContentLength {
		return nil, errors.New("invalid content length")
	}

	var ends []int
	switch shapeType {
	case ShapeTypePolyLine, ShapeTypePolyLineM, ShapeTypePolyLineZ:
		fallthrough
	case ShapeTypePolygon, ShapeTypePolygonM, ShapeTypePolygonZ:
		ends = byteSliceReader.readEnds(layout, numParts, numPoints)
	}

	flatCoords := make([]float64, layout.Stride()*numPoints)
	byteSliceReader.readXYs(flatCoords, numPoints, layout)

	var bounds *geom.Bounds
	switch layout {
	case geom.XY:
		bounds = geom.NewBounds(geom.XY).Set(minX, minY, maxX, maxY)
	case geom.XYM:
		minM, maxM := byteSliceReader.readFloat64Pair()
		byteSliceReader.readOrdinates(flatCoords, numPoints, layout, layout.MIndex())
		bounds = geom.NewBounds(geom.XYM).Set(minX, minY, minM, maxX, maxY, maxM)
	case geom.XYZM:
		minZ, maxZ := byteSliceReader.readFloat64Pair()
		byteSliceReader.readOrdinates(flatCoords, numPoints, layout, layout.ZIndex())
		minM, maxM := byteSliceReader.readFloat64Pair()
		byteSliceReader.readOrdinates(flatCoords, numPoints, layout, layout.MIndex())
		bounds = geom.NewBounds(geom.XYZM).Set(minX, minY, minZ, minM, maxX, maxY, maxZ, maxM)
	}

	if err := byteSliceReader.Err(); err != nil {
		return nil, err
	}

	var g geom.T
	switch shapeType {
	case ShapeTypeMultiPoint, ShapeTypeMultiPointM, ShapeTypeMultiPointZ:
		g = geom.NewMultiPointFlat(layout, flatCoords)
	case ShapeTypePolyLine, ShapeTypePolyLineM, ShapeTypePolyLineZ:
		g = geom.NewMultiLineStringFlat(layout, flatCoords, ends)
	case ShapeTypePolygon, ShapeTypePolygonM, ShapeTypePolygonZ:
		g = geom.NewPolygonFlat(layout, flatCoords, ends)
	}

	return &SHPRecord{
		Number:        recordNumber,
		ContentLength: contentLength,
		ShapeType:     shapeType,
		Bounds:        bounds,
		Geom:          g,
	}, nil
}

func ReadSHPZipFile(zipFile *zip.File, options *ReadSHPOptions) (*SHP, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	return ReadSHP(readCloser, int64(zipFile.UncompressedSize64), options)
}

func (s *SHP) Record(i int) geom.T {
	return s.Records[i].Geom
}
