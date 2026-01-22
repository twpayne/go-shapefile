package shapefile

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

// A SHPRecord is a record in a SHP file.
type SHPRecord struct {
	Number        int
	ContentLength int
	ShapeType     ShapeType
	Bounds        *geom.Bounds
	Geom          geom.T
}

// ReadSHPOptions are options for ReadSHP.
type ReadSHPOptions struct {
	MaxParts      int
	MaxPoints     int
	MaxRecordSize int
}

// A SHP is a .shp file.
type SHP struct {
	SHxHeader

	Records []*SHPRecord
}

// ReadSHP reads a SHP from an io.Reader.
func ReadSHP(r io.Reader, fileLength int64, options *ReadSHPOptions) (*SHP, error) {
	header, err := readSHxHeader(r, fileLength)
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
			return nil, fmt.Errorf("record %d: invalid record number (expected %d)", recordNumber, record.Number)
		default:
			records = append(records, record)
		}
	}
	return &SHP{
		SHxHeader: *header,
		Records:   records,
	}, nil
}

// ReadSHPRecord reads the next *SHPRecord from r.
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
		endss, err := makeMultiPolygonEndss(layout, flatCoords, ends)
		if err != nil {
			return nil, err
		}
		g = geom.NewMultiPolygonFlat(layout, flatCoords, endss)
	}

	return &SHPRecord{
		Number:        recordNumber,
		ContentLength: contentLength,
		ShapeType:     shapeType,
		Bounds:        bounds,
		Geom:          g,
	}, nil
}

// ReadSHPZipFile reads a *SHP from a *zip.File.
func ReadSHPZipFile(zipFile *zip.File, options *ReadSHPOptions) (*SHP, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	shp, err := ReadSHP(readCloser, int64(zipFile.UncompressedSize64), options)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", zipFile.Name, err)
	}
	return shp, nil
}

// Record returns the ith geometry.
func (s *SHP) Record(i int) geom.T {
	return s.Records[i].Geom
}

// makeMultiPolygonEndss returns the multipolygon endss by inspecting the
// orientation of the rings defined by flatCoords and ends. Each clockwise ring
// defines the outer ring of a new polygon, and each anti-clockwise ring defines
// an inner ring of the current polygon.
//
// All rings are assumed to be in order, that is that inner rings always belong
// to the polygon with the most recently defined outer ring.
//
// From the Shapefile specification:
//
// A polygon consists of one or more rings. A ring is a connected sequence of
// four or more points that form a closed, non-self-intersecting loop. A polygon
// may contain multiple outer rings. The order of vertices or orientation for a
// ring indicates which side of the ring is the interior of the polygon. The
// neighborhood to the right of an observer walking along the ring in vertex
// order is the neighborhood inside the polygon. Vertices of rings defining
// holes in polygons are in a counterclockwise direction. Vertices for a single,
// ringed polygon are, therefore, always in clockwise order. The rings of a
// polygon are referred to as its parts.
//
// Because this specification does not forbid consecutive points with identical
// coordinates, shapefile readers must handle such cases. On the other hand, the
// degenerate, zero length or zero area parts that might result are not allowed.
func makeMultiPolygonEndss(layout geom.Layout, flatCoords []float64, ends []int) ([][]int, error) {
	var endss [][]int
	polygonOffset := 0
	offset := 0
	stride := layout.Stride()
	for i, end := range ends {
		if (end-offset)/stride < 4 {
			return nil, errors.New("too few points in ring")
		}
		switch doubleArea := doubleArea(flatCoords, offset, end, stride); {
		case doubleArea == 0:
			return nil, errors.New("zero area ring")
		case i != 0 && doubleArea < 0:
			endss = append(endss, ends[polygonOffset:i])
			polygonOffset = i
		}
		offset = end
	}
	if len(ends) > 0 {
		endss = append(endss, ends[polygonOffset:])
	}
	return endss, nil
}

// doubleArea returns double the area of the polygon from offset to end in
// flatCoords.
func doubleArea(flatCoords []float64, offset, end, stride int) float64 {
	var doubleArea float64
	for i := offset + stride; i < end; i += stride {
		doubleArea += (flatCoords[i+1] - flatCoords[i+1-stride]) * (flatCoords[i] + flatCoords[i-stride])
	}
	return doubleArea
}
