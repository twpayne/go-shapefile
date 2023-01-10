package shapefile

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/twpayne/go-geom"
)

// A SHxHeader is header of a .shp or .shx file.
type SHxHeader struct {
	ShapeType ShapeType
	Bounds    *geom.Bounds
}

// readSHxHeader reads a SHxHeader from an io.Reader.
func readSHxHeader(r io.Reader, fileLength int64) (*SHxHeader, error) {
	if fileLength < headerSize {
		return nil, errors.New("file too short")
	}
	data := make([]byte, headerSize)
	if err := readFull(r, data); err != nil {
		return nil, err
	}
	return parseSHxHeader(data, fileLength)
}

// parseSHxHeader parses a SHxHeader from data.
func parseSHxHeader(data []byte, fileLength int64) (*SHxHeader, error) {
	if len(data) != headerSize {
		return nil, errors.New("invalid header length")
	}
	if headerFileCode := binary.BigEndian.Uint32(data[:4]); headerFileCode != fileCode {
		return nil, errors.New("invalid file code")
	}
	if headerFileLength := 2 * int64(binary.BigEndian.Uint32(data[24:28])); headerFileLength != fileLength {
		return nil, errors.New("invalid file length")
	}
	if headerVersion := binary.LittleEndian.Uint32(data[28:32]); headerVersion != version {
		return nil, errors.New("invalid header version")
	}

	shapeType := ShapeType(binary.LittleEndian.Uint32(data[32:36]))
	if _, validShapeType := validShapeTypes[shapeType]; !validShapeType {
		return nil, errors.New("invalid shape type")
	}
	if _, unsupportedShapeType := unsupportedShapeTypes[shapeType]; unsupportedShapeType {
		return nil, errors.New("unsupported shape type")
	}

	minX := math.Float64frombits(binary.LittleEndian.Uint64(data[36:44]))
	minY := math.Float64frombits(binary.LittleEndian.Uint64(data[44:52]))
	maxX := math.Float64frombits(binary.LittleEndian.Uint64(data[52:60]))
	maxY := math.Float64frombits(binary.LittleEndian.Uint64(data[60:68]))
	minZ := math.Float64frombits(binary.LittleEndian.Uint64(data[68:76]))
	maxZ := math.Float64frombits(binary.LittleEndian.Uint64(data[76:84]))
	minM := math.Float64frombits(binary.LittleEndian.Uint64(data[84:92]))
	maxM := math.Float64frombits(binary.LittleEndian.Uint64(data[92:100]))

	if NoData(minX) {
		minX = math.Inf(1)
	}
	if NoData(minY) {
		minY = math.Inf(1)
	}
	if NoData(maxX) {
		maxX = math.Inf(-1)
	}
	if NoData(maxY) {
		maxY = math.Inf(-1)
	}

	var bounds *geom.Bounds
	switch shapeType {
	case ShapeTypeNull:
	case ShapeTypePoint, ShapeTypeMultiPoint, ShapeTypePolyLine, ShapeTypePolygon:
		bounds = geom.NewBounds(geom.XY).Set(minX, minY, maxX, maxY)
	case ShapeTypePointM, ShapeTypeMultiPointM, ShapeTypePolyLineM, ShapeTypePolygonM:
		if NoData(minM) {
			minM = math.Inf(1)
		}
		if NoData(maxM) {
			maxM = math.Inf(-1)
		}
		bounds = geom.NewBounds(geom.XYM).Set(minX, minY, minM, maxX, maxY, maxM)
	case ShapeTypePointZ, ShapeTypeMultiPointZ, ShapeTypePolyLineZ, ShapeTypePolygonZ:
		if NoData(minM) {
			minM = math.Inf(1)
		}
		if NoData(maxM) {
			maxM = math.Inf(-1)
		}
		if NoData(minZ) {
			minZ = math.Inf(1)
		}
		if NoData(maxZ) {
			maxZ = math.Inf(-1)
		}
		bounds = geom.NewBounds(geom.XYZM).Set(minX, minY, minZ, minM, maxX, maxY, maxZ, maxM)
	}

	return &SHxHeader{
		ShapeType: shapeType,
		Bounds:    bounds,
	}, nil
}

// NoData returns if x represents no data.
func NoData(x float64) bool {
	return x <= -1e38
}

func readFull(r io.Reader, data []byte) error {
	for {
		switch n, err := r.Read(data); {
		case errors.Is(err, io.EOF) && n == len(data):
			return nil
		case err != nil:
			return err
		case n == 0:
			return io.ErrUnexpectedEOF
		case n < len(data):
			data = data[n:]
		default:
			return nil
		}
	}
}
