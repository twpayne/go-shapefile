package shapefile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/twpayne/go-geom"
)

var errUnexpectedEndOfData = errors.New("unexpected end of data")

type byteSliceReader struct {
	rest []byte
	err  error
}

func newByteSliceReader(data []byte) *byteSliceReader {
	return &byteSliceReader{
		rest: data,
	}
}

func (r *byteSliceReader) Err() error {
	return r.err
}

func (r *byteSliceReader) readEnds(layout geom.Layout, numParts, numPoints int) []int {
	if r.err != nil {
		return nil
	}
	if len(r.rest) < 4*numParts {
		r.err = errUnexpectedEndOfData
	}
	if part := binary.LittleEndian.Uint32(r.rest[:4]); part != 0 {
		r.err = fmt.Errorf("%d: invalid part", part)
		return nil
	}
	stride := layout.Stride()
	maxPart := stride * numPoints
	ends := make([]int, 0, numParts)
	for i := 1; i < numParts; i++ {
		part := stride * int(binary.LittleEndian.Uint32(r.rest[4*i:4*i+4]))
		if part > maxPart {
			r.err = fmt.Errorf("%d: invalid part", part)
			return nil
		}
		ends = append(ends, part)
	}
	ends = append(ends, maxPart)
	r.rest = r.rest[4*numParts:]
	return ends
}

func (r *byteSliceReader) readFloat64Pair() (float64, float64) {
	if r.err != nil {
		return 0, 0
	}
	if len(r.rest) < 16 {
		r.err = errUnexpectedEndOfData
		return 0, 0
	}
	a := math.Float64frombits(binary.LittleEndian.Uint64(r.rest[:8]))
	b := math.Float64frombits(binary.LittleEndian.Uint64(r.rest[8:16]))
	r.rest = r.rest[16:]
	return a, b
}

func (r *byteSliceReader) readFloat64s(n int) []float64 {
	if r.err != nil {
		return nil
	}
	if len(r.rest) < 8*n {
		r.err = errUnexpectedEndOfData
		return nil
	}
	float64s := make([]float64, 0, n)
	for i := range n {
		float64s = append(float64s, math.Float64frombits(binary.LittleEndian.Uint64(r.rest[8*i:8*i+8])))
	}
	r.rest = r.rest[8*n:]
	return float64s
}

func (r *byteSliceReader) readOrdinates(flatCoords []float64, n int, layout geom.Layout, index int) {
	if r.err != nil {
		return
	}
	if len(r.rest) < 8*n {
		r.err = errUnexpectedEndOfData
		return
	}
	stride := layout.Stride()
	for i := range n {
		flatCoords[i*stride+index] = math.Float64frombits(binary.LittleEndian.Uint64(r.rest[8*i : 8*i+8]))
	}
	r.rest = r.rest[8*n:]
}

func (r *byteSliceReader) readUint32() int {
	if r.err != nil {
		return 0
	}
	if len(r.rest) < 4 {
		r.err = errUnexpectedEndOfData
		return 0
	}
	u := int(binary.LittleEndian.Uint32(r.rest[:4]))
	r.rest = r.rest[4:]
	return u
}

func (r *byteSliceReader) readXYs(flatCoords []float64, n int, layout geom.Layout) {
	if r.err != nil {
		return
	}
	if len(r.rest) < 16*n {
		r.err = errUnexpectedEndOfData
		return
	}
	stride := layout.Stride()
	for i := range n {
		flatCoords[i*stride] = math.Float64frombits(binary.LittleEndian.Uint64(r.rest[16*i : 16*i+8]))
		flatCoords[i*stride+1] = math.Float64frombits(binary.LittleEndian.Uint64(r.rest[16*i+8 : 16*i+16]))
	}
	r.rest = r.rest[16*n:]
}
