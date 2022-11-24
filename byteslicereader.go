package shapefile

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/twpayne/go-geom"
)

type byteSliceReader []byte

func (r *byteSliceReader) readEnds(n, last int) ([]int, error) {
	sl := *r
	if part := binary.LittleEndian.Uint32(sl[:4]); part != 0 {
		return nil, fmt.Errorf("%d: invalid part", part)
	}
	ends := make([]int, 0, n)
	for i := 1; i < n; i++ {
		part := int(binary.LittleEndian.Uint32(sl[4*i : 4*i+4]))
		if part > last {
			return nil, fmt.Errorf("%d: invalid part", part)
		}
		ends = append(ends, part)
	}
	ends = append(ends, last)
	*r = sl[4*n:]
	return ends, nil
}

func (r *byteSliceReader) readFloat64Pair() (float64, float64) {
	sl := *r
	a := math.Float64frombits(binary.LittleEndian.Uint64(sl[:8]))
	b := math.Float64frombits(binary.LittleEndian.Uint64(sl[8:16]))
	*r = sl[16:]
	return a, b
}

func (r *byteSliceReader) readFloat64s(n int) []float64 {
	sl := *r
	float64s := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		float64s = append(float64s, math.Float64frombits(binary.LittleEndian.Uint64(sl[8*i:8*i+8])))
	}
	*r = sl[8*n:]
	return float64s
}

func (r *byteSliceReader) readOrdinates(flatCoords []float64, n int, layout geom.Layout, index int) {
	sl := *r
	stride := layout.Stride()
	for i := 0; i < n; i++ {
		flatCoords[i*stride+index] = math.Float64frombits(binary.LittleEndian.Uint64(sl[8*i : 8*i+8]))
	}
	*r = sl[8*n:]
}

func (r *byteSliceReader) readUint32() int {
	sl := *r
	u := int(binary.LittleEndian.Uint32(sl[:4]))
	*r = sl[4:]
	return u
}

func (r *byteSliceReader) readXYs(flatCoords []float64, n int, layout geom.Layout) {
	sl := *r
	stride := layout.Stride()
	for i := 0; i < n; i++ {
		flatCoords[i*stride] = math.Float64frombits(binary.LittleEndian.Uint64(sl[16*i : 16*i+8]))
		flatCoords[i*stride+1] = math.Float64frombits(binary.LittleEndian.Uint64(sl[16*i+8 : 16*i+16]))
	}
	*r = sl[16*n:]
}
