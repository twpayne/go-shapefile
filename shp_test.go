package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twpayne/go-geom"
)

func TestMakeMultiPolygonEndss(t *testing.T) {
	for _, tc := range []struct {
		name        string
		layout      geom.Layout
		flatCoords  []float64
		ends        []int
		expected    [][]int
		expectedErr string
	}{
		{
			name:   "empty",
			layout: geom.XY,
		},
		{
			name:       "single_polygon_without_hole",
			layout:     geom.XY,
			flatCoords: []float64{0, 0, 0, 4, 4, 0, 0, 0},
			ends:       []int{8},
			expected:   [][]int{{8}},
		},
		{
			name:       "single_polygon_with_hole",
			layout:     geom.XY,
			flatCoords: []float64{0, 0, 0, 4, 4, 0, 0, 0, 1, 1, 2, 1, 1, 2, 1, 1},
			ends:       []int{8, 16},
			expected:   [][]int{{8, 16}},
		},
		{
			name:   "two_polygons_without_holes",
			layout: geom.XY,
			flatCoords: []float64{
				0, 0, 0, 4, 4, 0, 0, 0,
				5, 1, 1, 5, 5, 5, 5, 1,
			},
			ends:     []int{8, 16},
			expected: [][]int{{8}, {16}},
		},
		{
			name:   "two_polygons_with_holes",
			layout: geom.XY,
			flatCoords: []float64{
				0, 0, 0, 4, 4, 0, 0, 0, 1, 1, 2, 1, 1, 2, 1, 1,
				5, 1, 1, 5, 5, 5, 5, 1, 4, 3, 4, 4, 3, 4, 4, 3,
			},
			ends:     []int{8, 16, 24, 32},
			expected: [][]int{{8, 16}, {24, 32}},
		},
		{
			name:        "too_few_points_in_ring",
			layout:      geom.XY,
			flatCoords:  []float64{0, 0, 0, 4, 4, 0},
			ends:        []int{6},
			expectedErr: "too few points in ring",
		},
		{
			name:        "zero_area_ring",
			layout:      geom.XY,
			flatCoords:  []float64{0, 0, 0, 4, 4, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
			ends:        []int{8, 16},
			expectedErr: "zero area ring",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := makeMultiPolygonEndss(tc.layout, tc.flatCoords, tc.ends)
			if tc.expectedErr != "" {
				assert.EqualError(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, actual)
			}
		})
	}
}

func FuzzReadSHP(f *testing.F) {
	assert.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".shp"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadSHP(r, int64(len(data)), &ReadSHPOptions{
			MaxParts:      128,
			MaxPoints:     128,
			MaxRecordSize: 4096,
		})
	})
}
