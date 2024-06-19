package shapefile

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestIsMACOSX(t *testing.T) {
	type testCase struct {
		path     string
		expected bool
	}
	testCases := []testCase{
		{"__MACOSX/dir/._test.shp", true},
		{"dir/__MACOSX/._test.shp", true},
		{"dir/__MACOSX/dir/._test.shp", true},
		{"dir/__MACOSX/dir/__MACOSX/._test.shp", true},
		{"dir/._test.shp", false},
		{"dir/ABC__MACOSX", false},
		{"dir/ABC__MACOSX/._test.shp", false},
		{"dir/._test.shp.__MACOSX", false},
	}
	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			assert.Equal(t, tc.expected, isMacOSXPath(tc.path))
		})
	}
}
