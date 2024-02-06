package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

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
