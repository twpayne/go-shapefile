package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzReadSHP(f *testing.F) {
	require.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".shp"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadSHP(r, int64(len(data)), &ReadSHPOptions{
			MaxParts: 128,
		})
	})
}
