package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func FuzzReadSHX(f *testing.F) {
	assert.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".shx"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadSHX(r, int64(len(data)))
	})
}
