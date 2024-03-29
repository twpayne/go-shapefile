package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func FuzzReadDBF(f *testing.F) {
	assert.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".dbf"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadDBF(r, int64(len(data)), &ReadDBFOptions{
			MaxHeaderSize: 4096,
			MaxRecordSize: 4096,
			MaxRecords:    4096,
		})
	})
}
