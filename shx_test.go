package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzReadSHX(f *testing.F) {
	require.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".shx"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadSHX(r, int64(len(data)))
	})
}
