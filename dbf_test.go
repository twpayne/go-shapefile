package shapefile

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzReadDBF(f *testing.F) {
	require.NoError(f, addFuzzDataFromFS(f, os.DirFS("."), "testdata", ".dbf"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadDBF(r, int64(len(data)))
	})
}
