package shapefile

import (
	"bytes"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzReadSHP(f *testing.F) {
	require.NoError(f, fs.WalkDir(os.DirFS("."), "testdata", func(path string, dirEntry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(path, ".shp") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		f.Add(data)
		return nil
	}))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = ReadSHP(r, int64(len(data)))
	})
}
