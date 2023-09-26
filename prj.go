package shapefile

import (
	"archive/zip"
	"fmt"
	"io"
)

// A PRJ is a .prj file.
type PRJ struct {
	Projection string
}

// ReadPRJ reads a PRJ from an io.Reader.
func ReadPRJ(r io.Reader, _ int64) (*PRJ, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return &PRJ{
		Projection: string(data),
	}, nil
}

// ReadPRJZipFile reads a PRJ from a *zip.File.
func ReadPRJZipFile(zipFile *zip.File) (*PRJ, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	prj, err := ReadPRJ(readCloser, int64(zipFile.UncompressedSize64))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", zipFile.Name, err)
	}
	return prj, nil
}
