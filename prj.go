package shapefile

// FIXME document all exported types

import (
	"archive/zip"
	"io"
)

type PRJ struct {
	Projection string
}

func ReadPRJ(r io.Reader, size int64) (*PRJ, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return &PRJ{
		Projection: string(data),
	}, nil
}

func ReadPRJZipFile(zipFile *zip.File) (*PRJ, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	return ReadPRJ(readCloser, int64(zipFile.UncompressedSize64))
}
