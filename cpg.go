package shapefile

import (
	"archive/zip"
	"fmt"
	"io"
	"strings"

	"golang.org/x/net/html/charset"
)

// A PRJ is a .cpg file.
type CPG struct {
	Charset string
}

// ReadPRJ reads a CPG from an io.Reader.
func ReadCPG(r io.Reader, _ int64) (*CPG, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	enc, name := charset.Lookup(strings.ToLower(string(data)))
	if enc == nil {
		return nil, fmt.Errorf("unkown charset '%s'", (string(data)))
	}
	return &CPG{
		Charset: name,
	}, nil
}

// ReadCPGZipFile reads a CPG from a *zip.File.
func ReadCPGZipFile(zipFile *zip.File) (*CPG, error) {
	readCloser, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	cpg, err := ReadCPG(readCloser, int64(zipFile.UncompressedSize64))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", zipFile.Name, err)
	}
	return cpg, nil
}
