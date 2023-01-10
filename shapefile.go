// Package shapefile reads ESRI Shapefiles.
//
// See https://support.esri.com/en/white-paper/279.
package shapefile

// FIXME make everything robust to malicious inputs
// FIXME fuzz testing
// FIXME tidy up errors
// FIXME provide lazy, random access to individual records, using SHX
// FIXME cross-file validation of offsets and record lengths

import (
	"archive/zip"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/twpayne/go-geom"
)

const (
	headerSize = 100
	fileCode   = 9994
	version    = 1000
)

// A ShapeType is a shape type.
type ShapeType uint

// Shape types.
const (
	ShapeTypeNull        ShapeType = 0
	ShapeTypePoint       ShapeType = 1
	ShapeTypePolyLine    ShapeType = 3
	ShapeTypePolygon     ShapeType = 5
	ShapeTypeMultiPoint  ShapeType = 8
	ShapeTypePointZ      ShapeType = 11
	ShapeTypePolyLineZ   ShapeType = 13
	ShapeTypePolygonZ    ShapeType = 15
	ShapeTypeMultiPointZ ShapeType = 18
	ShapeTypePointM      ShapeType = 21
	ShapeTypePolyLineM   ShapeType = 23
	ShapeTypePolygonM    ShapeType = 25
	ShapeTypeMultiPointM ShapeType = 28
	ShapeTypeMultiPatch  ShapeType = 31
)

var (
	validShapeTypes = map[ShapeType]struct{}{
		ShapeTypeNull:        {},
		ShapeTypePoint:       {},
		ShapeTypePolyLine:    {},
		ShapeTypePolygon:     {},
		ShapeTypeMultiPoint:  {},
		ShapeTypePointM:      {},
		ShapeTypePolyLineM:   {},
		ShapeTypePolygonM:    {},
		ShapeTypeMultiPointM: {},
		ShapeTypePointZ:      {},
		ShapeTypePolyLineZ:   {},
		ShapeTypePolygonZ:    {},
		ShapeTypeMultiPointZ: {},
		ShapeTypeMultiPatch:  {},
	}
	unsupportedShapeTypes = map[ShapeType]struct{}{
		ShapeTypeMultiPatch: {}, // FIXME
	}
)

// A Shapefile is an ESRI Shapefile.
type Shapefile struct {
	DBF *DBF
	PRJ *PRJ
	SHP *SHP
	SHX *SHX
}

// ReadShapefileOptions are options to ReadFS.
type ReadShapefileOptions struct {
	DBF *ReadDBFOptions
	SHP *ReadSHPOptions
}

// ReadFS reads a Shapefile from fsys with the given basename.
func ReadFS(fsys fs.FS, basename string, options *ReadShapefileOptions) (*Shapefile, error) {
	var dbf *DBF
	switch dbfFile, err := fsys.Open(basename + ".dbf"); {
	case errors.Is(err, fs.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, err
	default:
		defer dbfFile.Close()
		fileInfo, err := dbfFile.Stat()
		if err != nil {
			return nil, err
		}
		var readDBFOptions *ReadDBFOptions
		if options != nil {
			readDBFOptions = options.DBF
		}
		dbf, err = ReadDBF(dbfFile, fileInfo.Size(), readDBFOptions)
		if err != nil {
			return nil, fmt.Errorf("%s.dbf: %w", basename, err)
		}
	}

	var prj *PRJ
	switch prjFile, err := fsys.Open(basename + ".prj"); {
	case errors.Is(err, fs.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, err
	default:
		defer prjFile.Close()
		fileInfo, err := prjFile.Stat()
		if err != nil {
			return nil, err
		}
		prj, err = ReadPRJ(prjFile, fileInfo.Size())
		if err != nil {
			return nil, fmt.Errorf("%s.prj: %w", basename, err)
		}
	}

	var shp *SHP
	switch shpFile, err := fsys.Open(basename + ".shp"); {
	case errors.Is(err, fs.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, err
	default:
		defer shpFile.Close()
		fileInfo, err := shpFile.Stat()
		if err != nil {
			return nil, err
		}
		var readSHPOptions *ReadSHPOptions
		if options != nil {
			readSHPOptions = options.SHP
		}
		shp, err = ReadSHP(shpFile, fileInfo.Size(), readSHPOptions)
		if err != nil {
			return nil, fmt.Errorf("%s.shp: %w", basename, err)
		}
	}

	var shx *SHX
	switch shxFile, err := fsys.Open(basename + ".shx"); {
	case errors.Is(err, fs.ErrNotExist):
		// Do nothing.
	case err != nil:
		return nil, err
	default:
		defer shxFile.Close()
		fileInfo, err := shxFile.Stat()
		if err != nil {
			return nil, err
		}
		shx, err = ReadSHX(shxFile, fileInfo.Size())
		if err != nil {
			return nil, fmt.Errorf("%s.shx: %w", basename, err)
		}
	}

	return &Shapefile{
		DBF: dbf,
		PRJ: prj,
		SHP: shp,
		SHX: shx,
	}, nil
}

// ReadZipFile reads a Shapefile from a .zip file.
func ReadZipFile(name string, options *ReadShapefileOptions) (*Shapefile, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	zipReader, err := zip.NewReader(file, fileInfo.Size())
	if err != nil {
		return nil, err
	}

	shapefile, err := ReadZipReader(zipReader, options)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return shapefile, nil
}

// ReadZipReader reads a Shapefile from a *zip.Reader.
func ReadZipReader(zipReader *zip.Reader, options *ReadShapefileOptions) (*Shapefile, error) {
	var dbfFiles []*zip.File
	var prjFiles []*zip.File
	var shxFiles []*zip.File
	var shpFiles []*zip.File
	for _, zipFile := range zipReader.File {
		switch strings.ToLower(filepath.Ext(zipFile.Name)) {
		case ".dbf":
			dbfFiles = append(dbfFiles, zipFile)
		case ".prj":
			prjFiles = append(prjFiles, zipFile)
		case ".shp":
			shpFiles = append(shpFiles, zipFile)
		case ".shx":
			shxFiles = append(shxFiles, zipFile)
		}
	}

	var dbf *DBF
	switch len(dbfFiles) {
	case 0:
		// Do nothing.
	case 1:
		var readDBFOptions *ReadDBFOptions
		if options != nil {
			readDBFOptions = options.DBF
		}
		var err error
		dbf, err = ReadDBFZipFile(dbfFiles[0], readDBFOptions)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("too many .dbf files")
	}

	var prj *PRJ
	switch len(prjFiles) {
	case 0:
		// Do nothing.
	case 1:
		var err error
		prj, err = ReadPRJZipFile(prjFiles[0])
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("too many .prj files")
	}

	var shp *SHP
	switch len(shpFiles) {
	case 0:
		// Do nothing.
	case 1:
		var readSHPOptions *ReadSHPOptions
		if options != nil {
			readSHPOptions = options.SHP
		}
		var err error
		shp, err = ReadSHPZipFile(shpFiles[0], readSHPOptions)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("too many .shp files")
	}

	var shx *SHX
	switch len(shxFiles) {
	case 0:
		// Do nothing.
	case 1:
		var err error
		shx, err = ReadSHXZipFile(shxFiles[0])
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("too many .shx files")
	}

	if dbf != nil && shp != nil && len(dbf.Records) != len(shp.Records) ||
		dbf != nil && shx != nil && len(dbf.Records) != len(shx.Records) ||
		shp != nil && shx != nil && len(shp.Records) != len(shx.Records) {
		return nil, errors.New("inconsistent number of records")
	}

	return &Shapefile{
		DBF: dbf,
		PRJ: prj,
		SHP: shp,
		SHX: shx,
	}, nil
}

// NumRecords returns the number of records in s.
func (s *Shapefile) NumRecords() int {
	switch {
	case s.DBF != nil:
		return len(s.DBF.Records)
	case s.SHP != nil:
		return len(s.SHP.Records)
	case s.SHX != nil:
		return len(s.SHX.Records)
	default:
		return 0
	}
}

// Record returns s's ith record's fields and geometry.
func (s *Shapefile) Record(i int) (map[string]any, geom.T) {
	var fields map[string]any
	if s.DBF != nil {
		fields = s.DBF.Record(i)
	}
	var g geom.T
	if s.SHP != nil {
		g = s.SHP.Record(i)
	}
	return fields, g
}
