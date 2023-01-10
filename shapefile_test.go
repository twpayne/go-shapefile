package shapefile

import (
	"archive/zip"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
)

func TestReadFS(t *testing.T) {
	for _, tc := range []struct {
		skipReason         string
		basename           string
		hasDBF             bool
		hasPRJ             bool
		hasSHX             bool
		expectedErr        string
		expectedShapeType  ShapeType
		expectedBounds     *geom.Bounds
		expectedNumRecords int
		expectedGeom0      geom.T
		expectedDBFRecord0 []any
	}{
		{
			basename:           "line",
			hasSHX:             true,
			expectedShapeType:  ShapeTypePolyLine,
			expectedBounds:     geom.NewBounds(geom.XY).Set(1, 1, 5, 6),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "MULTILINESTRING ((1 5,5 5,5 1,3 3,1 1),(3 2,2 6))"), //nolint:dupword
		},
		{
			basename:           "linem",
			expectedShapeType:  ShapeTypePolyLineM,
			expectedBounds:     geom.NewBounds(geom.XYM).Set(1, 1, 0, 5, 6, 3),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "MULTILINESTRING M ((1 5 0,5 5 -1E+39,5 1 3,3 3 -1E+39,1 1 0),(3 2 -1E+39,2 6 -1E+39))"),
		},
		{
			basename:           "linez",
			expectedShapeType:  ShapeTypePolyLineZ,
			expectedBounds:     geom.NewBounds(geom.XYZM).Set(1, 1, 0, 0, 5, 9, 22, 3),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "MULTILINESTRING ZM ((1 5 18 -1E+39,5 5 20 -1E+39,5 1 22 -1E+39,3 3 0 -1E+39,1 1 0 -1E+39),(3 2 0 -1E+39,2 6 0 -1E+39),(3 2 15 0,2 6 13 3,1 9 14 2))"),
		},
		{
			skipReason:        "multipatch is not supported",
			basename:          "multipatch",
			expectedShapeType: ShapeTypeMultiPatch,
		},
		{
			basename:           "multipoint",
			expectedShapeType:  ShapeTypeMultiPoint,
			expectedBounds:     geom.NewBounds(geom.XY).Set(122, 32, 124, 37),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "MULTIPOINT ((122 37),(124 32))"),
		},
		{
			basename:           "multipointz",
			expectedShapeType:  ShapeTypeMultiPointZ,
			expectedBounds:     geom.NewBounds(geom.XYZM).Set(1422671.7232666016, 4188903.4295959473, 71.99445343017578, math.Inf(1), 1422672.1022949219, 4188903.7578430176, 72.00995635986328, math.Inf(-1)),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "MULTIPOINT ZM ((1422671.7232666016 4188903.4295959473 72.00995635986328 -1E38),(1422672.1022949219 4188903.4295959473 72.0060806274414 -1E38),(1422671.9127807617 4188903.7578430176 72.00220489501953 -1E38),(1422671.9127807617 4188903.539001465 71.99445343017578 -1E38))"),
		},
		{
			basename:           "point",
			hasSHX:             true,
			expectedShapeType:  ShapeTypePoint,
			expectedBounds:     geom.NewBounds(geom.XY).Set(122, 37, 122, 37),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "POINT (122 37)"),
		},
		{
			skipReason:         "bounds in header do not match bounds of data; record 1 has record number 0, should be 1",
			basename:           "pointz",
			expectedShapeType:  ShapeTypePointZ,
			expectedBounds:     geom.NewBounds(geom.XYZM).Set(1422459.0908050265, 4188942.211755641, 72.40956470558095, 0, 1422464.3681007193, 4188962.3364355816, 72.58286959604922, 0),
			expectedNumRecords: 2,
			expectedGeom0:      newGeomFromWKT(t, "POINT ZM (1422464.3681007193 4188962.3364355816 72.40956470558095 -1e+39)"),
		},
		{
			basename:           "polygon_hole",
			hasSHX:             true,
			expectedShapeType:  ShapeTypePolygon,
			expectedBounds:     geom.NewBounds(geom.XY).Set(-120, -60, 120, 60),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "POLYGON ((-120 60,120 60,120 -60,-120 -60,-120 60),(-60 30,-60 -30,60 -30,60 30,-60 30))"), //nolint:dupword
		},
		{
			skipReason:         "rings are not closed",
			basename:           "polygon",
			expectedShapeType:  ShapeTypePolygon,
			expectedBounds:     geom.NewBounds(geom.XY).Set(15, 2, 122, 37),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "POLYGON ((122 37,117 36,115 32,118 20,113 24,122 37),(15 2,17 6,22 7,15 2),(122 37,117 36,115 32,122 37))"),
		},
		{
			basename:           "polygonm",
			expectedShapeType:  ShapeTypePolygonM,
			expectedBounds:     geom.NewBounds(geom.XYM).Set(159374.30785312195, 5403473.287488617, 0, 160420.36722814097, 5404314.139043656, 0),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "POLYGON M ((159814.75390576152 5404314.139043656 0,160420.36722814097 5403703.520652497 0,159374.30785312195 5403473.287488617 0,159814.753905761517 5404314.139043656 0))"),
		},
		{
			basename:           "polygonz",
			expectedShapeType:  ShapeTypePolygonZ,
			expectedBounds:     geom.NewBounds(geom.XYZM).Set(1422691.1637959871, 4188837.293869424, 0, math.Inf(1), 1422692.1644789441, 4188838.2945523816, 0, math.Inf(-1)),
			expectedNumRecords: 1,
			expectedGeom0:      newGeomFromWKT(t, "POLYGON ZM ((1422692.1644789441 4188837.794210903 72.46632654472523 0, 1422692.1625749937 4188837.75060327 72.46632654472523 1, 1422692.156877633 4188837.7073275167 72.46632654472523 2, 1422692.1474302218 4188837.664712999 72.46632654472523 3, 1422692.1343046608 4188837.6230840385 72.46632654472523 4, 1422692.1176008438 4188837.582757457 72.46632654472523 5, 1422692.0974458966 4188837.5440401635 72.46632654472523 6, 1422692.0739932107 4188837.5072268206 72.46632654472523 7, 1422692.047421275 4188837.4725976 72.46632654472523 8, 1422692.017932318 4188837.4404160506 72.46632654472523 9, 1422691.9857507686 4188837.4109270936 72.46632654472523 10, 1422691.951121548 4188837.384355158 72.46632654472523 11, 1422691.914308205 4188837.360902472 72.46632654472523 12, 1422691.8755909116 4188837.3407475245 72.46632654472523 13, 1422691.8352643298 4188837.3240437075 72.46632654472523 14, 1422691.7936353693 4188837.3109181467 72.46632654472523 15, 1422691.7510208515 4188837.3014707356 72.46632654472523 16, 1422691.7077450987 4188837.295773375 72.46632654472523 17, 1422691.6641374656 4188837.293869424 72.46632654472523 18, 1422691.6205298326 4188837.295773375 72.46632654472523 19, 1422691.5772540797 4188837.3014707356 72.46632654472523 20, 1422691.534639562 4188837.3109181467 72.46632654472523 21, 1422691.4930106015 4188837.3240437075 72.46632654472523 22, 1422691.4526840197 4188837.3407475245 72.46632654472523 23, 1422691.4139667263 4188837.360902472 72.46632654472523 24, 1422691.3771533833 4188837.384355158 72.46632654472523 25, 1422691.3425241627 4188837.4109270936 72.46632654472523 26, 1422691.3103426134 4188837.4404160506 72.46632654472523 27, 1422691.2808536564 4188837.4725976 72.46632654472523 28, 1422691.2542817206 4188837.5072268206 72.46632654472523 29, 1422691.2308290347 4188837.5440401635 72.46632654472523 30, 1422691.2106740875 4188837.582757457 72.46632654472523 31, 1422691.1939702705 4188837.6230840385 72.46632654472523 32, 1422691.1808447095 4188837.664712999 72.46632654472523 33, 1422691.1713972983 4188837.7073275167 72.46632654472523 34, 1422691.1656999376 4188837.75060327 72.46632654472523 35, 1422691.1637959871 4188837.794210903 72.46632654472523 36, 1422691.1656999376 4188837.837818536 72.46632654472523 37, 1422691.1713972983 4188837.881094289 72.46632654472523 38, 1422691.1808447095 4188837.9237088067 72.46632654472523 39, 1422691.1939702705 4188837.9653377673 72.46632654472523 40, 1422691.2106740875 4188838.0056643486 72.46632654472523 41, 1422691.2308290347 4188838.0443816422 72.46632654472523 42, 1422691.2542817206 4188838.081194985 72.46632654472523 43, 1422691.2808536564 4188838.115824206 72.46632654472523 44, 1422691.3103426134 4188838.148005755 72.46632654472523 45, 1422691.3425241627 4188838.177494712 72.46632654472523 46, 1422691.3771533833 4188838.2040666477 72.46632654472523 47, 1422691.4139667263 4188838.227519334 72.46632654472523 48, 1422691.4526840197 4188838.2476742812 72.46632654472523 49, 1422691.4930106015 4188838.2643780983 72.46632654472523 50, 1422691.534639562 4188838.277503659 72.46632654472523 51, 1422691.5772540797 4188838.28695107 72.46632654472523 52, 1422691.6205298326 4188838.292648431 72.46632654472523 53, 1422691.6641374656 4188838.2945523816 72.46632654472523 54, 1422691.7077450987 4188838.292648431 72.46632654472523 55, 1422691.7510208515 4188838.28695107 72.46632654472523 56, 1422691.7936353693 4188838.277503659 72.46632654472523 57, 1422691.8352643298 4188838.2643780983 72.46632654472523 58, 1422691.8755909116 4188838.2476742812 72.46632654472523 59, 1422691.914308205 4188838.227519334 72.46632654472523 60, 1422691.951121548 4188838.2040666477 72.46632654472523 61, 1422691.9857507686 4188838.177494712 72.46632654472523 62, 1422692.017932318 4188838.148005755 72.46632654472523 63, 1422692.047421275 4188838.115824206 72.46632654472523 64, 1422692.0739932107 4188838.081194985 72.46632654472523 65, 1422692.0974458966 4188838.0443816422 72.46632654472523 66, 1422692.1176008438 4188838.0056643486 72.46632654472523 67, 1422692.1343046608 4188837.9653377673 72.46632654472523 68, 1422692.1474302218 4188837.9237088067 72.46632654472523 69, 1422692.156877633 4188837.881094289 72.46632654472523 70, 1422692.1625749937 4188837.837818536 72.46632654472523 71, 1422692.1644789441 4188837.794210903 72.46632654472523 72))"),
		},
		{
			basename:           "poly",
			hasDBF:             true,
			hasPRJ:             true,
			hasSHX:             true,
			expectedShapeType:  ShapeTypePolygon,
			expectedBounds:     geom.NewBounds(geom.XY).Set(478315.531250, 4762880.5, 481645.312500, 4765610.5),
			expectedNumRecords: 10,
			expectedGeom0:      newGeomFromWKT(t, "POLYGON ((479819.84375 4765180.5,479690.1875 4765259.5,479647.0 4765369.5,479730.375 4765400.5,480039.03125 4765539.5,480035.34375 4765558.5,480159.78125 4765610.5,480202.28125 4765482.0,480365.0 4765015.5,480389.6875 4764950.0,480133.96875 4764856.5,480080.28125 4764979.5,480082.96875 4765049.5,480088.8125 4765139.5,480059.90625 4765239.5,480019.71875 4765319.5,479980.21875 4765409.5,479909.875 4765370.0,479859.875 4765270.0,479819.84375 4765180.5))"),
			expectedDBFRecord0: []any{215229.266, 168, "35043411"},
		},
	} {
		t.Run(tc.basename, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skip(tc.skipReason)
			}

			shapefile, err := ReadFS(os.DirFS("testdata"), tc.basename, nil)
			if tc.expectedErr != "" {
				require.Error(t, err, tc.expectedErr)
			}
			require.NoError(t, err)

			assert.Equal(t, tc.expectedShapeType, shapefile.SHP.ShapeType)
			assert.Equal(t, tc.expectedBounds, shapefile.SHP.Bounds)
			assert.Equal(t, tc.expectedNumRecords, shapefile.NumRecords())
			assert.Equal(t, tc.expectedGeom0, shapefile.SHP.Records[0].Geom)

			if tc.hasDBF {
				assert.Len(t, shapefile.DBF.Records, tc.expectedNumRecords)
				assert.Equal(t, tc.expectedDBFRecord0, shapefile.DBF.Records[0])
			} else {
				assert.Nil(t, shapefile.DBF)
			}

			if tc.hasPRJ {
				assert.NotNil(t, shapefile.PRJ)
			} else {
				assert.Nil(t, shapefile.PRJ)
			}

			if tc.hasSHX {
				assert.Equal(t, tc.expectedShapeType, shapefile.SHX.ShapeType)
				assert.Equal(t, tc.expectedBounds, shapefile.SHX.Bounds)
				assert.Len(t, shapefile.SHX.Records, tc.expectedNumRecords)
			} else {
				assert.Nil(t, shapefile.SHX)
			}
		})
	}
}

func TestReadFSAndZipFile(t *testing.T) {
	for _, tc := range []struct {
		filename                 string
		basename                 string
		expectedShapeType        ShapeType
		expectedBounds           *geom.Bounds
		expectedRecordsLen       int
		expectedDBFRecord0Fields map[string]any
		expectedSHPRecord0       *SHPRecord
	}{
		{
			filename:          "testdata/110m-admin-0-countries.zip",
			basename:          "ne_110m_admin_0_countries",
			expectedShapeType: ShapeTypePolygon,
			expectedBounds: geom.NewBounds(geom.XY).Set(
				-179.99999999999997, -90.00000000000003,
				180.00000000000014, 83.64513000000001,
			),
			expectedRecordsLen: 177,
			expectedDBFRecord0Fields: map[string]any{
				"ABBREV":     "Afg.",
				"ADM0_A3":    "AFG",
				"ADM0_DIF":   0.,
				"ADMIN":      "Afghanistan",
				"FIPS_10_":   0.,
				"FeatureCla": "Admin-0 countries",
				"GDP_MD_EST": 22270.,
				"GEOUNIT":    "Afghanistan",
				"GEOU_DIF":   0.,
				"GU_A3":      "AFG",
				"ISO_A2":     "AF",
				"ISO_A3":     "AFG",
				"ISO_N3":     4.,
				"LEVEL":      2.,
				"LabelRank":  1,
				"MAP_COLOR":  7.,
				"NAME":       "Afghanistan",
				"NAME_FORMA": "Islamic State of Afghanistan",
				"NAME_SORT":  "Afghanistan",
				"POP_EST":    28400000.,
				"POSTAL":     "AF",
				"SOVEREIGNT": "Afghanistan",
				"SOV_A3":     "AFG",
				"SUBUNIT":    "Afghanistan",
				"SU_A3":      "AFG",
				"SU_DIF":     0.,
				"ScaleRank":  1,
				"TERR_":      "",
				"TYPE":       "Sovereign country",
			},
			expectedSHPRecord0: &SHPRecord{
				Number:        1,
				ContentLength: 1152,
				ShapeType:     ShapeTypePolygon,
			},
		},
		{
			filename:          "testdata/Luftfahrthindernisse.zip",
			basename:          "Luftfahrthindernisse",
			expectedShapeType: ShapeTypePoint,
			expectedBounds: geom.NewBounds(geom.XY).Set(
				13.580271133050555, 46.621281718756464,
				16.12994444409849, 47.78517335054476,
			),
			expectedRecordsLen: 1097,
			expectedDBFRecord0Fields: map[string]any{
				"Art":        "Windkraftanlage",
				"Befeuert":   "N",
				"Betreiber":  "Viktor Kaplan Mürz GmbH",
				"GZ":         "FA18E-88-1082/2002-18",
				"Hoehe_Fp":   1580.,
				"Hoehe_Obj":  100.,
				"LFH_ID":     2,
				"Name":       "Windkraftanlage Windpark Moschkogel WKA 04",
				"OBJECTID":   191,
				"POINT_X":    15.74447664,
				"POINT_Y":    47.56136608,
				"Protnr":     17829,
				"Tagkennzg":  "N",
				"WGS_Breite": "47 33 41,0",
				"WGS_Laenge": "15 44 40,0",
				"changeDate": "20210222130000",
				"changeUser": "",
				"createDate": "20210222130000",
				"createUser": "",
			},
			expectedSHPRecord0: &SHPRecord{
				Number:        1,
				ContentLength: 20,
				ShapeType:     ShapeTypePoint,
				Geom:          newGeomFromWKT(t, "POINT (15.744476635247011 47.56136608020768)"),
			},
		},
		{
			filename:          "testdata/SZ.exe",
			basename:          "sz",
			expectedShapeType: ShapeTypePolygon,
			expectedBounds: geom.NewBounds(geom.XY).Set(
				5.9661102294921875, 45.829437255859375,
				10.488912582397461, 47.806938171386720,
			),
			expectedRecordsLen: 26,
			expectedDBFRecord0Fields: map[string]any{
				"ADMIN_NAME": "Aargau",
				"CNTRY_NAME": "Switzerland",
				"COLOR_MAP":  "6",
				"CONTINENT":  "Europe",
				"FIPS_ADMIN": "SZ01",
				"FIPS_CNTRY": "SZ",
				"GMI_ADMIN":  "CHE-AAR",
				"GMI_CNTRY":  "CHE",
				"POP_ADMIN":  524648,
				"REGION":     "Western Europe",
				"SQKM_ADMIN": 1441.17,
				"SQMI_ADMIN": 556.436,
				"TYPE_ENG":   "Canton",
				"TYPE_LOC":   "Canton(French), Cantone(Italian), Kanton(German)",
			},
			expectedSHPRecord0: &SHPRecord{
				Number:        1,
				ContentLength: 1248,
				ShapeType:     ShapeTypePolygon,
			},
		},
	} {
		t.Run(tc.filename, func(t *testing.T) {
			testShapefile := func(t *testing.T, shapefile *Shapefile) {
				t.Helper()
				assert.Equal(t, tc.expectedShapeType, shapefile.SHP.ShapeType)
				assert.Equal(t, tc.expectedBounds, shapefile.SHP.Bounds)

				assert.Len(t, shapefile.DBF.Records, tc.expectedRecordsLen)
				if tc.expectedDBFRecord0Fields != nil {
					fields, geom := shapefile.Record(0)
					assert.Equal(t, tc.expectedDBFRecord0Fields, fields)
					if tc.expectedSHPRecord0.Geom != nil {
						assert.Equal(t, tc.expectedSHPRecord0.Geom, geom)
					}
				}

				assert.Len(t, shapefile.SHP.Records, tc.expectedRecordsLen)
				if tc.expectedSHPRecord0 != nil {
					shpRecord0 := shapefile.SHP.Records[0]
					assert.Equal(t, tc.expectedSHPRecord0.Number, shpRecord0.Number)
					assert.Equal(t, tc.expectedSHPRecord0.ContentLength, shpRecord0.ContentLength)
					assert.Equal(t, tc.expectedSHPRecord0.ShapeType, shpRecord0.ShapeType)
					if tc.expectedSHPRecord0.Geom != nil {
						assert.Equal(t, tc.expectedSHPRecord0.Geom, shpRecord0.Geom)
					}
				}

				assert.Len(t, shapefile.SHX.Records, tc.expectedRecordsLen)
			}

			t.Run("ReadFS", func(t *testing.T) {
				file, err := os.Open(tc.filename)
				require.NoError(t, err)
				defer file.Close()

				fileInfo, err := file.Stat()
				require.NoError(t, err)

				zipReader, err := zip.NewReader(file, fileInfo.Size())
				require.NoError(t, err)

				shapefile, err := ReadFS(zipReader, tc.basename, nil)
				require.NoError(t, err)

				testShapefile(t, shapefile)
			})

			t.Run("ReadZipFile", func(t *testing.T) {
				shapefile, err := ReadZipFile(tc.filename, nil)
				require.NoError(t, err)
				testShapefile(t, shapefile)
			})
		})
	}
}

func addFuzzDataFromFS(f *testing.F, fsys fs.FS, root, ext string) error {
	f.Helper()
	return fs.WalkDir(fsys, root, func(path string, dirEntry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch filepath.Ext(path) {
		case ext:
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			f.Add(data)
		case ".exe", ".zip":
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			fileInfo, err := file.Stat()
			if err != nil {
				return err
			}

			zipReader, err := zip.NewReader(file, fileInfo.Size())
			if err != nil {
				return err
			}

			for _, zipFile := range zipReader.File {
				if filepath.Ext(zipFile.Name) != ext {
					continue
				}
				readCloser, err := zipFile.Open()
				if err != nil {
					return err
				}
				data, err := io.ReadAll(readCloser)
				readCloser.Close()
				if err != nil {
					return err
				}
				f.Add(data)
			}
		}
		return nil
	})
}

func newGeomFromWKT(t *testing.T, wktStr string) geom.T {
	t.Helper()
	g, err := wkt.Unmarshal(wktStr)
	require.NoError(t, err)
	return g
}
