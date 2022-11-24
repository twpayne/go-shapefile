package shapefile

// FIXME add test with PolyLine geometries
// FIXME add test with *M geometries
// FIXME add test with *Z geometries

import (
	"archive/zip"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
)

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

				shapefile, err := ReadFS(zipReader, tc.basename)
				require.NoError(t, err)

				testShapefile(t, shapefile)
			})

			t.Run("ReadZipFile", func(t *testing.T) {
				shapefile, err := ReadZipFile(tc.filename)
				require.NoError(t, err)
				testShapefile(t, shapefile)
			})
		})
	}
}

func newGeomFromWKT(t *testing.T, wktStr string) geom.T {
	t.Helper()
	g, err := wkt.Unmarshal(wktStr)
	require.NoError(t, err)
	return g
}
