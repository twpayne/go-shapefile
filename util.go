package shapefile

import (
	"path/filepath"
	"strings"
)

func isMacOSXPath(p string) bool {
	dir, _ := filepath.Split(p)
	for elem := range strings.SplitSeq(dir, string(filepath.Separator)) {
		if elem == "__MACOSX" {
			return true
		}
	}
	return false
}
