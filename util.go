package shapefile

import (
	"path/filepath"
	"strings"
)

func isMacOSXPath(p string) bool {
	dir, _ := filepath.Split(p)
	pathElements := strings.Split(dir, string(filepath.Separator))
	for _, elem := range pathElements {
		if elem == "__MACOSX" {
			return true
		}
	}
	return false
}
