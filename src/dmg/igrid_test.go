package dmg

import (
	"testing"
)

func TestLoadIGrid(t *testing.T) {
	testiGridFile := "testdata/1200.0.iGrid"
	iGrid, err := readIGrid(testiGridFile)
	if err != nil {
		t.Error("Unexpected error", err)
		return
	}
	if iGrid.nCols != 12 {
		t.Error("Expected 12 columns but got", iGrid.nCols)
	}
	if iGrid.nRows != 10 {
		t.Error("Expected 10 rows but got", iGrid.nRows)
	}
}
