package dmg

import (
	"os"
	"reflect"
	"testing"
)

const (
	testiGridFile = "testdata/1200.0.iGrid"
	emptyTileName = "/tier2/flyTEM/nobackup/rendered_boxes/FAFB00/v12_align_tps/8192x8192/empty.png"
)

func TestLoadIGrid(t *testing.T) {
	grid, err := readIGrid(testiGridFile)
	if err != nil {
		t.Error("Unexpected error", err)
		return
	}
	if grid.nCols != 20 {
		t.Error("Expected 12 columns but got", grid.nCols)
	}
	if grid.nRows != 12 {
		t.Error("Expected 10 rows but got", grid.nRows)
	}
	var expectedMinCol, expectedMinRow, expectedMaxCol, expectedMaxRow = 9, 2, 20, 12
	if grid.minCol != expectedMinCol ||
		grid.minRow != expectedMinRow ||
		grid.maxCol != expectedMaxCol ||
		grid.maxRow != expectedMaxRow {
		t.Error("Expected cropped coordinates", expectedMinCol, expectedMinRow, expectedMaxCol, expectedMaxRow,
			"but it got", grid.minCol, grid.minRow, grid.maxCol, grid.maxRow)
	}
}

func TestSplitAndMerge(t *testing.T) {
	const nSections = 4
	grid, err := readIGrid(testiGridFile)
	if err != nil {
		t.Error("Unexpected error", err)
		return
	}
	cropInfo := CropInfo{
		MinCol: grid.minCol,
		MaxCol: grid.maxCol + (grid.maxCol-grid.minCol)%nSections,
		NCols:  grid.nCols,
		MinRow: grid.minRow,
		MaxRow: grid.maxRow,
		NRows:  grid.nRows,
	}

	gridSections := splitGrid(grid, cropInfo, nSections)

	mergedGrid := mergeSectionGrids(gridSections...)

	uncroppedGrid := uncrop(mergedGrid, cropInfo.MinCol, cropInfo.MinRow, cropInfo.NCols, cropInfo.NRows)
	if uncroppedGrid.nCols != grid.nCols ||
		uncroppedGrid.nRows != grid.nRows ||
		!reflect.DeepEqual(uncroppedGrid.tiles, grid.tiles) {
		t.Error("Expected merged grid to be equal to original grid but it wasn't")
	}

	testOutput := "testdata/testOut.iGrid"
	err = writeIGrid(testOutput, uncroppedGrid, emptyTileName)
	if err != nil {
		t.Error("Unexpected error while writing the result grid", err)
	}
	defer os.Remove(testOutput)
}
