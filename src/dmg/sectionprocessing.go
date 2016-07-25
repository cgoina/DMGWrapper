package dmg

import (
	"fmt"

	"process"
)

// sectionSplitter splits a section into multiple bands.
type sectionSplitter struct {
}

// SplitJob splits the job into multiple parallelizable jobs
func (s sectionSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
	var err error
	var dmgAttrs Attrs

	args := &j.JArgs
	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return err
	}
	pixelsGrid, err := readIGrid(dmgAttrs.sourcePixels)
	if err != nil {
		return err
	}
	labelsGrid, err := readIGrid(dmgAttrs.sourceLabels)
	if err != nil {
		return err
	}
	if pixelsGrid.nCols != labelsGrid.nCols || pixelsGrid.nRows != labelsGrid.nRows {
		return fmt.Errorf("Pixels and labels have different dimensions: (%d, %d) vs (%d, %d)",
			pixelsGrid.nCols, pixelsGrid.nRows, labelsGrid.nCols, labelsGrid.nRows)
	}
	if pixelsGrid.minCol != labelsGrid.minCol ||
		pixelsGrid.minRow != labelsGrid.minRow ||
		pixelsGrid.maxCol != labelsGrid.maxCol ||
		pixelsGrid.maxRow != labelsGrid.maxRow {
		return fmt.Errorf("Pixels and labels have different boundaries: (%d, %d, %d, %d) vs (%d, %d, %d, %d)",
			pixelsGrid.minCol, pixelsGrid.minRow, pixelsGrid.maxCol, pixelsGrid.maxRow,
			labelsGrid.minCol, labelsGrid.minRow, labelsGrid.maxCol, labelsGrid.maxRow)
	}
	
	// TODO !!!!
	return nil
}

// NewSectionSplitter creates a new section splitter
func NewSectionSplitter() process.Splitter {
	return sectionSplitter{}
}

func readIGrid(name string) (*iGrid, error) {
	gr, err := open(name)
	defer func() {
		gr.close()
	}()
	if err != nil {
		return nil, err
	}
	return gr.read()
}
