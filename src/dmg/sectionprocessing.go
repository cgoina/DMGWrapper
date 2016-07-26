package dmg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"arg"
	"config"
)

// CropInfo structure that holds cropping info for the original images
type CropInfo struct {
	InputPixelsName string `json:"pixels_in"`
	InputLabelsName string `json:"labels_in"`
	MinCol          int    `json:"offset_x_tiles"`
	MaxCol          int    `json:"max_x_tiles"`
	NCols           int    `json:"original_x_tiles"`
	MinRow          int    `json:"offset_y_tiles"`
	MaxRow          int    `json:"max_y_tiles"`
	NRows           int    `json:"original_y_tiles"`
	TileWidth       int    `json:"tile_size_x"`
	TileHeight      int    `json:"tile_size_y"`
}

// SectionPreparer prepares the section data
type SectionPreparer struct {
}

// CreateSectionJobArgs splits the grid into multiple bands and creates the corresponding job
func (s SectionPreparer) CreateSectionJobArgs(args *arg.Args, resources config.Config) (*arg.Args, error) {
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return nil, err
	}
	sectionArgs := args.Clone()
	pixelsGrid, err := readIGrid(dmgAttrs.sourcePixels)
	if err != nil {
		return nil, err
	}
	labelsGrid, err := readIGrid(dmgAttrs.sourceLabels)
	if err != nil {
		return nil, err
	}
	if pixelsGrid.nCols != labelsGrid.nCols || pixelsGrid.nRows != labelsGrid.nRows {
		return nil, fmt.Errorf("Pixels and labels have different dimensions: (%d, %d) vs (%d, %d)",
			pixelsGrid.nCols, pixelsGrid.nRows, labelsGrid.nCols, labelsGrid.nRows)
	}
	if pixelsGrid.minCol != labelsGrid.minCol ||
		pixelsGrid.minRow != labelsGrid.minRow ||
		pixelsGrid.maxCol != labelsGrid.maxCol ||
		pixelsGrid.maxRow != labelsGrid.maxRow {
		return nil, fmt.Errorf("Pixels and labels have different boundaries: (%d, %d, %d, %d) vs (%d, %d, %d, %d)",
			pixelsGrid.minCol, pixelsGrid.minRow, pixelsGrid.maxCol, pixelsGrid.maxRow,
			labelsGrid.minCol, labelsGrid.minRow, labelsGrid.maxCol, labelsGrid.maxRow)
	}
	if len(pixelsGrid.tiles) != len(labelsGrid.tiles) {
		return nil, fmt.Errorf("The number of non empty pixel and label tiles must be equal: %d vs %d",
			len(pixelsGrid.tiles), len(labelsGrid.tiles))
	}

	pixelsName := strings.TrimRight(filepath.Base(dmgAttrs.sourcePixels), ".iGrid")
	labelsName := strings.TrimRight(filepath.Base(dmgAttrs.sourceLabels), ".iGrid")
	nSections := dmgAttrs.nSections

	cropInfo := CropInfo{
		InputPixelsName: dmgAttrs.sourcePixels,
		InputLabelsName: dmgAttrs.sourceLabels,
		MinCol:          pixelsGrid.minCol,
		MaxCol:          pixelsGrid.maxCol + (pixelsGrid.maxCol-pixelsGrid.minCol)%nSections,
		NCols:           pixelsGrid.nCols,
		MinRow:          pixelsGrid.minRow,
		MaxRow:          pixelsGrid.maxRow,
		NRows:           pixelsGrid.nRows,
	}
	// split the pixels and labels grids into the specified number of
	pixelSections := splitGrid(pixelsGrid, cropInfo, nSections)
	labelSections := splitGrid(labelsGrid, cropInfo, nSections)

	var pixelsList, labelsList []string

	emptyPixels := resources.GetStringProperty("EMPTY_PIXELS_TILE")
	for i, pg := range pixelSections {
		pn := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s.crop.pixels.%d.iGrid", pixelsName, i))
		if err := writeIGrid(pn, pg, emptyPixels); err != nil {
			return nil, err
		}
		pixelsList = append(pixelsList, pn)
	}

	emptyLabels := resources.GetStringProperty("EMPTY_LABELS_TILE")
	for i, lg := range labelSections {
		ln := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s.crop.labels.%d.iGrid", labelsName, i))
		if err := writeIGrid(ln, lg, emptyLabels); err != nil {
			return nil, err
		}
		labelsList = append(labelsList, ln)
	}

	coordFile := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s", dmgAttrs.coordFile))
	coordJson, err := json.Marshal(cropInfo)
	if err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(coordFile, coordJson, 0664); err != nil {
		return nil, err
	}

	sectionArgs.UpdateStringArg("pixels", "")
	sectionArgs.UpdateStringArg("labels", "")
	sectionArgs.UpdateStringListArg("pixelsList", pixelsList)
	sectionArgs.UpdateStringListArg("labelsList", labelsList)

	return &sectionArgs, nil
}

func readIGrid(filename string) (*iGrid, error) {
	log.Printf("Read iGrid %s", filename)
	gr, err := open(filename)
	defer func() {
		gr.close()
	}()
	if err != nil {
		return nil, err
	}
	return gr.read()
}

func splitGrid(g *iGrid, ci CropInfo, nSections int) []*iGrid {
	sections := make([]*iGrid, nSections)

	croppedGrid := crop(g, ci.MinCol, ci.MinRow, ci.MaxCol, ci.MaxRow)
	tilePerSection := croppedGrid.nCols / nSections

	for section := 0; section < nSections; section++ {
		sections[section] = crop(croppedGrid, section*tilePerSection, 0, (section+1)*tilePerSection, croppedGrid.nRows)
	}
	return sections
}

func writeIGrid(filename string, g *iGrid, emptyTileName string) error {
	log.Printf("Write iGrid %s", filename)
	gw, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer func() {
		gw.Close()
	}()

	return write(gw, g, emptyTileName)
}
