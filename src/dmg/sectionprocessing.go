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

const (
	cropPixelsExt    = ".crop.pixels"
	cropLabelsExt    = ".crop.labels"
	croppedResultExt = ".croppedResult.iGrid"
)

// CoordInfo structure that holds cropping info for the original images
type CoordInfo struct {
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

// SectionHelper is an object that can be used for preparing the job arguments for a section
// as well as for creating the final results
type SectionHelper struct {
}

// PrepareSectionJobArgs splits the grid into multiple bands and creates the corresponding job
func (s SectionHelper) PrepareSectionJobArgs(args *arg.Args, resources config.Config) (*arg.Args, error) {
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return nil, err
	}

	err = os.MkdirAll(dmgAttrs.targetDir, 0775)
	if err != nil {
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

	// split the pixels and labels grids into the specified number of
	pixelsName := strings.TrimRight(filepath.Base(dmgAttrs.sourcePixels), ".iGrid")
	labelsName := strings.TrimRight(filepath.Base(dmgAttrs.sourceLabels), ".iGrid")
	nSections := dmgAttrs.nSections

	width := pixelsGrid.maxCol - pixelsGrid.minCol
	if width < nSections {
		width = nSections
	} else if width%nSections != 0 {
		width = width + nSections - width%nSections
	}

	maxCol := pixelsGrid.maxCol
	minCol := maxCol - width
	if minCol < 0 {
		maxCol = maxCol - minCol
		minCol = 0
	}
	fmt.Printf("Image grid bounds are: (%d, %d), (%d, %d)\n", minCol, pixelsGrid.minRow, maxCol, pixelsGrid.maxRow)
	coordInfo := CoordInfo{
		InputPixelsName: dmgAttrs.sourcePixels,
		InputLabelsName: dmgAttrs.sourceLabels,
		MinCol:          minCol,
		MaxCol:          maxCol,
		NCols:           pixelsGrid.nCols,
		MinRow:          pixelsGrid.minRow,
		MaxRow:          pixelsGrid.maxRow,
		NRows:           pixelsGrid.nRows,
	}

	emptyPixels := resources.GetStringProperty("emptyPixelsTile")
	emptyLabels := resources.GetStringProperty("emptyLabelsTile")

	// crop the pixels iGrid
	croppedPixelsGrid := crop(pixelsGrid, coordInfo.MinCol, coordInfo.MinRow, coordInfo.MaxCol, coordInfo.MaxRow)
	// save the cropped pixels iGrid
	cpn := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s.crop.pixels.iGrid", pixelsName))
	if err := writeIGrid(cpn, croppedPixelsGrid, emptyPixels); err != nil {
		return nil, err
	}
	// split the cropped pixels iGrid
	pixelSections := splitGrid(croppedPixelsGrid, nSections)

	// crop the labels iGrid
	croppedLabelsGrid := crop(labelsGrid, coordInfo.MinCol, coordInfo.MinRow, coordInfo.MaxCol, coordInfo.MaxRow)
	// save the cropped labels iGrid
	cln := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s.crop.labels.iGrid", labelsName))
	if err := writeIGrid(cln, croppedLabelsGrid, emptyLabels); err != nil {
		return nil, err
	}
	// split the cropped labels iGrid
	labelSections := splitGrid(croppedLabelsGrid, nSections)

	var pixelsList, labelsList []string

	for i, pg := range pixelSections {
		pn := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s.%d.iGrid", pixelsName, cropPixelsExt, i))
		if err := writeIGrid(pn, pg, emptyPixels); err != nil {
			return nil, err
		}
		pixelsList = append(pixelsList, pn)
	}

	for i, lg := range labelSections {
		ln := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s.%d.iGrid", labelsName, cropLabelsExt, i))
		if err := writeIGrid(ln, lg, emptyLabels); err != nil {
			return nil, err
		}
		labelsList = append(labelsList, ln)
	}

	coordFile := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s", dmgAttrs.coordFile))
	coordJSON, err := json.Marshal(coordInfo)
	if err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(coordFile, coordJSON, 0664); err != nil {
		return nil, err
	}

	outputFileName := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s", pixelsName, croppedResultExt))

	sectionArgs.UpdateStringArg("pixels", "")
	sectionArgs.UpdateStringArg("labels", "")
	sectionArgs.UpdateStringListArg("pixelsList", pixelsList)
	sectionArgs.UpdateStringListArg("labelsList", labelsList)
	sectionArgs.UpdateStringArg("out", outputFileName)

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

func splitGrid(g *iGrid, nSections int) []*iGrid {
	sections := make([]*iGrid, nSections)

	tilePerSection := g.nCols / nSections

	for section := 0; section < nSections; section++ {
		sections[section] = crop(g, section*tilePerSection, 0, (section+1)*tilePerSection, g.nRows)
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

// CreateSectionJobResults is responsible with merging and creating the final result
func (s SectionHelper) CreateSectionJobResults(args *arg.Args, resources config.Config) error {
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(args); err != nil {
		return err
	}

	coordFile := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s", dmgAttrs.coordFile))
	coordInfo, err := readCoordFile(coordFile)
	if err != nil {
		return err
	}

	resultGridFile := dmgAttrs.destImg
	resultGrid, err := readIGrid(resultGridFile)
	if err != nil {
		return err
	}

	finalGrid := uncrop(resultGrid, coordInfo.MinCol, coordInfo.MinRow, coordInfo.NCols, coordInfo.NRows)

	resultDir := filepath.Dir(resultGridFile)
	resultBaseName := strings.Replace(filepath.Base(resultGridFile), croppedResultExt, "", -1)

	// rename the tile image files to have the right col/row
	for row := 0; row < finalGrid.nRows; row++ {
		for col := 0; col < finalGrid.nCols; col++ {
			oldTileName := finalGrid.getTile(col, row)
			if oldTileName == "" {
				continue
			}
			oldTileExt := filepath.Ext(oldTileName)
			newTileName := filepath.Join(resultDir, fmt.Sprintf("%s.%d.%d%s", resultBaseName, col, row, oldTileExt))
			if renameErr := os.Rename(oldTileName, newTileName); renameErr != nil {
				log.Printf("Error trying to rename %s -> %s: %v", oldTileName, newTileName, renameErr)
			}
			finalGrid.setTile(col, row, newTileName)
		}
	}
	finalResultGridFile := strings.Replace(resultGridFile, croppedResultExt, ".final.iGrid", -1)
	emptyPixels := resources.GetStringProperty("emptyPixelsTile")
	if err := writeIGrid(finalResultGridFile, finalGrid, emptyPixels); err != nil {
		return err
	}
	return nil
}

func readCoordFile(coordFile string) (*CoordInfo, error) {
	r, err := os.Open(coordFile)
	if err != nil {
		return nil, fmt.Errorf("Error opening coordinates file %s: %v", coordFile, err)
	}
	defer r.Close()
	coordDecoder := json.NewDecoder(r)

	coordInfo := &CoordInfo{}
	if err = coordDecoder.Decode(coordInfo); err != nil {
		return nil, fmt.Errorf("Error reading coordinates from %s as JSON: %v", coordFile, err)
	}
	return coordInfo, nil
}
