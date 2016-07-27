package dmg

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"arg"
	"config"
	"process"
)

const (
	croppedPixelsMarker = ".crop.pixels"
	croppedLabelsMarker = ".crop.labels"
	croppedResultMarker = ".crop.result"
	finalResultMarker   = ".final"
)

// sectionJobInfo DMG section job info
type sectionJobInfo struct {
	dmgProcessInfo process.Info
	sectionArgs    *arg.Args
	resources      config.Config
}

// JobStdout job's standard output
func (sj sectionJobInfo) JobStdout() (io.ReadCloser, error) {
	return os.Stdout, nil
}

// JobStderr job's standard error
func (sj sectionJobInfo) JobStderr() (io.ReadCloser, error) {
	return os.Stderr, nil
}

// WaitForTermination wait for job's completion
func (sj sectionJobInfo) WaitForTermination() error {
	var sectionHelper SectionHelper
	if err := sectionHelper.CreateSectionJobResults(sj.sectionArgs, sj.resources); err != nil {
		return err
	}
	return nil
}

// SectionProcessor - section processor
type SectionProcessor struct {
	process.JobWatcher
	ImageProcessor   process.Processor
	Resources        config.Config
	DMGProcessorType string
}

// Run the given job
func (sp SectionProcessor) Run(j process.Job) error {
	ji, err := sp.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return sp.Wait(ji)
}

// Start launches the server
func (sp SectionProcessor) Start(j process.Job) (process.Info, error) {
	var sectionHelper SectionHelper
	sectionArgs, err := sectionHelper.PrepareSectionJobArgs(&j.JArgs, sp.Resources)
	if err != nil {
		return nil, err
	}
	sj := process.Job{
		Executable: sp.Resources.GetStringProperty("dmgexec"),
		Name:       j.Name,
		JArgs:      *sectionArgs,
		CmdlineBuilder: SectionJobCmdlineBuilder{
			Operation:            "dmgImage",
			DMGProcessorType:     sp.DMGProcessorType,
			SectionProcessorType: "local",
		},
	}
	dmgProcessInfo, err := sp.ImageProcessor.Start(sj)
	if err != nil {
		return nil, err
	}
	return sectionJobInfo{
		dmgProcessInfo: dmgProcessInfo,
		sectionArgs:    sectionArgs,
		resources:      sp.Resources,
	}, nil
}

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

// SectionJobCmdlineBuilder - command line builder for a section job
type SectionJobCmdlineBuilder struct {
	ClusterAccountID     string
	SessionName          string
	JobName              string
	Operation            string
	DMGProcessorType     string
	SectionProcessorType string
}

// GetCmdlineArgs section command line builder method
func (sclb SectionJobCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var dmgAttrs Attrs

	if err = dmgAttrs.extractDmgAttrs(&a); err != nil {
		return cmdargs, err
	}
	cmdargs = arg.AddArgs(cmdargs, "-dmgProcessor", sclb.DMGProcessorType, "-sectionProcessor", sclb.SectionProcessorType)
	if sclb.ClusterAccountID != "" {
		cmdargs = arg.AddArgs(cmdargs, "-A", sclb.ClusterAccountID)
	}
	if sclb.SessionName != "" {
		cmdargs = arg.AddArgs(cmdargs, "-sessionName", sclb.SessionName)
	}
	if sclb.JobName != "" {
		cmdargs = arg.AddArgs(cmdargs, "-jobName", sclb.JobName)
	}
	cmdargs = arg.AddArgs(cmdargs, sclb.Operation)
	if dmgAttrs.serverPort > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-serverPort", strconv.FormatInt(int64(dmgAttrs.serverPort), 10))
	}
	cmdargs = arg.AddArgs(cmdargs, "-config", dmgAttrs.Configs.String())
	if dmgAttrs.sourcePixels != "" && dmgAttrs.sourceLabels != "" {
		cmdargs = arg.AddArgs(cmdargs,
			"-pixels", dmgAttrs.sourcePixels,
			"-labels", dmgAttrs.sourceLabels)
	}
	if dmgAttrs.destImg != "" {
		cmdargs = arg.AddArgs(cmdargs, "-out", dmgAttrs.destImg)
	}
	if len(dmgAttrs.sourcePixelsList) > 0 && len(dmgAttrs.sourceLabelsList) > 0 {
		cmdargs = arg.AddArgs(cmdargs,
			"-pixelsList", dmgAttrs.sourcePixelsList.String(),
			"-labelsList", dmgAttrs.sourceLabelsList.String())
	}
	if len(dmgAttrs.destImgList) > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-outList", dmgAttrs.destImgList.String())
	}
	cmdargs = arg.AddArgs(cmdargs, "-temp", dmgAttrs.scratchDir, "-targetDir", dmgAttrs.targetDir)
	cmdargs = arg.AddArgs(cmdargs,
		"-threads", strconv.FormatInt(int64(dmgAttrs.nThreads), 10),
		"-sections", strconv.FormatInt(int64(dmgAttrs.nSections), 10),
		"-iters", strconv.FormatInt(int64(dmgAttrs.iterations), 10),
		"-vCycles", strconv.FormatInt(int64(dmgAttrs.vCycles), 10),
		"-iWeight", strconv.FormatFloat(dmgAttrs.iWeight, 'g', -1, 64),
		"-gWeight", strconv.FormatFloat(dmgAttrs.gWeight, 'g', -1, 64),
		"-gScale", strconv.FormatFloat(dmgAttrs.gScale, 'g', -1, 64))
	cmdargs = arg.AddArgs(cmdargs, "-tileExt", dmgAttrs.tileExt)
	cmdargs = arg.AddArgs(cmdargs, "-tileWidth", strconv.FormatInt(int64(dmgAttrs.tileWidth), 10))
	cmdargs = arg.AddArgs(cmdargs, "-tileHeight", strconv.FormatInt(int64(dmgAttrs.tileHeight), 10))

	if dmgAttrs.verbose {
		cmdargs = arg.AddArgs(cmdargs, "-verbose")
	}
	if dmgAttrs.gray {
		cmdargs = arg.AddArgs(cmdargs, "-gray")
	}
	if dmgAttrs.deramp {
		cmdargs = arg.AddArgs(cmdargs, "-deramp")
	}
	return cmdargs, nil

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
	width = width + nSections - width%nSections
	var minCol, maxCol int
	if pixelsGrid.maxCol-width > 0 {
		maxCol = pixelsGrid.maxCol
		minCol = maxCol - width
	} else {
		minCol = 0
		maxCol = minCol + width
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

	var pixelsList, labelsList, outputList []string

	for i, pg := range pixelSections {
		pn := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s.%d.iGrid", pixelsName, croppedPixelsMarker, i))
		if err := writeIGrid(pn, pg, emptyPixels); err != nil {
			return nil, err
		}
		on := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s.%d.iGrid", pixelsName, croppedResultMarker, i))
		pixelsList = append(pixelsList, pn)
		outputList = append(outputList, on)
	}

	for i, lg := range labelSections {
		ln := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s%s.%d.iGrid", labelsName, croppedLabelsMarker, i))
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

	sectionArgs.UpdateStringArg("pixels", "")
	sectionArgs.UpdateStringArg("labels", "")
	sectionArgs.UpdateStringArg("out", "")
	sectionArgs.UpdateStringListArg("pixelsList", pixelsList)
	sectionArgs.UpdateStringListArg("labelsList", labelsList)
	sectionArgs.UpdateStringListArg("outList", outputList)

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
	// read the coordinates file
	coordFile := filepath.Join(dmgAttrs.targetDir, fmt.Sprintf("%s", dmgAttrs.coordFile))
	coordInfo, err := readCoordFile(coordFile)
	if err != nil {
		return err
	}
	emptyPixels := resources.GetStringProperty("emptyPixelsTile")
	// read the result grids
	var resultDir, resultBaseName string
	var gridResults []*iGrid
	for i, rfn := range dmgAttrs.destImgList {
		gr, err := readIGrid(rfn)
		if err != nil {
			return err
		}
		gridResults = append(gridResults, gr)
		rd := filepath.Dir(rfn) // result directory
		if resultDir == "" {
			resultDir = rd
		} else if resultDir != rd {
			log.Printf("Result directory inconsistency found; '%s' does not appear to be in the '%s' directory",
				rfn, resultDir)
		}
		rbn := strings.Replace(filepath.Base(rfn), fmt.Sprintf("%s.%d.iGrid", croppedResultMarker, i), "", -1) // basename
		if resultBaseName == "" {
			resultBaseName = rbn
		} else if resultBaseName != rbn {
			log.Printf("Result basename inconsistency found; '%s' does not appear to have the basename '%s'",
				rfn, resultBaseName)
		}
	}
	// merge the results
	mergedResultGrid := mergeColumnGrids(gridResults...)
	mergedResultGridFile := filepath.Join(resultDir, fmt.Sprintf("%s%s.iGrid", resultBaseName, croppedResultMarker))
	if err := writeIGrid(mergedResultGridFile, mergedResultGrid, emptyPixels); err != nil {
		return err
	}
	// uncrop the result
	finalGrid := uncrop(mergedResultGrid, coordInfo.MinCol, coordInfo.MinRow, coordInfo.NCols, coordInfo.NRows)
	// rename the tile image files to have the right col/row
	for row := 0; row < finalGrid.nRows; row++ {
		for col := 0; col < finalGrid.nCols; col++ {
			oldTileName := finalGrid.getTile(col, row)
			if oldTileName == "" {
				continue
			}
			oldTileExt := filepath.Ext(oldTileName)
			newTileName := filepath.Join(resultDir, fmt.Sprintf("%s.%d.%d%s", resultBaseName, row, col, oldTileExt))
			fmt.Printf("Rename %s -> %s\n", oldTileName, newTileName)
			if renameErr := os.Rename(oldTileName, newTileName); renameErr != nil {
				log.Printf("Error trying to rename %s -> %s: %v", oldTileName, newTileName, renameErr)
			}
			finalGrid.setTile(col, row, newTileName)
		}
	}
	finalResultGridFile := filepath.Join(resultDir, fmt.Sprintf("%s%s.iGrid", resultBaseName, finalResultMarker))
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
