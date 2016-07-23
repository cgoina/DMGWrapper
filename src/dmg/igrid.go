package dmg

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type iGridTileCoord struct {
	col, row int
}

type iGridTile struct {
	coord  iGridTileCoord
	pixels string
	labels string
}

type iGrid struct {
	ncols, nrows int
	tiles        map[iGridTileCoord]*iGridTile
}

type iGridReader struct {
	pixelsName   string
	pixelsReader io.ReadCloser
	labelsName   string
	labelsReader io.ReadCloser
}

func open(pixels, labels string) (*iGridReader, error) {
	var err error
	var pixelsReader, labelsReader io.ReadCloser

	if pixelsReader, err = os.Open(pixels); err != nil {
		return nil, fmt.Errorf("Error opening %s: %v", pixels, err)
	}
	if labelsReader, err = os.Open(labels); err != nil {
		return nil, fmt.Errorf("Error opening %s: %v", labels, err)
	}
	return &iGridReader{
		pixelsName:   pixels,
		pixelsReader: pixelsReader,
		labelsName:   labels,
		labelsReader: labelsReader,
	}, nil
}

func (gr *iGridReader) read() (*iGrid, error) {
	var col, row int
	var err error
	g := &iGrid{
		tiles: make(map[iGridTileCoord]*iGridTile),
	}
	defer func() {
		gr.pixelsReader.Close()
		gr.labelsReader.Close()
	}()
	
	pixelsScanner := bufio.NewScanner(gr.pixelsReader)
	labelsScanner := bufio.NewScanner(gr.labelsReader)

	if g.ncols, err = gr.readTileDim(pixelsScanner, labelsScanner, "Columns:"); err != nil {
		return g, err
	}
	if g.nrows, err = gr.readTileDim(pixelsScanner, labelsScanner, "Rows:"); err != nil {
		return g, err
	}
	for {
		pline, lline, done, err := gr.readLine(pixelsScanner, labelsScanner)
		if err != nil {
			return g, err
		}
		if done {
			break
		}
		tile := &iGridTile{
			coord:  iGridTileCoord{col, row},
			pixels: pline,
			labels: lline,
		}
		g.tiles[tile.coord] = tile
		col++
		if col >= g.ncols {
			col = 0
			row++
		}
	}
	if err = pixelsScanner.Err(); err != nil {
		return g, fmt.Errorf("Error reading iGrid pixels %s: %v", gr.pixelsName, err)
	}
	if err = labelsScanner.Err(); err != nil {
		return g, fmt.Errorf("Error reading iGrid labels %s: %v", gr.labelsName, err)
	}
	return g, nil
}

func (gr *iGridReader) readLine(pixelsScanner, labelsScanner *bufio.Scanner) (pixelsLine, labelsLine string,
	done bool, err error) {
	pixelsDone := pixelsScanner.Scan()
	labelsDone := labelsScanner.Scan()
	if pixelsDone && labelsDone {
		return "", "", true, nil
	}
	if pixelsDone {
		return "", labelsScanner.Text(), true, fmt.Errorf("There are more labels than pixels in (%s, %s)",
			gr.pixelsName, gr.labelsName)
	}
	if labelsDone {
		return pixelsScanner.Text(), "", true, fmt.Errorf("There are more pixels than labels in (%s, %s)",
			gr.pixelsName, gr.labelsName)
	}
	return pixelsScanner.Text(), labelsScanner.Text(), false, nil
}

func (gr *iGridReader) readTileDim(pixelsScanner, labelsScanner *bufio.Scanner, dimPrefix string) (int, error) {
	pdimline, ldimline, done, err := gr.readLine(pixelsScanner, labelsScanner)
	if err != nil {
		return 0, fmt.Errorf("Error reading %s from (%s, %s): %v", dimPrefix,
			gr.pixelsName, gr.labelsName, err)
	}
	if done {
		return 0, fmt.Errorf("Error unexpected EOF reading %s from (%s, %s)", dimPrefix,
			gr.pixelsName, gr.labelsName)
	}
	pdimline = strings.TrimSpace(strings.TrimLeft(pdimline, dimPrefix))
	ldimline = strings.TrimSpace(strings.TrimLeft(ldimline, dimPrefix))
	pdim, err := strconv.Atoi(pdimline)
	if err != nil {
		return 0, fmt.Errorf("Error converting to an int %s pixels from (%s, %s): %v", dimPrefix,
			gr.pixelsName, gr.labelsName, err)
	}
	ldim, err := strconv.Atoi(ldimline)
	if err != nil {
		return 0, fmt.Errorf("Error converting to an int %s labels from (%s, %s): %v", dimPrefix,
			gr.pixelsName, gr.labelsName, err)
	}
	if pdim != ldim || pdim == 0 {
		return 0, fmt.Errorf("Error invalid %s value from (%s, %s) - (%d, %d)", dimPrefix,
			gr.pixelsName, gr.labelsName, pdim, ldim)
	}
	return pdim, nil
}
