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
	coord iGridTileCoord
	name  string
}

// iGrid contains grid dimensions and the the list of non empty tiles indexed by their coordinates.
type iGrid struct {
	nCols, nRows   int
	minCol, minRow int
	maxCol, maxRow int
	tiles          map[iGridTileCoord]*iGridTile
}

func (g iGrid) getTile(col, row int) string {
	tc := iGridTileCoord{col, row}
	t := g.tiles[tc]
	if t == nil {
		return ""
	}
	return t.name
}

func (g *iGrid) setTile(col, row int, name string) {
	tc := iGridTileCoord{col, row}
	g.tiles[tc] = &iGridTile{tc, name}
}

type iGridReader struct {
	name   string
	reader io.ReadCloser
}

func open(name string) (*iGridReader, error) {
	var err error
	var reader io.ReadCloser

	if reader, err = os.Open(name); err != nil {
		return nil, fmt.Errorf("Error opening %s: %v", name, err)
	}
	return &iGridReader{
		name:   name,
		reader: reader,
	}, nil
}

func (gr *iGridReader) read() (*iGrid, error) {
	var col, row int
	var err error
	g := &iGrid{
		tiles: make(map[iGridTileCoord]*iGridTile),
	}
	scanner := bufio.NewScanner(gr.reader)

	if g.nCols, err = gr.readTileDim(scanner, "Columns:"); err != nil {
		return g, err
	}
	if g.nRows, err = gr.readTileDim(scanner, "Rows:"); err != nil {
		return g, err
	}
	var minCol, minRow, maxCol, maxRow int = -1, -1, -1, -1
	for {
		line, done, err := gr.readLine(scanner)
		if err != nil {
			return g, err
		}
		if done {
			break
		}
		if !strings.Contains(line, "empty") {
			if minCol == -1 || col < minCol {
				minCol = col
			}
			if minRow == -1 || row < minRow {
				minRow = row
			}
			if maxCol == -1 || col >= maxCol {
				maxCol = col + 1
			}
			if maxRow == -1 || row >= maxRow {
				maxRow = row + 1
			}
			tile := &iGridTile{
				coord: iGridTileCoord{col, row},
				name:  line,
			}
			g.tiles[tile.coord] = tile
		}
		col++
		if col >= g.nCols {
			col = 0
			row++
		}
	}
	g.minCol = minCol
	g.minRow = minRow
	g.maxCol = maxCol
	g.maxRow = maxRow
	if err = scanner.Err(); err != nil {
		return g, fmt.Errorf("Error reading iGrid pixels %s: %v", gr.name, err)
	}
	return g, nil
}

func (gr *iGridReader) close() error {
	return gr.reader.Close()
}

func (gr *iGridReader) readLine(scanner *bufio.Scanner) (line string, done bool, err error) {
	success := scanner.Scan()
	if success {
		return scanner.Text(), false, nil
	}
	return scanner.Text(), true, scanner.Err()
}

func (gr *iGridReader) readTileDim(scanner *bufio.Scanner, dimPrefix string) (int, error) {
	dimline, done, err := gr.readLine(scanner)
	if err != nil {
		return 0, fmt.Errorf("Error reading %s from %s: %v", dimPrefix, gr.name, err)
	}
	if done {
		return 0, fmt.Errorf("Error unexpected EOF reading %s from %s", dimPrefix, gr.name)
	}
	dimline = strings.TrimSpace(strings.TrimLeft(dimline, dimPrefix))
	dim, err := strconv.Atoi(dimline)
	if err != nil {
		return 0, fmt.Errorf("Error converting to an int %s value from %s: %v", dimPrefix, gr.name, err)
	}
	if dim == 0 {
		return 0, fmt.Errorf("Error invalid %s value from %s", dimPrefix, gr.name)
	}
	return dim, nil
}

func crop(sg *iGrid, minCol, minRow, maxCol, maxRow int) *iGrid {
	tg := &iGrid{
		nCols: maxCol - minCol,
		nRows: maxRow - minRow,
		tiles: make(map[iGridTileCoord]*iGridTile),
	}
	for row := minRow; row < maxRow; row++ {
		for col := minCol; col < maxCol; col++ {
			tn := sg.getTile(col, row)
			if tn == "" {
				continue
			}
			tg.setTile(col-minCol, row-minRow, tn)
		}
	}
	return tg
}

func mergeSectionGrids(gs ...*iGrid) *iGrid {
	mg := &iGrid{
		tiles: make(map[iGridTileCoord]*iGridTile),
	}
	for _, g := range gs {
		for row := 0; row < g.nRows; row++ {
			for col := 0; col < g.nCols; col++ {
				tn := g.getTile(col, row)
				if tn == "" {
					continue
				}
				mg.setTile(mg.nCols+col, row, tn)
			}
		}
		mg.nCols += g.nCols
		if g.nRows > mg.nRows {
			mg.nRows = g.nRows
		}
	}
	return mg
}

func uncrop(sg *iGrid, minCol, minRow, maxCol, maxRow int) *iGrid {
	tg := &iGrid{
		nCols: maxCol,
		nRows: maxRow,
		tiles: make(map[iGridTileCoord]*iGridTile),
	}
	for row := 0; row < sg.nRows; row++ {
		for col := 0; col < sg.nCols; col++ {
			tn := sg.getTile(col, row)
			if tn == "" {
				continue
			}
			tg.setTile(minCol+col, minRow+row, tn)
		}
	}
	return tg
}

func write(w io.Writer, g *iGrid, emptyTileName string) error {
	if _, err := fmt.Fprintf(w, "Columns: %d\nRows: %d\n", g.nCols, g.nRows); err != nil {
		return err
	}
	for row := 0; row < g.nRows; row++ {
		for col := 0; col < g.nCols; col++ {
			tn := g.getTile(col, row)
			if tn == "" {
				tn = emptyTileName
			}
			if _, err := fmt.Fprintf(w, "%s\n", tn); err != nil {
				return err
			}
		}
	}
	return nil
}
