package mipmaps

import (
	"flag"
	"fmt"
	"math"
	"strings"

	"arg"
)

// orientation is the type that represents the plane's orientation
type orientation int

const (
	// XY - plane
	XY orientation = iota
	// XZ - plane
	XZ
	// ZY - plane
	ZY
)

// String representation of an orientation value
func (o orientation) String() string {
	switch o {
	case XY:
		return "xy"
	case XZ:
		return "xz"
	case ZY:
		return "zy"
	default:
		panic("Invalid orientation value")
	}
}

// formatDVID - format the orientation for DVID
func (o orientation) formatDVID() string {
	switch o {
	case XY:
		return "xy"
	case XZ:
		return "xz"
	case ZY:
		return "yz"
	default:
		panic("Invalid orientation value")
	}
}

// Set the orientation value - Orientation implements a flag.Value Set method
func (o *orientation) Set(value string) (err error) {
	switch strings.ToLower(value) {
	case "xy", "yx":
		*o = XY
	case "xz", "zx":
		*o = XZ
	case "zy", "yz":
		*o = ZY
	default:
		err = fmt.Errorf("Invalid orientation value: %s - valid values are: {xy, xz, zy}", value)
	}
	return
}

// Attrs registers mipmaps attributes
type Attrs struct {
	Configs  arg.StringList
	helpFlag bool

	imageWidth, imageHeight, imageDepth int64

	sourceMinX, sourceMinY, sourceMinZ int64
	sourceMaxX, sourceMaxY, sourceMaxZ int64
	sourceTileWidth, sourceTileHeight  int64
	sourceRootURL, sourceStackFormat   string

	targetMinX, targetMinY, targetMinZ int64
	targetMaxX, targetMaxY, targetMaxZ int64
	targetTileWidth, targetTileHeight  int64
	targetRootURL, targetStackFormat   string

	xyTargetStackFormat string
	xzTargetStackFormat string
	zyTargetStackFormat string

	sourceXYRes, sourceZRes       float64
	sourceScale, sourceBackground uint

	targetOrientation                  orientation
	targetImageType, targetImageFormat string
	targetImageQuality                 float64

	interpolation     string
	processEmptyTiles bool

	srcScaleFmt                                            string
	srcTileColFmt, srcTileRowFmt, srcTileLayerFmt          string
	srcXFmt, srcYFmt, srcZFmt                              string
	targetScaleFmt                                         string
	targetTileColFmt, targetTileRowFmt, targetTileLayerFmt string
	targetXFmt, targetYFmt, targetZFmt                     string

	totalVolume, sourceVolume, processedVolume volume
}

// Name method
func (a *Attrs) Name() string {
	return "dmg"
}

// DefineArgs method
func (a *Attrs) DefineArgs(fs *flag.FlagSet) {
	fs.Var(&a.Configs, "config", "list of configuration files which applied in the order they are specified")
	fs.BoolVar(&a.helpFlag, "h", false, "gray image flag")
	fs.Int64Var(&a.imageWidth, "image_width", -1, "Image width")
	fs.Int64Var(&a.imageHeight, "image_height", -1, "Image height")
	fs.Int64Var(&a.imageDepth, "image_depth", -1, "Image depth")
	fs.Int64Var(&a.sourceMinX, "source_min_x", 0, "Cropped volume min X in pixel coordinates.")
	fs.Int64Var(&a.sourceMinY, "source_min_y", 0, "Cropped volume min Y in pixel coordinates.")
	fs.Int64Var(&a.sourceMinZ, "source_min_z", 0, "Cropped volume min Z in pixel coordinates.")
	fs.Int64Var(&a.sourceMaxX, "source_max_x", -1, "Cropped volume max X in pixel coordinates.")
	fs.Int64Var(&a.sourceMaxY, "source_max_y", -1, "Cropped volume max Y in pixel coordinates.")
	fs.Int64Var(&a.sourceMaxZ, "source_max_z", -1, "Cropped volume max Z in pixel coordinates.")
	fs.Int64Var(&a.sourceTileWidth, "source_tile_width", 8192, "Source tile width in pixels.")
	fs.Int64Var(&a.sourceTileHeight, "source_tile_height", 8192, "Source tile height in pixles.")
	fs.StringVar(&a.sourceRootURL, "source_url", "", "Source root url")
	fs.StringVar(&a.sourceStackFormat, "source_stack_format", "", "Source stack format")
	fs.Int64Var(&a.targetMinX, "target_min_x", 0, "Processed volume min X in pixel coordinates relative to sourceMinX.")
	fs.Int64Var(&a.targetMinY, "target_min_y", 0, "Processed volume min Y in pixel coordinates relative to sourceMinY.")
	fs.Int64Var(&a.targetMinZ, "target_min_z", 0, "Processed volume min Z in pixel coordinates relative to sourceMinZ.")
	fs.Int64Var(&a.targetMaxX, "target_max_x", -1, "Processed volume max X in pixel coordinates relative to sourceMinX.")
	fs.Int64Var(&a.targetMaxY, "target_max_y", -1, "Processed volume max Y in pixel coordinates relative to sourceMinY.")
	fs.Int64Var(&a.targetMaxZ, "target_max_z", -1, "Processed volume max Z in pixel coordinates relative to sourceMinZ.")
	fs.Int64Var(&a.targetTileWidth, "target_tile_width", 1024, "Target tile width in pixels.")
	fs.Int64Var(&a.targetTileHeight, "target_tile_height", 1024, "Target tile height in pixles.")
	fs.StringVar(&a.targetRootURL, "target_url", "", "Target root url, e.g., 'dvid://localdvid/<mynodeuuid>/<mytileinstance>/tile'")
	fs.StringVar(&a.targetStackFormat, "target_stack_format", "", "Target stack format, e.g., '{plane}/{scale}/{tile_col}/{tile_row}/{tile_layer}'")
	fs.StringVar(&a.xyTargetStackFormat, "xy_stack_format", "", "XY target stack format, e.g., '{plane}/{scale}/{tile_col}/{tile_row}/{tile_layer}'")
	fs.StringVar(&a.xzTargetStackFormat, "xz_stack_format", "", "XZ target stack format, e.g., '{plane}/{scale}/{tile_col}/{tile_layer}/{tile_row}'")
	fs.StringVar(&a.zyTargetStackFormat, "zy_stack_format", "", "ZY target stack format, e.g., '{plane}/{scale}/{tile_layer}/{tile_row}/{tile_col}'")
	fs.Float64Var(&a.sourceXYRes, "source_xy_res", 1.0, "Source XY resolution")
	fs.Float64Var(&a.sourceZRes, "source_z_res", 1.0, "Source Z resolution")
	fs.UintVar(&a.sourceScale, "source_scale", 0, "Source scale")
	fs.UintVar(&a.sourceBackground, "source_bg", 0, "Source background pixel")
	fs.Var(&a.targetOrientation, "orientation", "Target orientation")
	fs.StringVar(&a.targetImageType, "image_type", "gray", "Target image type: gray | rgb")
	fs.StringVar(&a.targetImageFormat, "image_format", "jpg", "Target image format: jpg | png | tiff")
	fs.Float64Var(&a.targetImageQuality, "image_quality", 1.0, "Target image quality")
	fs.StringVar(&a.interpolation, "interpolation", "", "Interpolation algorithm")
	fs.BoolVar(&a.processEmptyTiles, "process_empty_tiles", false, "Process empty tiles")
	fs.StringVar(&a.srcScaleFmt, "src_scale_fmt", "", "Scale format descriptor")
	fs.StringVar(&a.srcTileColFmt, "src_tile_col_fmt", "", "Tile col format descriptor")
	fs.StringVar(&a.srcTileRowFmt, "src_tile_row_fmt", "", "Tile row format descriptor")
	fs.StringVar(&a.srcTileLayerFmt, "src_tile_layer_fmt", "", "Tile layer format descriptor")
	fs.StringVar(&a.srcXFmt, "src_x_fmt", "", "X coordinate format descriptor")
	fs.StringVar(&a.srcYFmt, "src_y_fmt", "", "Y coordinate format descriptor")
	fs.StringVar(&a.srcZFmt, "src_z_fmt", "", "Z coordinate format descriptor")
	fs.StringVar(&a.targetScaleFmt, "scale_fmt", "", "Scale format descriptor")
	fs.StringVar(&a.targetTileColFmt, "tile_col_fmt", "", "Tile col format descriptor")
	fs.StringVar(&a.targetTileRowFmt, "tile_row_fmt", "", "Tile row format descriptor")
	fs.StringVar(&a.targetTileLayerFmt, "tile_layer_fmt", "", "Tile layer format descriptor")
	fs.StringVar(&a.targetXFmt, "x_fmt", "", "X coordinate format descriptor")
	fs.StringVar(&a.targetYFmt, "y_fmt", "", "Y coordinate format descriptor")
	fs.StringVar(&a.targetZFmt, "z_fmt", "", "Z coordinate format descriptor")
}

// IsHelpFlagSet method
func (a *Attrs) IsHelpFlagSet() bool {
	return a.helpFlag
}

func (a *Attrs) ignoreEmptyTiles() bool {
	return !a.processEmptyTiles
}

// extractMipmapsAttrs populates mipmaps attributes from command line flags
func (a *Attrs) extractMipmapsAttrs(ja *arg.Args) (err error) {
	if a.Configs, err = ja.GetStringListArgValue("config"); err != nil {
		return err
	}
	if a.imageWidth, err = ja.GetInt64ArgValue("image_width"); err != nil {
		return err
	}
	if a.imageHeight, err = ja.GetInt64ArgValue("image_height"); err != nil {
		return err
	}
	if a.imageDepth, err = ja.GetInt64ArgValue("image_depth"); err != nil {
		return err
	}
	if a.sourceMinX, err = ja.GetInt64ArgValue("source_min_x"); err != nil {
		return err
	}
	if a.sourceMinY, err = ja.GetInt64ArgValue("source_min_y"); err != nil {
		return err
	}
	if a.sourceMinZ, err = ja.GetInt64ArgValue("source_min_z"); err != nil {
		return err
	}
	if a.sourceMaxX, err = ja.GetInt64ArgValue("source_max_x"); err != nil {
		return err
	}
	if a.sourceMaxY, err = ja.GetInt64ArgValue("source_max_y"); err != nil {
		return err
	}
	if a.sourceMaxZ, err = ja.GetInt64ArgValue("source_max_z"); err != nil {
		return err
	}
	if a.sourceTileWidth, err = ja.GetInt64ArgValue("source_tile_width"); err != nil {
		return err
	}
	if a.sourceTileHeight, err = ja.GetInt64ArgValue("source_tile_height"); err != nil {
		return err
	}
	if a.sourceRootURL, err = ja.GetStringArgValue("source_url"); err != nil {
		return err
	}
	if a.sourceStackFormat, err = ja.GetStringArgValue("source_stack_format"); err != nil {
		return err
	}
	if a.targetMinX, err = ja.GetInt64ArgValue("target_min_x"); err != nil {
		return err
	}
	if a.targetMinY, err = ja.GetInt64ArgValue("target_min_y"); err != nil {
		return err
	}
	if a.targetMinZ, err = ja.GetInt64ArgValue("target_min_z"); err != nil {
		return err
	}
	if a.targetMaxX, err = ja.GetInt64ArgValue("target_max_x"); err != nil {
		return err
	}
	if a.targetMaxY, err = ja.GetInt64ArgValue("target_max_y"); err != nil {
		return err
	}
	if a.targetMaxZ, err = ja.GetInt64ArgValue("target_max_z"); err != nil {
		return err
	}
	if a.targetTileWidth, err = ja.GetInt64ArgValue("target_tile_width"); err != nil {
		return err
	}
	if a.targetTileHeight, err = ja.GetInt64ArgValue("target_tile_height"); err != nil {
		return err
	}
	if a.targetRootURL, err = ja.GetStringArgValue("target_url"); err != nil {
		return err
	}
	if a.targetStackFormat, err = ja.GetStringArgValue("target_stack_format"); err != nil {
		return err
	}
	if a.xyTargetStackFormat, err = ja.GetStringArgValue("xy_stack_format"); err != nil {
		return err
	}
	if a.xzTargetStackFormat, err = ja.GetStringArgValue("xz_stack_format"); err != nil {
		return err
	}
	if a.zyTargetStackFormat, err = ja.GetStringArgValue("zy_stack_format"); err != nil {
		return err
	}
	if a.sourceXYRes, err = ja.GetFloat64ArgValue("source_xy_res"); err != nil {
		return err
	}
	if a.sourceZRes, err = ja.GetFloat64ArgValue("source_z_res"); err != nil {
		return err
	}
	if a.sourceScale, err = ja.GetUintArgValue("source_scale"); err != nil {
		return err
	}
	if a.sourceBackground, err = ja.GetUintArgValue("source_bg"); err != nil {
		return err
	}
	if targetOrientation, err := ja.GetIntArgValue("orientation"); err != nil {
		return err
	} else {
		a.targetOrientation = orientation(targetOrientation)
	}
	if a.targetImageType, err = ja.GetStringArgValue("image_type"); err != nil {
		return err
	}
	if a.targetImageFormat, err = ja.GetStringArgValue("image_format"); err != nil {
		return err
	}
	if a.targetImageQuality, err = ja.GetFloat64ArgValue("image_quality"); err != nil {
		return err
	}
	if a.interpolation, err = ja.GetStringArgValue("interpolation"); err != nil {
		return err
	}
	if a.processEmptyTiles, err = ja.GetBoolArgValue("process_empty_tiles"); err != nil {
		return err
	}
	if a.srcScaleFmt, err = ja.GetStringArgValue("src_scale_fmt"); err != nil {
		return err
	}
	if a.srcTileColFmt, err = ja.GetStringArgValue("src_tile_col_fmt"); err != nil {
		return err
	}
	if a.srcTileRowFmt, err = ja.GetStringArgValue("src_tile_row_fmt"); err != nil {
		return err
	}
	if a.srcTileLayerFmt, err = ja.GetStringArgValue("src_tile_layer_fmt"); err != nil {
		return err
	}
	if a.srcXFmt, err = ja.GetStringArgValue("src_x_fmt"); err != nil {
		return err
	}
	if a.srcYFmt, err = ja.GetStringArgValue("src_y_fmt"); err != nil {
		return err
	}
	if a.srcZFmt, err = ja.GetStringArgValue("src_z_fmt"); err != nil {
		return err
	}
	if a.targetScaleFmt, err = ja.GetStringArgValue("scale_fmt"); err != nil {
		return err
	}
	if a.targetTileColFmt, err = ja.GetStringArgValue("tile_col_fmt"); err != nil {
		return err
	}
	if a.targetTileRowFmt, err = ja.GetStringArgValue("tile_row_fmt"); err != nil {
		return err
	}
	if a.targetTileLayerFmt, err = ja.GetStringArgValue("tile_layer_fmt"); err != nil {
		return err
	}
	if a.targetXFmt, err = ja.GetStringArgValue("x_fmt"); err != nil {
		return err
	}
	if a.targetYFmt, err = ja.GetStringArgValue("y_fmt"); err != nil {
		return err
	}
	if a.targetZFmt, err = ja.GetStringArgValue("z_fmt"); err != nil {
		return err
	}
	a.updateTotalVolume()
	a.updateSourceVolume()
	a.updateProcessedVolume()
	return nil
}

// updateTotalVolume updates total image volume
func (a *Attrs) updateTotalVolume() {
	setDim := func(totalDim, croppedDim int64, setter func(int64)) {
		if totalDim > 0 {
			setter(totalDim)
		} else if croppedDim > 0 {
			setter(croppedDim)
		}
	}
	a.totalVolume.x = 0
	a.totalVolume.y = 0
	a.totalVolume.z = 0
	setDim(a.imageWidth, a.sourceMaxX, a.totalVolume.setMaxX)
	setDim(a.imageHeight, a.sourceMaxY, a.totalVolume.setMaxY)
	setDim(a.imageDepth, a.sourceMaxZ, a.totalVolume.setMaxZ)
}

// updateSourceVolume updates the source volume translated to 0,0,0 in source pixel coordinates.
// The precondition is that the arguments are valid
func (a *Attrs) updateSourceVolume() {
	setDim := func(imageDim, sourceDim int64, setter func(int64)) {
		if sourceDim > 0 {
			setter(sourceDim)
		} else if imageDim > 0 {
			setter(imageDim)
		}
	}
	a.sourceVolume.x = a.sourceMinX
	a.sourceVolume.y = a.sourceMinY
	a.sourceVolume.z = a.sourceMinZ
	setDim(a.imageWidth, a.sourceMaxX, a.sourceVolume.setMaxX)
	setDim(a.imageHeight, a.sourceMaxY, a.sourceVolume.setMaxY)
	setDim(a.imageDepth, a.sourceMaxZ, a.sourceVolume.setMaxZ)
}

// updateProcessedVolume updates the volume processed in pixel coordinates relative to the source volume.
func (a *Attrs) updateProcessedVolume() {
	setDim := func(maxLimit, processed int64, setter func(int64)) {
		if processed > 0 && processed < maxLimit {
			setter(processed)
		} else {
			setter(maxLimit)
		}
	}
	a.processedVolume.x = a.targetMinX
	a.processedVolume.y = a.targetMinY
	a.processedVolume.z = a.targetMinZ
	setDim(a.sourceVolume.x, a.targetMaxX, a.processedVolume.setMaxX)
	setDim(a.sourceVolume.y, a.targetMaxY, a.processedVolume.setMaxY)
	setDim(a.sourceVolume.z, a.targetMaxZ, a.processedVolume.setMaxZ)
}

// validate arguments
func (a *Attrs) validate() error {
	// validate the width
	if a.imageWidth <= 0 && a.sourceMaxX <= 0 {
		return fmt.Errorf("Invalid image width: imageWidth=%v, maxX=%v", a.imageWidth, a.sourceMaxX)
	}
	// validate the height
	if a.imageHeight <= 0 && a.sourceMaxY <= 0 {
		return fmt.Errorf("Invalid image height: imageHeight=%v, maxY=%v", a.imageHeight, a.sourceMaxY)
	}
	// validate the depth
	if a.imageDepth <= 0 && a.sourceMaxZ <= 0 {
		return fmt.Errorf("Invalid image depth: imageDepth=%v, maxZ=%v", a.imageDepth, a.sourceMaxZ)
	}
	return nil
}

func (a Attrs) getScaleZFactor() float64 {
	var scaleZ float64
	if a.sourceXYRes <= 0 || a.sourceZRes <= 0 {
		scaleZ = 1.0
	} else {
		scaleZ = a.sourceZRes / a.sourceXYRes
	}
	return scaleZ
}

func (a Attrs) scaleArgs() Attrs {
	ra := a
	scaleZFactor := a.getScaleZFactor()
	// XY mipmaps are typically generated without any Z scaling so for XY orientation we
	// don't use Z scaling factor but for all other orientation we use it
	if a.targetOrientation == XY {
		ra.imageWidth = a.imageWidth >> a.sourceScale
		ra.imageHeight = a.imageHeight >> a.sourceScale
		ra.imageDepth = a.imageDepth
		sourceVolume := volume{
			x: a.sourceVolume.x, y: a.sourceVolume.y, z: a.sourceVolume.z,
			dx: a.sourceVolume.dx, dy: a.sourceVolume.dy, dz: a.sourceVolume.dz,
		}
		ra.sourceVolume = sourceVolume.scale(a.sourceScale, a.sourceScale, 0)
		processedVolume := volume{
			x: a.processedVolume.x, y: a.processedVolume.y, z: a.processedVolume.z,
			dx: a.processedVolume.dx, dy: a.processedVolume.dy, dz: a.processedVolume.dz,
		}
		ra.processedVolume = processedVolume.scale(a.sourceScale, a.sourceScale, 0)
	} else if a.targetOrientation == XZ {
		ra.imageWidth = a.imageWidth >> a.sourceScale
		ra.imageHeight = scaleDim(a.imageDepth, scaleZFactor, math.Ceil) >> a.sourceScale
		ra.imageDepth = a.imageHeight >> a.sourceScale
		sourceVolume := volume{
			x:  a.sourceVolume.x,
			y:  scaleDim(a.sourceVolume.z, scaleZFactor, math.Floor),
			z:  a.sourceVolume.y,
			dx: a.sourceVolume.dx,
			dy: scaleDim(a.sourceVolume.dz, scaleZFactor, math.Ceil),
			dz: a.sourceVolume.dy,
		}
		ra.sourceVolume = sourceVolume.scale(a.sourceScale, a.sourceScale, a.sourceScale)
		processedVolume := volume{
			x:  a.processedVolume.x,
			y:  scaleDim(a.processedVolume.z, scaleZFactor, math.Floor),
			z:  a.processedVolume.y,
			dx: a.processedVolume.dx,
			dy: scaleDim(a.processedVolume.dz, scaleZFactor, math.Ceil),
			dz: a.processedVolume.dy,
		}
		ra.processedVolume = processedVolume.scale(a.sourceScale, a.sourceScale, a.sourceScale)
	} else if a.targetOrientation == ZY {
		ra.imageWidth = scaleDim(a.imageDepth, scaleZFactor, math.Ceil) >> a.sourceScale
		ra.imageHeight = a.imageHeight >> a.sourceScale
		ra.imageDepth = a.imageWidth >> a.sourceScale
		sourceVolume := volume{
			x:  scaleDim(a.sourceVolume.z, scaleZFactor, math.Floor),
			y:  a.sourceVolume.y,
			z:  a.sourceVolume.x,
			dx: scaleDim(a.sourceVolume.dz, scaleZFactor, math.Ceil),
			dy: a.sourceVolume.dy,
			dz: a.sourceVolume.dx,
		}
		ra.sourceVolume = sourceVolume.scale(a.sourceScale, a.sourceScale, a.sourceScale)
		processedVolume := volume{
			x:  scaleDim(a.processedVolume.z, scaleZFactor, math.Floor),
			y:  a.processedVolume.y,
			z:  a.processedVolume.x,
			dx: scaleDim(a.processedVolume.dz, scaleZFactor, math.Ceil),
			dy: a.processedVolume.dy,
			dz: a.processedVolume.dx,
		}
		ra.processedVolume = processedVolume.scale(a.sourceScale, a.sourceScale, a.sourceScale)
	}
	return ra
}

func scaleDim(val int64, scaleFactor float64, roundFunc func(float64) float64) int64 {
	return int64(roundFunc(float64(val) * scaleFactor))
}

// volume struct
type volume struct {
	x, y, z    int64
	dx, dy, dz int64
}

// maxX volume's max X
func (v volume) maxX() int64 {
	return v.x + v.dx
}

// setMaxX sets volume's max X
func (v *volume) setMaxX(x int64) {
	v.dx = x - v.x
}

// maxY volume's max Y
func (v volume) maxY() int64 {
	return v.y + v.dy
}

// setMaxY sets volume's max Y
func (v *volume) setMaxY(y int64) {
	v.dy = y - v.y
}

// maxZ volume's max Z
func (v volume) maxZ() int64 {
	return v.z + v.dz
}

// endZ closed upper bound of the volume's Z
func (v volume) endZ() int64 {
	end := v.z + v.dz - 1
	if end < 0 {
		end = 0
	}
	return end
}

// setMaxZ sets volume's max Z
func (v *volume) setMaxZ(z int64) {
	v.dz = z - v.z
}

// scale scales the volume by x, y, z with the given factors
func (v volume) scale(xFactor, yFactor, zFactor uint) volume {
	return volume{
		x:  v.x >> xFactor,
		y:  v.y >> yFactor,
		z:  v.z >> zFactor,
		dx: v.dx >> xFactor,
		dy: v.dy >> yFactor,
		dz: v.dz >> zFactor,
	}
}

// String returns the string representation of a volume
func (v volume) String() string {
	return fmt.Sprintf("(%d, %d, %d) (%d, %d, %d)", v.x, v.y, v.z, v.dx, v.dy, v.dz)
}
