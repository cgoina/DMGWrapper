package mipmaps

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"arg"
	"config"
	"process"
)

// retileCmdlineBuilder retiler command line builder
type retileCmdlineBuilder struct {
	baseMipmapsCmdlineBuilder
}

// GetCmdlineArgs creates command line arguments for retiling
func (clb retileCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var mipmapsAttrs Attrs

	if err = mipmapsAttrs.extractMipmapsAttrs(&a); err != nil {
		return cmdargs, err
	}
	cmdargs = clb.setJvmMemory(cmdargs, clb.resources.GetStringProperty("tilingMemory"))
	cmdargs = arg.AddIntArg(cmdargs, "-DtileCacheSize", clb.resources.GetInt64Property("tilerCacheSize"), "=")

	sourceCTStackFormat := toCatmaidToolsStackFmt(mipmapsAttrs.sourceStackFormat, map[string]string{
		"{plane}":        XY.String(),
		"{scale}":        arg.DefaultIfEmpty(mipmapsAttrs.srcScaleFmt, "%1$d"),
		"{tile_col}":     arg.DefaultIfEmpty(mipmapsAttrs.srcTileColFmt, "%9$d"),
		"{tile_row}":     arg.DefaultIfEmpty(mipmapsAttrs.srcTileRowFmt, "%8$d"),
		"{tile_layer}":   arg.DefaultIfEmpty(mipmapsAttrs.srcTileLayerFmt, "%5$d"),
		"{x}":            arg.DefaultIfEmpty(mipmapsAttrs.srcXFmt, "%3$d"),
		"{y}":            arg.DefaultIfEmpty(mipmapsAttrs.srcYFmt, "%4$d"),
		"{z}":            arg.DefaultIfEmpty(mipmapsAttrs.srcZFmt, "%5$d"),
		"{tile_width}":   strconv.FormatInt(mipmapsAttrs.sourceTileWidth, 10),
		"{tile:_height}": strconv.FormatInt(mipmapsAttrs.sourceTileHeight, 10),
	})
	cmdargs = arg.AddArg(cmdargs, "-DsourceUrlFormat", makeURL(clb.formatRootURL(mipmapsAttrs.sourceRootURL), sourceCTStackFormat), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceWidth", mipmapsAttrs.totalVolume.dx, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceHeight", mipmapsAttrs.totalVolume.dy, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceDepth", mipmapsAttrs.totalVolume.dz, "=")
	cmdargs = arg.AddUintArg(cmdargs, "-DsourceScaleLevel", uint64(mipmapsAttrs.sourceScale), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceTileWidth", mipmapsAttrs.sourceTileWidth, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceTileHeight", mipmapsAttrs.sourceTileHeight, "=")
	cmdargs = arg.AddFloatArg(cmdargs, "-DsourceResXY", mipmapsAttrs.sourceXYRes, 3, 64, "=")
	cmdargs = arg.AddFloatArg(cmdargs, "-DsourceResZ", mipmapsAttrs.sourceZRes, 3, 64, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminX", mipmapsAttrs.sourceVolume.x, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminY", mipmapsAttrs.sourceVolume.y, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminZ", mipmapsAttrs.sourceVolume.z, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-Dwidth", mipmapsAttrs.sourceVolume.dx, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-Dheight", mipmapsAttrs.sourceVolume.dy, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-Ddepth", mipmapsAttrs.sourceVolume.dz, "=")

	cmdargs = arg.AddArg(cmdargs, "-DexportBasePath", clb.formatRootURL(mipmapsAttrs.targetRootURL), "=")
	targetCTStackFormat := toCatmaidToolsStackFmt(mipmapsAttrs.targetStackFormat, map[string]string{
		"{plane}":        mipmapsAttrs.targetOrientation.String(),
		"{scale}":        arg.DefaultIfEmpty(mipmapsAttrs.targetScaleFmt, "%1$d"),
		"{tile_col}":     arg.DefaultIfEmpty(mipmapsAttrs.targetTileColFmt, "%9$d"),
		"{tile_row}":     arg.DefaultIfEmpty(mipmapsAttrs.targetTileRowFmt, "%8$d"),
		"{tile_layer}":   arg.DefaultIfEmpty(mipmapsAttrs.targetTileLayerFmt, "%5$d"),
		"{x}":            arg.DefaultIfEmpty(mipmapsAttrs.targetXFmt, "%3$d"),
		"{y}":            arg.DefaultIfEmpty(mipmapsAttrs.targetYFmt, "%4$d"),
		"{z}":            arg.DefaultIfEmpty(mipmapsAttrs.targetZFmt, "%5$d"),
		"{tile_width}":   strconv.FormatInt(mipmapsAttrs.targetTileWidth, 10),
		"{tile:_height}": strconv.FormatInt(mipmapsAttrs.targetTileHeight, 10),
	})
	cmdargs = arg.AddArg(cmdargs, "-DtilePattern", targetCTStackFormat, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DtileWidth", mipmapsAttrs.targetTileWidth, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DtileHeight", mipmapsAttrs.targetTileHeight, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMinX", mipmapsAttrs.processedVolume.x, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMinY", mipmapsAttrs.processedVolume.y, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMinZ", mipmapsAttrs.processedVolume.z, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMaxX", mipmapsAttrs.processedVolume.maxX(), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMaxY", mipmapsAttrs.processedVolume.maxY(), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DexportMaxZ", mipmapsAttrs.processedVolume.maxZ(), "=")
	cmdargs = arg.AddArg(cmdargs, "-Dorientation", mipmapsAttrs.targetOrientation.String(), "=")

	cmdargs = arg.AddArg(cmdargs, "-Dformat", mipmapsAttrs.targetImageFormat, "=")
	cmdargs = arg.AddFloatArg(cmdargs, "-Dquality", mipmapsAttrs.targetImageQuality, 2, 32, "=")
	cmdargs = arg.AddArg(cmdargs, "-Dtype", mipmapsAttrs.targetImageType, "=")

	cmdargs = arg.AddUintArg(cmdargs, "-DbgValue", uint64(mipmapsAttrs.sourceBackground), "=")
	cmdargs = arg.AddBoolArg(cmdargs, "-DignoreEmptyTiles", mipmapsAttrs.ignoreEmptyTiles(), "=")
	cmdargs = arg.AddArg(cmdargs, "-Dinterpolation", mipmapsAttrs.interpolation, "=")

	cmdargs = arg.AddArgs(cmdargs, "-jar", clb.resources.GetStringProperty("tilingJar"))

	return cmdargs, nil
}

// scaleCmdlineBuilder scaler command line builder
type scaleCmdlineBuilder struct {
	baseMipmapsCmdlineBuilder
}

// GetCmdlineArgs creates command line arguments for retiling
func (clb scaleCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var mipmapsAttrs Attrs

	if err = mipmapsAttrs.extractMipmapsAttrs(&a); err != nil {
		return cmdargs, err
	}
	cmdargs = clb.setJvmMemory(cmdargs, clb.resources.GetStringProperty("scalingMemory"))
	tileCTStackFormat := toCatmaidToolsStackFmt(mipmapsAttrs.targetStackFormat, map[string]string{
		"{plane}":        mipmapsAttrs.targetOrientation.String(),
		"{scale}":        arg.DefaultIfEmpty(mipmapsAttrs.targetScaleFmt, "%1$d"),
		"{tile_col}":     arg.DefaultIfEmpty(mipmapsAttrs.targetTileColFmt, "%9$d"),
		"{tile_row}":     arg.DefaultIfEmpty(mipmapsAttrs.targetTileRowFmt, "%8$d"),
		"{tile_layer}":   arg.DefaultIfEmpty(mipmapsAttrs.targetTileLayerFmt, "%5$d"),
		"{x}":            arg.DefaultIfEmpty(mipmapsAttrs.targetXFmt, "%3$d"),
		"{y}":            arg.DefaultIfEmpty(mipmapsAttrs.targetYFmt, "%4$d"),
		"{z}":            arg.DefaultIfEmpty(mipmapsAttrs.targetZFmt, "%5$d"),
		"{tile_width}":   strconv.FormatInt(mipmapsAttrs.targetTileWidth, 10),
		"{tile:_height}": strconv.FormatInt(mipmapsAttrs.targetTileHeight, 10),
	})
        cmdargs = arg.AddArg(cmdargs, "-DtileFormat", makeURL(clb.formatRootURL(mipmapsAttrs.targetRootURL), tileCTStackFormat), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceWidth", mipmapsAttrs.totalVolume.dx, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceHeight", mipmapsAttrs.totalVolume.dy, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DsourceDepth", mipmapsAttrs.totalVolume.dz, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminX", mipmapsAttrs.sourceVolume.x, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminY", mipmapsAttrs.sourceVolume.y, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DminZ", mipmapsAttrs.processedVolume.z, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-Dwidth", mipmapsAttrs.sourceVolume.dx, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-Dheight", mipmapsAttrs.sourceVolume.dy, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DmaxZ", mipmapsAttrs.processedVolume.endZ(), "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DtileWidth", mipmapsAttrs.targetTileWidth, "=")
	cmdargs = arg.AddIntArg(cmdargs, "-DtileHeight", mipmapsAttrs.targetTileHeight, "=")

	cmdargs = arg.AddArg(cmdargs, "-Dformat", mipmapsAttrs.targetImageFormat, "=")
	cmdargs = arg.AddFloatArg(cmdargs, "-Dquality", mipmapsAttrs.targetImageQuality, 2, 32, "=")
	cmdargs = arg.AddArg(cmdargs, "-Dtype", mipmapsAttrs.targetImageType, "=")

	cmdargs = arg.AddUintArg(cmdargs, "-DbgValue", uint64(mipmapsAttrs.sourceBackground), "=")
	cmdargs = arg.AddBoolArg(cmdargs, "-DignoreEmptyTiles", mipmapsAttrs.ignoreEmptyTiles(), "=")

	cmdargs = arg.AddArgs(cmdargs, "-jar", clb.resources.GetStringProperty("scalingJar"))

	return cmdargs, nil
}

type baseMipmapsCmdlineBuilder struct {
	resources   config.Config
	dvidProxies map[string]string
}

func (clb baseMipmapsCmdlineBuilder) setJvmMemory(cmdargs []string, jvmMemory string) []string {
	if jvmMemory != "" {
		cmdargs = arg.AddArg(cmdargs, "-Xms", jvmMemory, "")
		cmdargs = arg.AddArg(cmdargs, "-Xms", jvmMemory, "")
	}
	return cmdargs
}

func (clb baseMipmapsCmdlineBuilder) formatRootURL(url string) string {
	if strings.HasPrefix(url, "dvid://") {
		dvidInstance := url[len("dvid://"):]
		if sepIndex := strings.Index(dvidInstance, "/"); sepIndex != -1 {
			dvidInstance = dvidInstance[0:sepIndex]
		}
		dvidProxyURL := clb.dvidProxies[dvidInstance]
		if dvidProxyURL != "" {
			return strings.Replace(url, "dvid://"+dvidInstance, dvidProxyURL, 1)
		}
		return strings.Replace(url, "dvid://", "http://", 1)
	}
	return url
}

func (clb baseMipmapsCmdlineBuilder) createCmdline(cmdargs []string, mipmapsAttrs Attrs) ([]string, error) {
	if mipmapsAttrs.imageWidth > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-image_width", strconv.FormatInt(mipmapsAttrs.imageWidth, 10))
	}
	if mipmapsAttrs.imageHeight > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-image_height", strconv.FormatInt(mipmapsAttrs.imageHeight, 10))
	}
	if mipmapsAttrs.imageDepth > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-image_depth", strconv.FormatInt(mipmapsAttrs.imageDepth, 10))
	}
	if mipmapsAttrs.sourceMinX > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_min_x", strconv.FormatInt(mipmapsAttrs.sourceMinX, 10))
	}
	if mipmapsAttrs.sourceMinY > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_min_y", strconv.FormatInt(mipmapsAttrs.sourceMinY, 10))
	}
	if mipmapsAttrs.sourceMinZ > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_min_z", strconv.FormatInt(mipmapsAttrs.sourceMinZ, 10))
	}
	if mipmapsAttrs.sourceMaxX > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_max_x", strconv.FormatInt(mipmapsAttrs.sourceMaxX, 10))
	}
	if mipmapsAttrs.sourceMaxY > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_max_y", strconv.FormatInt(mipmapsAttrs.sourceMaxY, 10))
	}
	if mipmapsAttrs.sourceMaxZ > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_max_z", strconv.FormatInt(mipmapsAttrs.sourceMaxZ, 10))
	}
	cmdargs = arg.AddArgs(cmdargs,
		"-source_tile_width", strconv.FormatInt(mipmapsAttrs.sourceTileWidth, 10),
		"-source_tile_height", strconv.FormatInt(mipmapsAttrs.sourceTileHeight, 10))

	cmdargs = arg.AddArgs(cmdargs,
		"-source_url", mipmapsAttrs.sourceRootURL,
		"-source_stack_format", mipmapsAttrs.sourceStackFormat)

	if mipmapsAttrs.targetMinX > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_min_x", strconv.FormatInt(mipmapsAttrs.targetMinX, 10))
	}
	if mipmapsAttrs.targetMinY > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_min_y", strconv.FormatInt(mipmapsAttrs.targetMinY, 10))
	}
	if mipmapsAttrs.targetMinZ > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_min_z", strconv.FormatInt(mipmapsAttrs.targetMinZ, 10))
	}
	if mipmapsAttrs.targetMaxX > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_max_x", strconv.FormatInt(mipmapsAttrs.targetMaxX, 10))
	}
	if mipmapsAttrs.targetMaxY > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_max_y", strconv.FormatInt(mipmapsAttrs.targetMaxY, 10))
	}
	if mipmapsAttrs.targetMaxZ > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_max_z", strconv.FormatInt(mipmapsAttrs.targetMaxZ, 10))
	}
	if mipmapsAttrs.targetTileWidth > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_tile_width", strconv.FormatInt(mipmapsAttrs.targetTileWidth, 10))
	}
	if mipmapsAttrs.targetTileHeight > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-target_tile_height", strconv.FormatInt(mipmapsAttrs.targetTileHeight, 10))
	}

	cmdargs = arg.AddArgs(cmdargs,
		"-target_url", mipmapsAttrs.targetRootURL,
		"-target_stack_format", mipmapsAttrs.targetStackFormat)

	cmdargs = arg.AddArgs(cmdargs,
		"-source_xy_res", strconv.FormatFloat(mipmapsAttrs.sourceXYRes, 'g', -1, 64),
		"-source_z_res", strconv.FormatFloat(mipmapsAttrs.sourceZRes, 'g', -1, 64))

	if mipmapsAttrs.sourceScale > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_scale", strconv.FormatUint(uint64(mipmapsAttrs.sourceScale), 10))
	}
	if mipmapsAttrs.sourceBackground > 0 {
		cmdargs = arg.AddArgs(cmdargs, "-source_bg", strconv.FormatUint(uint64(mipmapsAttrs.sourceBackground), 10))
	}

	cmdargs = arg.AddArgs(cmdargs, "-orientation", mipmapsAttrs.targetOrientation.String())
	cmdargs = arg.AddArgs(cmdargs,
		"-image_type", mipmapsAttrs.targetImageType,
		"-image_format", mipmapsAttrs.targetImageFormat,
		"-image_quality", strconv.FormatFloat(mipmapsAttrs.targetImageQuality, 'g', -1, 64),
		"-interpolation", mipmapsAttrs.interpolation)
	if mipmapsAttrs.processEmptyTiles {
		cmdargs = arg.AddArgs(cmdargs, "process_empty_tiles")
	}
	if mipmapsAttrs.srcScaleFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_scale_fmt", mipmapsAttrs.srcScaleFmt)
	}
	if mipmapsAttrs.srcTileColFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_tile_col_fmt", mipmapsAttrs.srcTileColFmt)
	}
	if mipmapsAttrs.srcTileRowFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_tile_row_fmt", mipmapsAttrs.srcTileRowFmt)
	}
	if mipmapsAttrs.srcTileLayerFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_tile_layer_fmt", mipmapsAttrs.srcTileLayerFmt)
	}
	if mipmapsAttrs.srcXFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_x_fmt", mipmapsAttrs.srcXFmt)
	}
	if mipmapsAttrs.srcYFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_y_fmt", mipmapsAttrs.srcYFmt)
	}
	if mipmapsAttrs.srcZFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "src_z_fmt", mipmapsAttrs.srcZFmt)
	}
	if mipmapsAttrs.targetScaleFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "scale_fmt", mipmapsAttrs.targetScaleFmt)
	}
	if mipmapsAttrs.targetTileColFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "tile_col_fmt", mipmapsAttrs.targetTileColFmt)
	}
	if mipmapsAttrs.targetTileRowFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "tile_row_fmt", mipmapsAttrs.targetTileRowFmt)
	}
	if mipmapsAttrs.targetTileLayerFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "tile_layer_fmt", mipmapsAttrs.targetTileLayerFmt)
	}
	if mipmapsAttrs.targetXFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "x_fmt", mipmapsAttrs.targetXFmt)
	}
	if mipmapsAttrs.targetYFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "y_fmt", mipmapsAttrs.targetYFmt)
	}
	if mipmapsAttrs.targetZFmt != "" {
		cmdargs = arg.AddArgs(cmdargs, "z_fmt", mipmapsAttrs.targetZFmt)
	}
	return cmdargs, nil
}

// retilerSplitter splitter for retile jobs
type retilerSplitter struct {
	resources    config.Config
	dvidProxies  map[string]string
	nextJobIndex uint64
}

// SplitJob splits the job into multiple parallelizable jobs
func (s retilerSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
	var err error
	var processedXTiles, processedYTiles, processedZLayers, processedDepth int64
	var mipmapsAttrs Attrs
	if processedXTiles = s.resources.GetInt64Property("xTilesPerJob"); processedXTiles == 0 {
		processedXTiles = 1
	}
	if processedYTiles = s.resources.GetInt64Property("yTilesPerJob"); processedYTiles == 0 {
		processedYTiles = 1
	}
	if processedZLayers = s.resources.GetInt64Property("zLayersPerJob"); processedZLayers == 0 {
		processedZLayers = 1
	}

	if err = mipmapsAttrs.extractMipmapsAttrs(&j.JArgs); err != nil {
		return err
	}
	minX := mipmapsAttrs.processedVolume.x
	maxX := mipmapsAttrs.processedVolume.maxX()
	minY := mipmapsAttrs.processedVolume.y
	maxY := mipmapsAttrs.processedVolume.maxY()
	minZ := mipmapsAttrs.processedVolume.z
	maxZ := mipmapsAttrs.processedVolume.maxZ()
	if mipmapsAttrs.targetOrientation == XY {
		processedDepth = processedZLayers
	} else {
		processedDepth = processedZLayers * mipmapsAttrs.sourceTileHeight
	}
	if processedDepth > mipmapsAttrs.processedVolume.dz {
		processedDepth = mipmapsAttrs.processedVolume.dz
	}
	processedWidth := processedXTiles * mipmapsAttrs.sourceTileWidth
	processedHeight := processedYTiles * mipmapsAttrs.sourceTileHeight

	cmdlineBuilder := retileCmdlineBuilder{
		baseMipmapsCmdlineBuilder: baseMipmapsCmdlineBuilder{
			resources:   s.resources,
			dvidProxies: s.dvidProxies,
		},
	}

	for z := minZ; z < maxZ; z += processedDepth {
		jobDepth := processedDepth
		if z+jobDepth > maxZ {
			jobDepth = maxZ - z
		}
		for y := minY; y < maxY; y += processedHeight {
			jobHeight := processedHeight
			if y+jobHeight > maxY {
				jobHeight = maxY - y
			}
			for x := minX; x < maxX; x += processedWidth {
				jobWidth := processedWidth
				if x+jobWidth > maxX {
					jobWidth = maxX - x
				}
				newJobProcessedVolume := volume{x, y, z, jobWidth, jobHeight, jobDepth}
				newJobArgs := j.JArgs.Clone()
				newJobArgs.UpdateInt64Arg("source_min_x", mipmapsAttrs.sourceVolume.x)
				newJobArgs.UpdateInt64Arg("source_min_y", mipmapsAttrs.sourceVolume.y)
				newJobArgs.UpdateInt64Arg("source_min_z", mipmapsAttrs.sourceVolume.z)
				newJobArgs.UpdateInt64Arg("source_max_x", mipmapsAttrs.sourceVolume.maxX())
				newJobArgs.UpdateInt64Arg("source_max_y", mipmapsAttrs.sourceVolume.maxY())
				newJobArgs.UpdateInt64Arg("source_max_z", mipmapsAttrs.sourceVolume.maxZ())

				newJobArgs.UpdateInt64Arg("target_min_x", x)
				newJobArgs.UpdateInt64Arg("target_min_y", y)
				newJobArgs.UpdateInt64Arg("target_min_z", z)
				newJobArgs.UpdateInt64Arg("target_max_x", newJobProcessedVolume.maxX())
				newJobArgs.UpdateInt64Arg("target_max_y", newJobProcessedVolume.maxY())
				newJobArgs.UpdateInt64Arg("target_max_z", newJobProcessedVolume.maxZ())

				newJob := process.Job{
					Executable:     s.resources.GetStringProperty("mipmapsExec"),
					Name:           fmt.Sprintf("%s_%d", j.Name, s.nextJobIndex),
					JArgs:          newJobArgs,
					CmdlineBuilder: cmdlineBuilder,
				}
				log.Printf("Generate Retiling Job %v", newJob)
				jch <- newJob
				s.nextJobIndex++
			}
		}
	}
	return nil
}

// scalerSplitter splitter for scale jobs
type scalerSplitter struct {
	resources    config.Config
	dvidProxies  map[string]string
	nextJobIndex uint64
}

// SplitJob splits the job into multiple parallelizable jobs
func (s scalerSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
	var err error
	var processedZLayers int64
	var mipmapsAttrs Attrs

	if processedZLayers = s.resources.GetInt64Property("zLayersPerJob"); processedZLayers == 0 {
		processedZLayers = 1
	}
	if err = mipmapsAttrs.extractMipmapsAttrs(&j.JArgs); err != nil {
		return err
	}

	scaleAttrs := mipmapsAttrs.scaleArgs()
	minX := scaleAttrs.processedVolume.x
	dX := scaleAttrs.processedVolume.dx
	minY := scaleAttrs.processedVolume.y
	dY := scaleAttrs.processedVolume.dy
	minZ := scaleAttrs.processedVolume.z
	maxZ := scaleAttrs.processedVolume.maxZ()

	cmdlineBuilder := scaleCmdlineBuilder{
		baseMipmapsCmdlineBuilder: baseMipmapsCmdlineBuilder{
			resources:   s.resources,
			dvidProxies: s.dvidProxies,
		},
	}

	for z := minZ; z < maxZ; z += processedZLayers {
		jobDepth := processedZLayers
		if z+jobDepth > maxZ {
			jobDepth = maxZ - z
		}
		newJobArgs := j.JArgs.Clone()

		newJobArgs.UpdateInt64Arg("image_width", scaleAttrs.imageWidth)
		newJobArgs.UpdateInt64Arg("image_height", scaleAttrs.imageHeight)
		newJobArgs.UpdateInt64Arg("image_depth", scaleAttrs.imageDepth)

		newJobArgs.UpdateInt64Arg("source_min_x", scaleAttrs.sourceVolume.x)
		newJobArgs.UpdateInt64Arg("source_min_y", scaleAttrs.sourceVolume.y)
		newJobArgs.UpdateInt64Arg("source_min_z", scaleAttrs.sourceVolume.z)
		newJobArgs.UpdateInt64Arg("source_max_x", scaleAttrs.sourceVolume.maxX())
		newJobArgs.UpdateInt64Arg("source_max_y", scaleAttrs.sourceVolume.maxY())
		newJobArgs.UpdateInt64Arg("source_max_z", scaleAttrs.sourceVolume.maxZ())

		newJobProcessedVolume := volume{minX, minY, z, dX, dY, jobDepth}

		newJobArgs.UpdateInt64Arg("target_min_x", newJobProcessedVolume.x)
		newJobArgs.UpdateInt64Arg("target_min_y", newJobProcessedVolume.y)
		newJobArgs.UpdateInt64Arg("target_min_z", newJobProcessedVolume.z)
		newJobArgs.UpdateInt64Arg("target_max_x", newJobProcessedVolume.maxX())
		newJobArgs.UpdateInt64Arg("target_max_y", newJobProcessedVolume.maxY())
		newJobArgs.UpdateInt64Arg("target_max_z", newJobProcessedVolume.maxZ())

		newJob := process.Job{
			Executable:     s.resources.GetStringProperty("mipmapsExec"),
			Name:           fmt.Sprintf("%s_%d", j.Name, s.nextJobIndex),
			JArgs:          newJobArgs,
			CmdlineBuilder: cmdlineBuilder,
		}
		log.Printf("Generate Scaling Job: %v", newJob)
		jch <- newJob
		s.nextJobIndex++
	}

	return nil
}

func toCatmaidToolsStackFmt(fmtDescriptor string, context map[string]string) string {
	var fmtDescriptorMapping []string
	for k, v := range context {
		fmtDescriptorMapping = append(fmtDescriptorMapping, k, v)
	}
	r := strings.NewReplacer(fmtDescriptorMapping...)
	return r.Replace(fmtDescriptor)
}

func makeURL(base, path string) string {
	components := []string{
		strings.TrimRight(base, "/"),
		strings.Trim(path, "/"),
	}
	return strings.Join(components, "/")
}
