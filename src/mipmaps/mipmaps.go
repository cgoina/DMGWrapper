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

// localRetileCmdlineBuilder retiler command line builder
type localRetileCmdlineBuilder struct {
	resources   config.Config
	dvidProxies DVIDProxyURLMapping
}

// NewLocalRetileCmdlineBuilder creates a command line builder for a local retile job
func NewLocalRetileCmdlineBuilder(resources config.Config, dvidProxies DVIDProxyURLMapping) arg.CmdlineArgBuilder {
	return localRetileCmdlineBuilder{resources, dvidProxies}
}

// GetCmdlineArgs creates command line arguments for retiling
func (clb localRetileCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var mipmapsAttrs Attrs

	if err = mipmapsAttrs.extractMipmapsAttrs(&a); err != nil {
		return cmdargs, err
	}
	cmdargs = setJvmMemory(cmdargs, clb.resources.GetStringProperty("tilingMemory"))
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
	cmdargs = arg.AddArg(cmdargs, "-DsourceUrlFormat", makeURL(clb.dvidProxies.formatRootURL(mipmapsAttrs.sourceRootURL), sourceCTStackFormat), "=")
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

	cmdargs = arg.AddArg(cmdargs, "-DexportBasePath", clb.dvidProxies.formatRootURL(mipmapsAttrs.targetRootURL), "=")
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

// localScaleCmdlineBuilder scaler command line builder
type localScaleCmdlineBuilder struct {
	resources   config.Config
	dvidProxies DVIDProxyURLMapping
}

// NewLocalScaleCmdlineBuilder creates a command line builder for a local retile job
func NewLocalScaleCmdlineBuilder(resources config.Config, dvidProxies DVIDProxyURLMapping) arg.CmdlineArgBuilder {
	return localScaleCmdlineBuilder{resources, dvidProxies}
}

// GetCmdlineArgs creates command line arguments for retiling
func (clb localScaleCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	var mipmapsAttrs Attrs

	if err = mipmapsAttrs.extractMipmapsAttrs(&a); err != nil {
		return cmdargs, err
	}
	cmdargs = setJvmMemory(cmdargs, clb.resources.GetStringProperty("scalingMemory"))
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
	cmdargs = arg.AddArg(cmdargs, "-DtileFormat", makeURL(clb.dvidProxies.formatRootURL(mipmapsAttrs.targetRootURL), tileCTStackFormat), "=")
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

type serviceCmdlineBuilder struct {
	processorType string
	operation     string
	resources     config.Config
}

// NewServiceCmdlineBuilder creates a command line builder for a mipmaps service
func NewServiceCmdlineBuilder(processorType, operation string, resources config.Config) (arg.CmdlineArgBuilder, error) {
	switch operation {
	case "retile", "scale":
	default:
		return nil, fmt.Errorf("Invalid operation - ServiceCmdlineBuilder supports only retile and scale")
	}
	return serviceCmdlineBuilder{
		processorType: processorType,
		operation:     operation,
		resources:     resources,
	}, nil
}

// GetCmdlineArgs creates command line arguments for a mipmaps service invocation
func (clb serviceCmdlineBuilder) GetCmdlineArgs(a arg.Args) ([]string, error) {
	var cmdargs []string
	var err error
	// TODO !!!!
	return cmdargs, err
}

// retileJobSplitter splitter for retile jobs
type retileJobSplitter struct {
	resources    config.Config
	nextJobIndex uint64
}

// NewRetileJobSplitter creates a retile job splitter
func NewRetileJobSplitter(resources config.Config) process.Splitter {
	return retileJobSplitter{resources: resources}
}

// SplitJob splits the job into multiple parallelizable jobs
func (s retileJobSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
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

	cmdlineBuilder, err := NewServiceCmdlineBuilder("local", "retile", s.resources)
	if err != nil {
		return err
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

// scaleJobSplitter splitter for scale jobs
type scaleJobSplitter struct {
	resources    config.Config
	nextJobIndex uint64
}

// NewScaleJobSplitter creates a scale job splitter
func NewScaleJobSplitter(resources config.Config) process.Splitter {
	return scaleJobSplitter{resources: resources}
}

// SplitJob splits the job into multiple parallelizable jobs
func (s scaleJobSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
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

	cmdlineBuilder, err := NewServiceCmdlineBuilder("local", "scale", s.resources)
	if err != nil {
		return err
	}

	for z := minZ; z < maxZ; z += processedZLayers {
		jobDepth := processedZLayers
		if z+jobDepth > maxZ {
			jobDepth = maxZ - z
		}
		newJobArgs := j.JArgs.Clone()

		newJobArgs.UpdateInt64Arg("image_width", scaleAttrs.totalVolume.dx)
		newJobArgs.UpdateInt64Arg("image_height", scaleAttrs.totalVolume.dy)
		newJobArgs.UpdateInt64Arg("image_depth", scaleAttrs.totalVolume.dz)

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

func setJvmMemory(cmdargs []string, jvmMemory string) []string {
	if jvmMemory != "" {
		cmdargs = arg.AddArg(cmdargs, "-Xms", jvmMemory, "")
		cmdargs = arg.AddArg(cmdargs, "-Xms", jvmMemory, "")
	}
	return cmdargs
}

func makeURL(base, path string) string {
	components := []string{
		strings.TrimRight(base, "/"),
		strings.Trim(path, "/"),
	}
	return strings.Join(components, "/")
}
