package dmg

import (
	"flag"
	"fmt"

	"arg"
)

// Attrs registers DMG client and server attributes
type Attrs struct {
	Configs          arg.StringList
	helpFlag         bool
	serverAddress    string
	serverPort       int
	nSections        int
	iterations       int
	vCycles          int
	iWeight          float64
	gWeight          float64
	gScale           float64
	nThreads         int
	verbose          bool
	gray             bool
	deramp           bool
	tileExt          string
	tileWidth        int
	tileHeight       int
	clientIndex      int
	minZ, maxZ       int64
	sourcePixelsList arg.StringList
	sourceLabelsList arg.StringList
	destImgList      arg.StringList
	sourcePixels     string
	sourceLabels     string
	destImg          string
	scratchDir       string
	targetDir        string
	coordFile        string
}

// Name method
func (a *Attrs) Name() string {
	return "dmg"
}

// DefineArgs method
func (a *Attrs) DefineArgs(fs *flag.FlagSet) {
	fs.Var(&a.Configs, "config", "list of configuration files which applied in the order they are specified")
	fs.IntVar(&a.nSections, "sections", 1, "Number of sections processed in parallel")
	fs.IntVar(&a.iterations, "iters", 5, "Number of Gauss-Siebel iterations")
	fs.IntVar(&a.vCycles, "vCycles", 1, "Number of V-cycles")
	fs.Float64Var(&a.iWeight, "iWeight", 0, "Value interpolation weight")
	fs.Float64Var(&a.gWeight, "gWeight", 1, "Gradient interpolation weight")
	fs.Float64Var(&a.gScale, "gScale", 1, "Gradient scale")
	fs.StringVar(&a.serverAddress, "serverAddress", "", "DMG server address - host[:port]")
	fs.IntVar(&a.serverPort, "serverPort", 0, "DMG server port")
	fs.BoolVar(&a.verbose, "verbose", false, "verbosity flag")
	fs.BoolVar(&a.gray, "gray", true, "gray image flag")
	fs.BoolVar(&a.deramp, "deramp", true, "deramp flag")
	fs.IntVar(&a.tileWidth, "tileWidth", 8192, "Tile width")
	fs.IntVar(&a.tileHeight, "tileHeight", 8192, "Tile height")
	fs.StringVar(&a.tileExt, "tileExt", "png", "Destination image extension")
	fs.BoolVar(&a.helpFlag, "h", false, "gray image flag")
	fs.IntVar(&a.clientIndex, "clientIndex", 0, "Client index")
	fs.IntVar(&a.nThreads, "threads", 1, "Number of threads")
	fs.Int64Var(&a.minZ, "minZ", 0, "Min Z")
	fs.Int64Var(&a.maxZ, "maxZ", 0, "Max Z (inclusive)")
	fs.Var(&a.sourcePixelsList, "pixelsList", "List of image pixels")
	fs.Var(&a.sourceLabelsList, "labelsList", "List of image labels")
	fs.Var(&a.destImgList, "outList", "List of output images")
	fs.StringVar(&a.sourcePixels, "pixels", "", "Source image pixels")
	fs.StringVar(&a.sourceLabels, "labels", "", "Source image labels")
	fs.StringVar(&a.destImg, "out", "", "Output image")
	fs.StringVar(&a.scratchDir, "temp", "/var/tmp", "Scratch directory")
	fs.StringVar(&a.targetDir, "targetDir", "", "Destination directory")
	fs.StringVar(&a.coordFile, "coordFile", "offset.json", "Coordinates file")
}

// IsHelpFlagSet method
func (a *Attrs) IsHelpFlagSet() bool {
	return a.helpFlag
}

// validate arguments
func (a *Attrs) validate() error {
	nImages := len(a.sourcePixelsList)
	if len(a.sourceLabelsList) != nImages {
		return fmt.Errorf("PixelsList and LabelsList must have the same length")
	}
	if len(a.destImgList) != nImages {
		return fmt.Errorf("PixelsList and Output images must have the same length")
	}
	if nImages == 0 {
		if a.sourcePixels == "" {
			return fmt.Errorf("No source pixels has been defined")
		}
		if a.sourceLabels == "" {
			return fmt.Errorf("No source labels has been defined")
		}
		if a.destImg == "" {
			return fmt.Errorf("No destination image has been defined")
		}
		if a.nSections > 1 {
			return fmt.Errorf("The number of sections must be equal to the number of source images")
		}
		return nil
	}
	if a.nSections <= 0 {
		return fmt.Errorf("Invalid number of serctions %d", a.nSections)
	}
	if nImages != a.nSections {
		return fmt.Errorf("The number of sections must be equal to the number of source images")
	}
	for i := 0; i < nImages; i++ {
		sourcePixels := a.sourcePixelsList[i]
		sourceLabels := a.sourceLabelsList[i]
		destImage := a.destImgList[i]
		if sourcePixels == "" {
			return fmt.Errorf("Pixels image not defined at index %d", i)
		}
		if sourceLabels == "" {
			return fmt.Errorf("Labels image not defined at index %d", i)
		}
		if destImage == "" {
			return fmt.Errorf("Output image not defined at index %d", i)
		}
	}
	return nil
}

// extractDmgAttrs populates dmg attributes from command line flags
func (a *Attrs) extractDmgAttrs(ja *arg.Args) (err error) {
	if a.Configs, err = ja.GetStringListArgValue("config"); err != nil {
		return err
	}
	if a.serverAddress, err = ja.GetStringArgValue("serverAddress"); err != nil {
		return err
	}
	if a.serverPort, err = ja.GetIntArgValue("serverPort"); err != nil {
		return err
	}
	if a.nSections, err = ja.GetIntArgValue("sections"); err != nil {
		return err
	}
	if a.iterations, err = ja.GetIntArgValue("iters"); err != nil {
		return err
	}
	if a.vCycles, err = ja.GetIntArgValue("vCycles"); err != nil {
		return err
	}
	if a.iWeight, err = ja.GetFloat64ArgValue("iWeight"); err != nil {
		return err
	}
	if a.gWeight, err = ja.GetFloat64ArgValue("gWeight"); err != nil {
		return err
	}
	if a.gScale, err = ja.GetFloat64ArgValue("gScale"); err != nil {
		return err
	}
	if a.verbose, err = ja.GetBoolArgValue("verbose"); err != nil {
		return err
	}
	if a.gray, err = ja.GetBoolArgValue("gray"); err != nil {
		return err
	}
	if a.deramp, err = ja.GetBoolArgValue("deramp"); err != nil {
		return err
	}
	if a.tileWidth, err = ja.GetIntArgValue("tileWidth"); err != nil {
		return err
	}
	if a.tileHeight, err = ja.GetIntArgValue("tileHeight"); err != nil {
		return err
	}
	if a.tileExt, err = ja.GetStringArgValue("tileExt"); err != nil {
		return err
	}
	if a.clientIndex, err = ja.GetIntArgValue("clientIndex"); err != nil {
		return err
	}
	if a.nThreads, err = ja.GetIntArgValue("threads"); err != nil {
		return err
	}
	if a.sourcePixels, err = ja.GetStringArgValue("pixels"); err != nil {
		return err
	}
	if a.sourceLabels, err = ja.GetStringArgValue("labels"); err != nil {
		return err
	}
	if a.destImg, err = ja.GetStringArgValue("out"); err != nil {
		return err
	}
	if a.minZ, err = ja.GetInt64ArgValue("minZ"); err != nil {
		return err
	}
	if a.maxZ, err = ja.GetInt64ArgValue("maxZ"); err != nil {
		return err
	}
	if a.sourcePixelsList, err = ja.GetStringListArgValue("pixelsList"); err != nil {
		return err
	}
	if a.sourceLabelsList, err = ja.GetStringListArgValue("labelsList"); err != nil {
		return err
	}
	if a.destImgList, err = ja.GetStringListArgValue("outList"); err != nil {
		return err
	}
	if a.scratchDir, err = ja.GetStringArgValue("temp"); err != nil {
		return err
	}
	if a.targetDir, err = ja.GetStringArgValue("targetDir"); err != nil {
		return err
	}
	if a.coordFile, err = ja.GetStringArgValue("coordFile"); err != nil {
		return err
	}
	return nil
}
