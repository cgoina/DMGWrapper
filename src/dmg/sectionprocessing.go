package dmg

import (
	"process"
)

// sectionSplitter splits a section into multiple bands.
type sectionSplitter struct {
}

// SplitJob splits the job into multiple parallelizable jobs
func (s sectionSplitter) SplitJob(j process.Job, jch chan<- process.Job) error {
	// TODO !!!!
	return nil
}

// NewSectionSplitter creates a new section splitter
func NewSectionSplitter() process.Splitter {
	return sectionSplitter{}
}
