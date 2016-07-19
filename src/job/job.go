package job

import (
	"fmt"
	"io"
	"log"

	"config"
)

// ActionType defines the action to be performed
type ActionType string

// Job - mipmaps job
type Job struct {
	// Action defines the job's action
	Action ActionType
	// Name job name
	Name string
	// JArgs job arguments
	JArgs Args
}

// JobInfo descriptor
type JobInfo interface {
	StdoutPipe() (io.ReadCloser, error)
	WaitForTermination() error
}

// Processor is responsible with processing a single job
type Processor interface {
	Process(j Job) (JobInfo, error)
}

// ParallelProcessor is responsible with splitting a job into multiple smaller jobs
// and processing them in parallel
type ParallelProcessor struct {
	resources    config.Config
	jobProcessor Processor
	nextJobIndex uint64
	jobSplitter  Splitter
}

// Splitter object which know how to split a job for the parallel processor
type Splitter interface {
	SplitJob(j Job, jch chan<- Job)
}

// NewParallelProcessor creates a new job processor that will process the job by
// first splitting it into multiple smaller jobs and than apply the given subJob processor.
func NewParallelProcessor(jobProcessor Processor, jobSplitter Splitter, resources config.Config) Processor {
	return &ParallelProcessor{
		resources:    resources,
		jobProcessor: jobProcessor,
		nextJobIndex: 1,
		jobSplitter:  jobSplitter,
	}
}

// ParallelJobInfo information about a parallel job
type ParallelJob struct {
}

func (pj ParallelJob) StdoutPipe() (io.ReadCloser, error) {
	return nil, fmt.Errorf("Method not supported")
}

func (pj ParallelJob) WaitForTermination() error {
	return nil
}

// Process the given job
func (p *ParallelProcessor) Process(j Job) (JobInfo, error) {
	maxRunningJobs := p.resources.GetIntProperty("maxRunningJobs")
	if maxRunningJobs <= 0 {
		maxRunningJobs = 1
	}
	workerPool := make(chan *processWorker, maxRunningJobs)

	stopWorkers := func() {
		var done struct{}

		for i := 0; i < maxRunningJobs; i++ {
			w, ok := <-workerPool
			if !ok {
				continue
			}
			w.quit <- done
			<-w.done
		}
	}

	defer stopWorkers()

	for i := 0; i < maxRunningJobs; i++ {
		quit := make(chan struct{}, 1)
		errc := make(chan error, 1)

		startWorker(errc, quit, workerPool, p.jobProcessor)
	}
	jch := p.splitJob(j)

	var processingErr error
	for sj := range jch {
		w := <-workerPool
		w.jobQueue <- sj
		if jobError := w.getError(); jobError != nil {
			processingErr = jobError
		}
	}

	return ParallelJob{}, processingErr
}

type processWorker struct {
	jobQueue chan Job
	pool     chan *processWorker
	errc     chan error
	quit     chan struct{}
	done     chan bool
	jp       Processor
}

func startWorker(errc chan error, quit chan struct{}, pool chan *processWorker, jp Processor) {
	w := &processWorker{
		jobQueue: make(chan Job),
		pool:     pool,
		errc:     errc,
		quit:     quit,
		done:     make(chan bool, 1),
		jp:       jp,
	}
	go w.run()
}

// splitJob splits the job into multiple smaller jobs
func (p *ParallelProcessor) splitJob(j Job) chan Job {
	jobQueueSize := p.resources.GetIntProperty("jobQueueSize")
	jch := make(chan Job, jobQueueSize)
	go p.jobSplitter.SplitJob(j, jch)
	return jch
}

func (w *processWorker) run() {
	done := false
	defer func() {
		recover()
		if !done {
			done = true
			w.done <- done
		}
	}()
	for !done {
		// tell the dispatcher that this worker is ready to accept more work
		w.pool <- w
		select {
		case j, ok := <-w.jobQueue:
			if !ok {
				done = true
				w.done <- done
				return
			}
			// process the job
			log.Printf("Process Job: %v", j)
			jobInfo, err := w.jp.Process(j)
			if err == nil {
				err = jobInfo.WaitForTermination()
			}
			log.Println(err)
			w.setError(err)
		case <-w.quit:
			// done
			done = true
			w.done <- done
			return
		}
	}
}

func (w *processWorker) setError(err error) {
	select {
	case w.errc <- err:
		return
	default:
		return
	}
}

func (w *processWorker) getError() (err error) {
	select {
	case err = <-w.errc:
		return
	default:
		return
	}
}
