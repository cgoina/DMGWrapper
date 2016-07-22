package process

import (
	"io"
	"fmt"
	"log"
	"os"

	"arg"
	"config"
)

// Job - mipmaps job
type Job struct {
	// Executable is the job's executable program
	Executable string
	// Name job name
	Name string
	// JArgs job arguments
	JArgs arg.Args
	// CmdlineBuilder command line builder
	CmdlineBuilder arg.CmdlineArgBuilder
}

// Info descriptor
type Info interface {
	JobStdout() (io.ReadCloser, error)
	JobStderr() (io.ReadCloser, error)
	WaitForTermination() error
}

// Processor is responsible with processing a single job
type Processor interface {
	Process(j Job) (Info, error)
}

// echoJobInfo echo job info
type echoJobInfo struct {
}

// JobStdout an echo job's standard output
func (ej echoJobInfo) JobStdout() (io.ReadCloser, error) {
	return os.Stdout, nil
}

// JobStderr an echo job's standard error
func (ej echoJobInfo) JobStderr() (io.ReadCloser, error) {
	return os.Stderr, nil
}

// WaitForTermination wait for job's completion
func (ej echoJobInfo) WaitForTermination() error {
	return nil
}

// echoProcessor is a processor that simply outputs the command line
type echoProcessor struct {
}

// NewEchoProcessor creates a job processor that prints out the cmd line
func NewEchoProcessor() Processor {
	return &echoProcessor{}
}

// Process the given job
func (p *echoProcessor) Process(j Job) (Info, error) {
	cmdline, err := j.CmdlineBuilder.GetCmdlineArgs(j.JArgs)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Execute %v %v\n", j.Name, cmdline)

	return echoJobInfo{}, nil
}

// parallelProcessor is responsible with splitting a job into multiple smaller jobs
// and processing them in parallel
type parallelProcessor struct {
	resources    config.Config
	jobProcessor Processor
	nextJobIndex uint64
	jobSplitter  Splitter
}

// Splitter object which know how to split a job for the parallel processor
type Splitter interface {
	SplitJob(j Job, jch chan<- Job) error
}

// NewParallelProcessor creates a new job processor that will process the job by
// first splitting it into multiple smaller jobs and than apply the given subJob processor.
func NewParallelProcessor(jobProcessor Processor, jobSplitter Splitter, resources config.Config) Processor {
	return &parallelProcessor{
		resources:    resources,
		jobProcessor: jobProcessor,
		nextJobIndex: 1,
		jobSplitter:  jobSplitter,
	}
}

// parallelJob information about a parallel job
type parallelJob struct {
	done chan struct{}
}

// JobStdout a parallel job's standard output
func (pj parallelJob) JobStdout() (io.ReadCloser, error) {
	return os.Stdout, nil
}

// JobStderr a parallel job's standard error
func (pj parallelJob) JobStderr() (io.ReadCloser, error) {
	return os.Stderr, nil
}

// WaitForTermination wait for job's completion
func (pj parallelJob) WaitForTermination() error {
	<-pj.done
	return nil
}

// Process the given job
func (p *parallelProcessor) Process(j Job) (Info, error) {
	maxRunningJobs := p.resources.GetIntProperty("maxRunningJobs")
	if maxRunningJobs <= 0 {
		maxRunningJobs = 1
	}
	workerPool := make(chan *processWorker, maxRunningJobs)

	jobInfo := parallelJob{
		done: make(chan struct{}, 1),
	}

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
		jobInfo.done <- done
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

	return jobInfo, processingErr
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
func (p *parallelProcessor) splitJob(j Job) chan Job {
	jobQueueSize := p.resources.GetIntProperty("jobQueueSize")
	jch := make(chan Job, jobQueueSize)
	go func() {
		p.jobSplitter.SplitJob(j, jch)
		close(jch)
	}()
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
				if err = jobInfo.WaitForTermination(); err != nil {
					log.Println(err)
					w.setError(err)
				}
			} else {
				log.Println(err)
				w.setError(err)
			}
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
