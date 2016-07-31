package process

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

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
	// Start the given job and returns as soon as it can without waiting for job's completion.
	// To check for the completion use Info's WaitForTermination method
	Start(j Job) (Info, error)
	// Run starts the given job and wait's until it completes.
	Run(j Job) error
}

// JobWatcher - implements a job watcher whose main job is to wait for job's completion
type JobWatcher struct {
}

// Wait method
func (w JobWatcher) Wait(ji Info) error {
	if err := ji.WaitForTermination(); err != nil {
		return fmt.Errorf("Processing error: %v", err)
	}
	return nil
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
	JobWatcher
}

// NewEchoProcessor creates a job processor that prints out the cmd line
func NewEchoProcessor() Processor {
	return &echoProcessor{}
}

// Run the given job
func (p *echoProcessor) Run(j Job) error {
	ji, err := p.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return p.Wait(ji)
}

// Start the given job
func (p *echoProcessor) Start(j Job) (Info, error) {
	cmdline, err := j.CmdlineBuilder.GetCmdlineArgs(j.JArgs)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Execute %v %v %v\n", j.Name, j.Executable, cmdline)

	return echoJobInfo{}, nil
}

// localCmdInfo local process info
type localCmdInfo struct {
	cmd       *exec.Cmd
	jobStdout io.ReadCloser
	jobStderr io.ReadCloser
}

func (lci *localCmdInfo) JobStdout() (io.ReadCloser, error) {
	return lci.jobStdout, nil
}

func (lci *localCmdInfo) JobStderr() (io.ReadCloser, error) {
	return lci.jobStderr, nil
}

func (lci *localCmdInfo) WaitForTermination() error {
	var donech chan struct{}
	var done struct{}
	donech = make(chan struct{})
	go func() {
		for {
			select {
			case <-time.After(500 * time.Millisecond):
				lci.readOutput()
			case <-donech:
				lci.readOutput()
				return
			}
		}
	}()
	lci.readOutput()
	err := lci.cmd.Wait()
	donech <- done
	return err
}

func (lci *localCmdInfo) readOutput() {
	io.Copy(os.Stdout, lci.jobStdout)
	io.Copy(os.Stderr, lci.jobStderr)
}

// localCmdProcessor - local command processor
type localCmdProcessor struct {
	JobWatcher
	Resources config.Config
}

// NewLocalCmdProcessor creates a local command job processor that processes the job on the current machine
// by creating and calling the corresponding command.
func NewLocalCmdProcessor(resources config.Config) Processor {
	return &localCmdProcessor{Resources: resources}
}

// Run the given job
func (p *localCmdProcessor) Run(j Job) error {
	ji, err := p.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return p.Wait(ji)
}

// Start launches the server
func (p *localCmdProcessor) Start(j Job) (Info, error) {
	cmdargs, err := j.CmdlineBuilder.GetCmdlineArgs(j.JArgs)
	if err != nil {
		return nil, fmt.Errorf("Error preparing the command line arguments: %v", err)
	}
	cmd := exec.Command(j.Executable, cmdargs...)
	log.Printf("Execute %v\n", cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("Error opening the command stdout: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("Error opening the command stderr: %v", err)
	}
	lci := &localCmdInfo{
		cmd:       cmd,
		jobStdout: stdout,
		jobStderr: stderr,
	}
	err = cmd.Start()
	return lci, err
}

// parallelProcessor is responsible with splitting a job into multiple smaller jobs
// and processing them in parallel
type parallelProcessor struct {
	JobWatcher
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

// Run the given job
func (p *parallelProcessor) Run(j Job) error {
	ji, err := p.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return p.Wait(ji)
}

// Start the given job
func (p *parallelProcessor) Start(j Job) (Info, error) {
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
			// run the job
			log.Printf("Run Job: %v", j)
			if err := w.jp.Run(j); err != nil {
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
