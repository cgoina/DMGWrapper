package drmaautils

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"config"
	"job"
)

const defaultJobTimeout = 10800

// GridProcessor processor that submits the job to the grid
type GridProcessor struct {
	sessionName  string
	accountingID string
	resources    config.Config
	dp           DRMAAProxy
	js           DRMAASession
}

// NewGridProcessor creates a grid processor
func NewGridProcessor(sessionName, accountingID string, drmaaProxy DRMAAProxy, resources config.Config) (p *GridProcessor, err error) {
	p = &GridProcessor{
		sessionName:  sessionName,
		accountingID: accountingID,
		resources:    resources,
		dp:           drmaaProxy,
	}
	if p.js, err = p.dp.CreateSession(sessionName); err != nil {
		return p, fmt.Errorf("Cannot create job session '%s'", sessionName)
	}
	return p, nil
}

// GridJobInfo grid job info
type GridJobInfo struct {
	js              DRMAASession
	jt              *JobTemplate
	jobInfo         *JobInfo
	jobTimeoutInSec int64
}

func (gji GridJobInfo) JobStdout() (io.ReadCloser, error) {
	// !!!!! TODO
	return nil, nil
}

func (gji GridJobInfo) JobStderr() (io.ReadCloser, error) {
	// !!!!! TODO
	return nil, nil
}

func (gji GridJobInfo) WaitForTermination() error {
	quit := make(chan struct{})
	if gji.jobTimeoutInSec > 0 {
		to := time.Duration(gji.jobTimeoutInSec * int64(time.Second))
		time.AfterFunc(to, func() {
			var q struct{}
			quit <- q
		})
	}
	pollingInterval := time.Duration(30 * time.Second)
	for {
		select {
		case <-time.After(pollingInterval):
			jobStatus, err := checkJobState(gji.jobInfo, gji.js)
			if err != nil {
				return fmt.Errorf("Error getting job %s status %v", gji.jobInfo.ID, err)
			}
			switch jobStatus {
			case Done:
				return nil
			case Failed:
				return fmt.Errorf("Job %s failed", gji.jobInfo.ID)
			}
		case <-quit:
			return fmt.Errorf("Job %s timeout", gji.jobInfo.ID)
		}
	}
}

// Process submits a single job to the grid
func (p *GridProcessor) Process(j job.Job) (job.JobInfo, error) {
	var (
		jt       JobTemplate
		jobInfo  *JobInfo
		err      error
		jTimeout int64 // job timeout
	)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Panic while processing job %s, %v: %r", jt.RemoteCommand, jt.Args, r)
		}
	}()
	jt.RemoteCommand = p.resources.GetStringProperty("mipmapsExec")
	cmdline := j.JArgs.GetCmdline()
	jt.Args = make([]string, len(cmdline)+1, len(cmdline)+1)
	jt.Args[0] = string(j.Action)
	copy(jt.Args[1:], cmdline)
	jt.JobName = j.Name
	jt.AccountingID = p.accountingID
	workingDirectory := p.resources.GetStringProperty("workingDir")
	if workingDirectory != "" {
		jt.WorkingDirectory = workingDirectory
	} else if jt.WorkingDirectory, err = os.Getwd(); err != nil {
		log.Printf("Error retrieving the working directory: %v", err)
		jt.WorkingDirectory = "."
	}
	jt.QueueName = p.resources.GetStringProperty("ugeQueue")
	jt.MinSlots = p.resources.GetInt64Property("ugeMinSlots")
	jt.MaxSlots = p.resources.GetInt64Property("ugeMaxSlots")
	jt.ResourceLimits = p.resources.GetStringMapProperty("ugeResources")
	jt.JobEnvironment = p.resources.GetStringMapProperty("ugeJobEnvironment")
	jt.OutputPath = p.resources.GetStringProperty("outputDir")
	jt.ErrorPath = p.resources.GetStringProperty("errorDir")

	if jTimeout = p.resources.GetInt64Property("jobTimeout"); jTimeout == 0 {
		jTimeout = defaultJobTimeout
	}

	// Submit the Job
	jt.SetExtension("uge_jt_pe", p.resources.GetStringProperty("ugeParallelEnvironment"))
	log.Printf("Submit (%d-%d) %s %s %v\n ", jt.MinSlots, jt.MaxSlots, j.Name, j.Action, jt.Args)
	if jobInfo, err = p.js.RunJob(jt); err != nil {
		return nil, err
	}
	log.Printf("Submitted job %s\n", jobInfo.ID)
	gji := GridJobInfo{
		js:              p.js,
		jt:              &jt,
		jobInfo:         jobInfo,
		jobTimeoutInSec: jTimeout,
	}
	return gji, err
}

func checkJobState(job *JobInfo, js DRMAASession) (JobState, error) {
	if err := js.UpdateJobInfo(job); err != nil {
		return Undetermined, err
	}
	return job.State, nil
}

// CloseSession close the processing session
func (p *GridProcessor) CloseSession() error {
	log.Println("Close session ", p.sessionName)
	if closeJsErr := p.js.Close(); closeJsErr != nil {
		log.Printf("Close session error %v", closeJsErr)
		return closeJsErr
	}
	return nil
}
