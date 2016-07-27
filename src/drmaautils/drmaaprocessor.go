package drmaautils

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"config"
	"process"
)

const defaultJobTimeout = 10800

// GridJobInfo grid job info
type GridJobInfo struct {
	js              DRMAASession
	jt              *JobTemplate
	jobInfo         *JobInfo
	jobTimeoutInSec int64
}

// JobStdout get the job standard output
func (gji GridJobInfo) JobStdout() (io.ReadCloser, error) {
	var logDir string
	jobID := gji.jobInfo.ID
	if gji.jt.OutputPath != "" {
		logDir = strings.TrimSuffix(gji.jt.OutputPath, "/")
	} else {
		logDir = strings.TrimSuffix(gji.jt.WorkingDirectory, "/")
	}
	outputPattern := logDir + "/*.o" + jobID
	return gji.openJobOutputFile(outputPattern)
}

// JobStderr get the job standard error
func (gji GridJobInfo) JobStderr() (io.ReadCloser, error) {
	var logDir string
	jobID := gji.jobInfo.ID
	if gji.jt.ErrorPath != "" {
		logDir = strings.TrimSuffix(gji.jt.ErrorPath, "/")
	} else {
		logDir = strings.TrimSuffix(gji.jt.WorkingDirectory, "/")
	}
	outputPattern := logDir + "/*.e" + jobID
	return gji.openJobOutputFile(outputPattern)
}

func (gji GridJobInfo) openJobOutputFile(outputPattern string) (io.ReadCloser, error) {
	outputCandidates, err := filepath.Glob(outputPattern)
	if err != nil {
		return nil, fmt.Errorf("Invalid output pattern %s: %v", outputPattern, err)
	}
	if outputCandidates == nil {
		return nil, fmt.Errorf("No file found that matches %s", outputPattern)
	}
	if len(outputCandidates) > 1 {
		return nil, fmt.Errorf("Found more than one match for %s: %v", outputPattern, outputCandidates)
	}
	log.Printf("Opening %s", outputCandidates[0])
	return os.Open(outputCandidates[0])
}

// WaitForTermination wait for job's completion
func (gji GridJobInfo) WaitForTermination() (err error) {
	_, err = waitForState(gji.jobInfo, gji.js, Unset, gji.jobTimeoutInSec)
	return err
}

// GridProcessor processor that submits the job to the grid
type GridProcessor struct {
	process.JobWatcher
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
		return p, fmt.Errorf("Cannot create job session '%s' for %s", sessionName, accountingID)
	}
	return p, nil
}

// Run the given job
func (p *GridProcessor) Run(j process.Job) error {
	ji, err := p.Start(j)
	if err != nil {
		return fmt.Errorf("Error starting %v: %v", j, err)
	}
	return p.Wait(ji)
}

// Start submits a single job to the grid
func (p *GridProcessor) Start(j process.Job) (process.Info, error) {
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
	jt.RemoteCommand = j.Executable
	cmdline, err := j.CmdlineBuilder.GetCmdlineArgs(j.JArgs)
	if err != nil {
		return nil, err
	}
	jt.Args = make([]string, len(cmdline), len(cmdline))
	copy(jt.Args, cmdline)
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
	log.Printf("Submit (%d-%d) %s %s %v\n ", jt.MinSlots, jt.MaxSlots, j.Name, j.Executable, jt.Args)
	if jobInfo, err = p.js.RunJob(jt); err != nil {
		return nil, err
	}
	log.Printf("Submitted job %s\n", jobInfo.ID)
	_, err = waitForState(jobInfo, p.js, Running, jTimeout)
	gji := GridJobInfo{
		js:              p.js,
		jt:              &jt,
		jobInfo:         jobInfo,
		jobTimeoutInSec: jTimeout,
	}
	return gji, err
}

func waitForState(ji *JobInfo, js DRMAASession, desiredState JobState, waitTimeoutInSec int64) (bool, error) {
	quit := make(chan struct{})
	if waitTimeoutInSec > 0 {
		to := time.Duration(waitTimeoutInSec * int64(time.Second))
		time.AfterFunc(to, func() {
			var q struct{}
			quit <- q
		})
	}
	pollingInterval := time.Duration(30 * time.Second)
	for {
		select {
		case <-time.After(pollingInterval):
			jobStatus, err := checkJobState(ji, js)
			if err != nil {
				return false, fmt.Errorf("Error getting job %s status %v", ji.ID, err)
			}
			if desiredState != Unset && desiredState == jobStatus {
				return true, nil
			}
			switch jobStatus {
			case Done:
				return false, nil
			case Failed:
				return false, fmt.Errorf("Job %s failed", ji.ID)
			}
		case <-quit:
			return false, fmt.Errorf("Job %s timeout", ji.ID)
		}
	}
}

func checkJobState(ji *JobInfo, js DRMAASession) (JobState, error) {
	if err := js.UpdateJobInfo(ji); err != nil {
		return Undetermined, err
	}
	return ji.State, nil
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
