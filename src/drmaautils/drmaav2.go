package drmaautils

import (
	"fmt"
	"github.com/dgruber/drmaa2"
	"log"
)

// DRMAAV2Proxy - drmaa2 proxy
type DRMAAV2Proxy struct {
	sm *drmaa2.SessionManager
}

// DRMAAV2Session - drmaa2 session
type DRMAAV2Session struct {
	js *drmaa2.JobSession
}

// NewDRMAAV2Proxy create a new drmaa2 proxy
func NewDRMAAV2Proxy() DRMAAProxy {
	return &DRMAAV2Proxy{
		sm: &drmaa2.SessionManager{},
	}
}

// CreateSession - DRMAAProxy implementation method
// For DRMAA2 it tries to open session with the given name.
// If such session does not exist it creates a new one.
func (d2p *DRMAAV2Proxy) CreateSession(name string) (DRMAASession, error) {
	var js *drmaa2.JobSession
	var err error

	if js, err = d2p.sm.OpenJobSession(name); err != nil {
		log.Println("Could not open session ", name, " - will try to create one")
		if js, err = d2p.sm.CreateJobSession(name, ""); err != nil {
			return nil, err
		}
	}
	return &DRMAAV2Session{js}, nil
}

// RunJob DRMAASession method
func (d2s *DRMAAV2Session) RunJob(jt JobTemplate) (*JobInfo, error) {
	d2jt := convertToV2Template(jt)
	var job *drmaa2.Job
	var err error
	if job, err = d2s.js.RunJob(d2jt); err != nil {
		return nil, err
	}
	return &JobInfo{ID: job.GetId()}, nil
}

// Close DRMAASession method
func (d2s *DRMAAV2Session) Close() error {
	return d2s.js.Close()
}

// UpdateJobInfo DRMAASession method
func (d2s *DRMAAV2Session) UpdateJobInfo(j *JobInfo) (err error) {
	filter := drmaa2.CreateJobInfo()
	filter.Id = j.ID
	var jobs []drmaa2.Job
	if jobs, err = d2s.js.GetJobs(&filter); err != nil {
		return err
	}
	if len(jobs) == 0 {
		j.State = Undetermined
		return fmt.Errorf("No job %s found", j.ID)
	}
	jobInfo, _ := jobs[0].GetJobInfo()
	// for now only map the state
	j.State = (JobState)(jobInfo.State)
	return nil
}

func convertToV2Template(jt JobTemplate) (v2jt drmaa2.JobTemplate) {
	v2jt.RemoteCommand = jt.RemoteCommand
	v2jt.Args = make([]string, len(jt.Args), len(jt.Args))
	copy(v2jt.Args, jt.Args)
	v2jt.SubmitAsHold = jt.SubmitAsHold
	v2jt.ReRunnable = jt.ReRunnable
	v2jt.JobEnvironment = copyMap(jt.JobEnvironment)
	v2jt.WorkingDirectory = jt.WorkingDirectory
	v2jt.JobCategory = jt.JobCategory
	v2jt.Email = make([]string, len(jt.Email), len(jt.Email))
	copy(v2jt.Email, jt.Email)
	v2jt.EmailOnStarted = jt.EmailOnStarted
	v2jt.EmailOnTerminated = jt.EmailOnTerminated
	v2jt.JobName = jt.JobName
	v2jt.InputPath = jt.InputPath
	v2jt.OutputPath = jt.OutputPath
	v2jt.ErrorPath = jt.ErrorPath
	v2jt.JoinFiles = jt.JoinFiles
	v2jt.ReservationId = jt.ReservationID
	v2jt.QueueName = jt.QueueName
	v2jt.MaxSlots = jt.MaxSlots
	v2jt.MinSlots = jt.MinSlots
	v2jt.Priority = jt.Priority
	v2jt.CandidateMachines = make([]string, len(jt.CandidateMachines), len(jt.CandidateMachines))
	copy(v2jt.CandidateMachines, jt.CandidateMachines)
	v2jt.MinPhysMemory = jt.MinPhysMemory
	v2jt.MachineOs = jt.MachineOs
	v2jt.MachineArch = jt.MachineArch
	v2jt.StartTime = jt.StartTime
	v2jt.DeadlineTime = jt.DeadlineTime
	v2jt.StageInFiles = copyMap(jt.StageInFiles)
	v2jt.StageOutFiles = copyMap(jt.StageOutFiles)
	v2jt.ResourceLimits = copyMap(jt.ResourceLimits)
	v2jt.AccountingId = jt.AccountingID
	return v2jt
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
