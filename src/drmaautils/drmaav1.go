package drmaautils

import (
	"bytes"
	"fmt"
	"github.com/dgruber/drmaa"
	"log"
	"os"
	"strconv"
)

// DRMAAV1Proxy - drmaa1 proxy
type DRMAAV1Proxy struct {
}

// DRMAAV1Session - drmaa1 session
type DRMAAV1Session struct {
	js *drmaa.Session
}

// NewDRMAAV1Proxy create a new drmaa2 proxy
func NewDRMAAV1Proxy() DRMAAProxy {
	return &DRMAAV1Proxy{}
}

var drmaaV1Session DRMAASession

// CreateSession DRMAAProxy method
func (d1p *DRMAAV1Proxy) CreateSession(name string) (DRMAASession, error) {
	var js drmaa.Session
	var err error

	if drmaaV1Session != nil {
		return drmaaV1Session, nil
	}
	sessionName := "session=" + name
	if err = js.Init(sessionName); err != nil {
		drmaaErr := err.(*drmaa.Error)
		switch drmaaErr.ID {
		case drmaa.AlreadyActiveSession:
			log.Printf("A DRMAA session is already active, continue using the current session")
			break
		default:
			return nil, fmt.Errorf("Could not open DRMAA v1 session: %v:%v", drmaaErr.ID, err)
		}
	}
	drmaaV1Session = &DRMAAV1Session{&js}
	return drmaaV1Session, nil
}

// RunJob DRMAASession method
func (d1s *DRMAAV1Session) RunJob(jt JobTemplate) (*JobInfo, error) {
	djt, err := d1s.js.AllocateJobTemplate()
	if err != nil {
		return nil, fmt.Errorf("Error allocating a new job template: %v", err)
	}
	defer d1s.js.DeleteJobTemplate(&djt)
	if err := djt.SetJobName(jt.JobName); err != nil {
		return nil, fmt.Errorf("Error during SetJobName %s: %v", jt.JobName, err)
	}
	if err = djt.SetRemoteCommand(jt.RemoteCommand); err != nil {
		return nil, fmt.Errorf("Error setting remote command %s: %v", jt.RemoteCommand, err)
	}
	if err = djt.SetArgs(jt.Args); err != nil {
		return nil, fmt.Errorf("Error setting args %v: %v", jt.Args, err)
	}
	if err = djt.SetWD(jt.WorkingDirectory); err != nil {
		return nil, fmt.Errorf("Error setting working directory %v: %v", jt.WorkingDirectory, err)
	}
	if jt.InputPath != "" {
		if err = djt.SetInputPath(":" + jt.InputPath); err != nil {
			return nil, fmt.Errorf("Error setting input directory %v: %v", jt.InputPath, err)
		}
	}
	if jt.OutputPath != "" {
		if err = os.MkdirAll(jt.OutputPath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("Error creating output directory %v: %v", jt.OutputPath, err)
		}
		if err = djt.SetOutputPath(":" + jt.OutputPath); err != nil {
			return nil, fmt.Errorf("Error setting output directory %v: %v", jt.OutputPath, err)
		}
	}
	if jt.ErrorPath != "" {
		if err = os.MkdirAll(jt.ErrorPath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("Error creating error directory %v: %v", jt.ErrorPath, err)
		}
		if err = djt.SetErrorPath(":" + jt.ErrorPath); err != nil {
			return nil, fmt.Errorf("Error setting error directory %v: %v", jt.ErrorPath, err)
		}
	}
	var nativeSpecBuffer bytes.Buffer
	appendAccountID(&nativeSpecBuffer, jt)
	appendPE(&nativeSpecBuffer, jt)
	appendQueue(&nativeSpecBuffer, jt)
	appendResourceLimits(&nativeSpecBuffer, jt)

	nativeSpec := nativeSpecBuffer.String()
	if nativeSpec != "" {
		if err = djt.SetNativeSpecification(nativeSpec); err != nil {
			return nil, fmt.Errorf("Error setting args %v: %v", jt.Args, err)
		}
	}

	jobID, err := d1s.js.RunJob(&djt)
	if err != nil {
		return nil, fmt.Errorf("Error submitting job %v", err)
	}
	return &JobInfo{ID: jobID}, nil
}

func appendAccountID(buf *bytes.Buffer, jt JobTemplate) {
	if jt.AccountingID == "" {
		return
	}
	buf.WriteString("-A ")
	buf.WriteString(jt.AccountingID)
	buf.WriteString(" ")
}

func appendPE(buf *bytes.Buffer, jt JobTemplate) {
	pe := jt.GetExtension("uge_jt_pe")
	if pe == "" {
		return
	}
	if jt.MinSlots == 0 && jt.MaxSlots == 0 {
		return
	}
	buf.WriteString("-pe ")
	buf.WriteString(pe)
	buf.WriteString(" ")
	if jt.MinSlots > 0 {
		buf.WriteString(strconv.FormatInt(jt.MinSlots, 10))
	}
	if jt.MaxSlots > 0 {
		buf.WriteString("-")
		buf.WriteString(strconv.FormatInt(jt.MaxSlots, 10))
	}
	buf.WriteString(" ")
	return
}

func appendQueue(buf *bytes.Buffer, jt JobTemplate) {
	if jt.QueueName == "" {
		return
	}
	buf.WriteString("-q ")
	buf.WriteString(jt.QueueName)
	buf.WriteString(" ")
}

func appendResourceLimits(buf *bytes.Buffer, jt JobTemplate) {
	jobResourceLimits := jt.ResourceLimits
	if len(jobResourceLimits) == 0 {
		return
	}
	buf.WriteString("-l ")
	lIndex := 0
	for k, v := range jobResourceLimits {
		if lIndex > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v)
		lIndex++
	}
	buf.WriteString(" ")
}

// Close DRMAASession method. So far it has not been an issue with multiple proxies closing the session
// while it's being used by another proxy. If this happens we need some counter to track the number of times
// the session is "created"
func (d1s *DRMAAV1Session) Close() error {
	err := d1s.js.Exit()
	drmaaV1Session = nil
	return err
}

// UpdateJobInfo DRMAASession method
func (d1s *DRMAAV1Session) UpdateJobInfo(j *JobInfo) error {
	state, err := d1s.js.JobPs(j.ID)
	if err != nil {
		j.State = Undetermined
		return err
	}
	j.State = convertPsToDRMAAState(state)
	return nil
}

// convertPsToDRMAAState converts DRMAA v1 state to JobState
func convertPsToDRMAAState(ds drmaa.PsType) JobState {
	switch ds {
	case drmaa.PsUndetermined:
		return Undetermined
	case drmaa.PsQueuedActive:
		return Queued
	case drmaa.PsSystemOnHold:
		return QueuedHeld
	case drmaa.PsUserOnHold:
		return QueuedHeld
	case drmaa.PsUserSystemOnHold:
		return QueuedHeld
	case drmaa.PsRunning:
		return Running
	case drmaa.PsSystemSuspended:
		return Suspended
	case drmaa.PsUserSuspended:
		return Suspended
	case drmaa.PsUserSystemSuspended:
		return Suspended
	case drmaa.PsDone:
		return Done
	case drmaa.PsFailed:
		return Failed
	}
	return Undetermined
}
