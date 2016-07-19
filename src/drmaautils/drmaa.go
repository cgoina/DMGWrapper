package drmaautils

import (
	"time"
)

// DRMAAProxy - drmaa proxy
type DRMAAProxy interface {
	CreateSession(name string) (DRMAASession, error)
}

// DRMAASession - drmaa session
type DRMAASession interface {
	RunJob(jt JobTemplate) (*JobInfo, error)
	UpdateJobInfo(j *JobInfo) error
	Close() error
}

// JobInfo - job info type
type JobInfo struct {
	Extension         `xml:"-" json:"-"`
	ID                string        `json:"id"`
	ExitStatus        int           `json:"exitStatus"`
	TerminatingSignal string        `json:"terminationSignal"`
	Annotation        string        `json:"annotation"`
	State             JobState      `json:"state"`
	SubState          string        `json:"subState"`
	AllocatedMachines []string      `json:"allocatedMachines"`
	SubmissionMachine string        `json:"submissionMachine"`
	JobOwner          string        `json:"jobOwner"`
	Slots             int64         `json:"slots"`
	QueueName         string        `json:"queueName"`
	WallclockTime     time.Duration `json:"wallockTime"`
	CPUTime           int64         `json:"cpuTime"`
	SubmissionTime    time.Time     `json:"submissionTime"`
	DispatchTime      time.Time     `json:"dispatchTime"`
	FinishTime        time.Time     `json:"finishTime"`
}

// JobState determines the DRMAA2 state of a job.
type JobState int

const (
	// Unset value not set
	Unset JobState = iota
	// Undetermined - unknown
	Undetermined
	// Queued - job queued
	Queued
	// QueuedHeld - job queueing is on hold
	QueuedHeld
	// Running - job running
	Running
	// Suspended - job was suspended
	Suspended
	// Requeued - job requeued
	Requeued
	// RequeuedHeld - job requeueing is on hold
	RequeuedHeld
	// Done - job completed successfully
	Done
	// Failed - job failed
	Failed
)

// String representation of a JobState
func (js JobState) String() string {
	switch js {
	case Undetermined:
		return "Undetermined"
	case Queued:
		return "Queued"
	case QueuedHeld:
		return "QueuedHeld"
	case Running:
		return "Running"
	case Suspended:
		return "Suspended"
	case Requeued:
		return "Requeued"
	case RequeuedHeld:
		return "RequeuedHeld"
	case Done:
		return "Done"
	case Failed:
		return "Failed"
	}
	return "Unset"
}

// JobTemplate is a representation of a job submission
type JobTemplate struct {
	Extension         `xml:"-" json:"-"`
	RemoteCommand     string            `json:"remoteCommand"`
	Args              []string          `json:"args"`
	SubmitAsHold      bool              `json:"submitAsHold"`
	ReRunnable        bool              `json:"reRunnable"`
	JobEnvironment    map[string]string `json:"jobEnvironment"`
	WorkingDirectory  string            `json:"workingDirectory"`
	JobCategory       string            `json:"jobCategory"`
	Email             []string          `json:"email"`
	EmailOnStarted    bool              `json:"emailOnStarted"`
	EmailOnTerminated bool              `json:"emailOnTerminated"`
	JobName           string            `json:"jobName"`
	InputPath         string            `json:"inputPath"`
	OutputPath        string            `json:"outputPath"`
	ErrorPath         string            `json:"errorPath"`
	JoinFiles         bool              `json:"joinFiles"`
	ReservationID     string            `json:"reservationId"`
	QueueName         string            `json:"queueName"`
	MinSlots          int64             `json:"minSlots"`
	MaxSlots          int64             `json:"maxSlots"`
	Priority          int64             `json:"priority"`
	CandidateMachines []string          `json:"candidateMachines"`
	MinPhysMemory     int64             `json:"minPhysMemory"`
	MachineOs         string            `json:"machineOs"`
	MachineArch       string            `json:"machineArch"`
	StartTime         time.Time         `json:"startTime"`
	DeadlineTime      time.Time         `json:"deadlineTime"`
	StageInFiles      map[string]string `json:"stageInFiles"`
	StageOutFiles     map[string]string `json:"stageOutFiles"`
	ResourceLimits    map[string]string `json:"resourceLimits"`
	AccountingID      string            `json:"accountingString"`
}

// SetExtension - sets a job extension
func (jt *JobTemplate) SetExtension(extension, value string) {
	if jt.ExtensionList == nil {
		jt.ExtensionList = make(map[string]string)
	}
	jt.ExtensionList[extension] = value
}

// GetExtension - returns a job extension
func (jt *JobTemplate) GetExtension(extension string) string {
	if jt.ExtensionList == nil {
		return ""
	}
	return jt.ExtensionList[extension]
}

// Extension struct which is embedded in DRMAA2 objects
// which are extensible.
type Extension struct {
	ExtensionList map[string]string // stores the extension requests as string
}
