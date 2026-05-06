package model

import "time"

// LogStatus represents the execution result of a single trigger.
type LogStatus int8

const (
	LogInit    LogStatus = 0
	LogSuccess LogStatus = 1
	LogFail    LogStatus = 2
	LogRunning LogStatus = 3
	LogTimeout LogStatus = 4
	LogKilled  LogStatus = 5
)

// TriggerType records how a job was triggered.
type TriggerType int8

const (
	TriggerCron   TriggerType = 1
	TriggerManual TriggerType = 2
	TriggerRetry  TriggerType = 3
	TriggerChild  TriggerType = 4
)

// JobLog maps to job_log.
// Each row represents one trigger attempt of a job.
type JobLog struct {
	ID              int64       `gorm:"primaryKey;autoIncrement" json:"id"`
	JobID           int64       `gorm:"not null"                 json:"job_id"`
	ExecutorID      int64       `gorm:"not null"                 json:"executor_id"`
	ExecutorAddress string      `gorm:"size:128"                 json:"executor_address"`
	ExecuteParam    string      `gorm:"size:512"                 json:"execute_param"`
	Status          LogStatus   `gorm:"not null;default:0"       json:"status"`
	ErrorMsg        string      `gorm:"size:512"                 json:"error_msg"`
	ShardingIndex   int         `gorm:"default:0"                json:"sharding_index"`
	ShardingTotal   int         `gorm:"default:0"                json:"sharding_total"`
	TriggerTime     time.Time   `gorm:"not null"                 json:"trigger_time"`
	StartTime       *time.Time  `gorm:"column:start_time"        json:"start_time"`
	EndTime         *time.Time  `gorm:"column:end_time"          json:"end_time"`
	DurationMs      int64       `gorm:"default:0"                json:"duration_ms"`
	TriggerType     TriggerType `gorm:"not null;default:1"       json:"trigger_type"`
	CreateTime      time.Time   `gorm:"autoCreateTime"           json:"create_time"`
}

func (JobLog) TableName() string { return "job_log" }

// JobLogDetail maps to job_log_detail.
// Stores the full text output reported by the executor for a single run.
type JobLogDetail struct {
	ID         int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	LogID      int64     `gorm:"not null;uniqueIndex"     json:"log_id"`
	JobID      int64     `gorm:"not null"                 json:"job_id"`
	LogContent string    `gorm:"type:longtext"            json:"log_content"`
	CreateTime time.Time `gorm:"autoCreateTime"           json:"create_time"`
}

func (JobLogDetail) TableName() string { return "job_log_detail" }
