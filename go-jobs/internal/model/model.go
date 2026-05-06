// Package model defines all database models for go-jobs.
// GORM tags are used for column mapping; JSON tags for API serialisation.
package model

import (
	"time"
)

// ─── Executor ─────────────────────────────────────────────────────────────────

// ExecutorStatus enumerates the online/offline states of an executor.
type ExecutorStatus int8

const (
	ExecutorOffline ExecutorStatus = 0
	ExecutorOnline  ExecutorStatus = 1
)

// RegisterType indicates how an executor was registered.
type RegisterType int8

const (
	RegisterAuto   RegisterType = 0 // executor auto-registers via heartbeat
	RegisterManual RegisterType = 1 // manually added in admin UI
)

// Executor maps to sys_executor.
type Executor struct {
	ID            int64          `gorm:"primaryKey;autoIncrement" json:"id"`
	AppName       string         `gorm:"size:64;not null"         json:"app_name"`
	Title         string         `gorm:"size:64"                  json:"title"`
	Address       string         `gorm:"size:128;not null"        json:"address"`
	RegisterType  RegisterType   `gorm:"not null;default:0"       json:"register_type"`
	Status        ExecutorStatus `gorm:"not null;default:1"       json:"status"`
	Weight        int            `gorm:"not null;default:1"       json:"weight"`
	HeartbeatTime *time.Time     `gorm:"column:heartbeat_time"    json:"heartbeat_time"`
	Version       string         `gorm:"size:32"                  json:"version"`
	Tags          string         `gorm:"size:255"                 json:"tags"`
	CreateTime    time.Time      `gorm:"autoCreateTime"           json:"create_time"`
	UpdateTime    time.Time      `gorm:"autoUpdateTime"           json:"update_time"`
}

func (Executor) TableName() string { return "sys_executor" }

// IsOnline returns true if the executor is considered alive.
func (e *Executor) IsOnline() bool { return e.Status == ExecutorOnline }

// ─── JobInfo ──────────────────────────────────────────────────────────────────

// JobType categorises a scheduled job.
type JobType int8

const (
	JobTypeCron      JobType = 1 // standard cron expression
	JobTypeDelay     JobType = 2 // delayed (fire once after N seconds)
	JobTypeOnce      JobType = 3 // one-shot, then disabled
	JobTypeSharding  JobType = 4 // broadcast across all executors with shard index
)

// ExecuteType defines how the executor runs the job.
type ExecuteType string

const (
	ExecuteTypeBean   ExecuteType = "BEAN"   // Go registered handler function
	ExecuteTypeShell  ExecuteType = "SHELL"  // shell script
	ExecuteTypeCmd    ExecuteType = "CMD"    // command line
	ExecuteTypePython ExecuteType = "PYTHON" // python script
)

// RouteStrategy defines how the scheduler picks an executor node.
type RouteStrategy string

const (
	RouteFirst             RouteStrategy = "FIRST"
	RouteLast              RouteStrategy = "LAST"
	RouteRoundRobin        RouteStrategy = "ROUND_ROBIN"
	RouteRandom            RouteStrategy = "RANDOM"
	RouteConsistentHash    RouteStrategy = "CONSISTENT_HASH"
	RouteLFU               RouteStrategy = "LFU"
	RouteLRU               RouteStrategy = "LRU"
	RouteFailover          RouteStrategy = "FAILOVER"
	RouteBusyTransfer      RouteStrategy = "BUSY_TRANSFER"
	RouteShardingBroadcast RouteStrategy = "SHARDING_BROADCAST"
)

// BlockStrategy controls behaviour when a job is triggered while still running.
type BlockStrategy int8

const (
	BlockSerial   BlockStrategy = 1 // queue and run serially
	BlockDiscard  BlockStrategy = 2 // discard the new trigger
	BlockOverride BlockStrategy = 3 // terminate the running job and start fresh
)

// MisfireStrategy handles triggers that fired while the scheduler was down.
type MisfireStrategy int8

const (
	MisfireIgnore      MisfireStrategy = 1
	MisfireRunOnce     MisfireStrategy = 2
)

// JobStatus represents the enabled/disabled state of a job.
type JobStatus int8

const (
	JobStop JobStatus = 0
	JobRun  JobStatus = 1
)

// JobInfo maps to job_info.
type JobInfo struct {
	ID              int64           `gorm:"primaryKey;autoIncrement"     json:"id"`
	ExecutorID      int64           `gorm:"not null"                     json:"executor_id"`
	ExecutorApp     string          `gorm:"size:64;not null"             json:"executor_app"`
	JobName         string          `gorm:"size:128;not null"            json:"job_name"`
	JobDesc         string          `gorm:"size:255"                     json:"job_desc"`
	JobType         JobType         `gorm:"not null;default:1"           json:"job_type"`
	CronExpression  string          `gorm:"size:64"                      json:"cron_expression"`
	ExecuteType     ExecuteType     `gorm:"size:16;not null;default:'BEAN'" json:"execute_type"`
	ExecuteParam    string          `gorm:"size:512"                     json:"execute_param"`
	ExecuteHandler  string          `gorm:"size:255;not null"            json:"execute_handler"`
	RouteStrategy   RouteStrategy   `gorm:"size:32;not null;default:'ROUND_ROBIN'" json:"route_strategy"`
	BlockStrategy   BlockStrategy   `gorm:"not null;default:1"           json:"block_strategy"`
	MisfireStrategy MisfireStrategy `gorm:"not null;default:1"           json:"misfire_strategy"`
	Timeout         int             `gorm:"not null;default:0"           json:"timeout"`
	RetryCount      int             `gorm:"not null;default:0"           json:"retry_count"`
	RetryInterval   int             `gorm:"not null;default:0"           json:"retry_interval"`
	ShardingNum     int             `gorm:"not null;default:1"           json:"sharding_num"`
	AlarmEmail      string          `gorm:"size:255"                     json:"alarm_email"`
	AlarmWebhook    string          `gorm:"size:255"                     json:"alarm_webhook"`
	Status          JobStatus       `gorm:"not null;default:1"           json:"status"`
	NextTriggerTime *time.Time      `gorm:"column:next_trigger_time"     json:"next_trigger_time"`
	LastTriggerTime *time.Time      `gorm:"column:last_trigger_time"     json:"last_trigger_time"`
	CreateUser      string          `gorm:"size:32;default:'admin'"      json:"create_user"`
	CreateTime      time.Time       `gorm:"autoCreateTime"               json:"create_time"`
	UpdateTime      time.Time       `gorm:"autoUpdateTime"               json:"update_time"`
}

func (JobInfo) TableName() string { return "job_info" }

// IsRunning returns true if the job is enabled.
func (j *JobInfo) IsRunning() bool { return j.Status == JobRun }

// ─── JobLog ───────────────────────────────────────────────────────────────────

// LogStatus represents the execution result of a single trigger.
type LogStatus int8

const (
	LogInit     LogStatus = 0
	LogSuccess  LogStatus = 1
	LogFail     LogStatus = 2
	LogRunning  LogStatus = 3
	LogTimeout  LogStatus = 4
	LogKilled   LogStatus = 5
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

// ─── JobLogDetail ─────────────────────────────────────────────────────────────

// JobLogDetail maps to job_log_detail.
type JobLogDetail struct {
	ID         int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	LogID      int64     `gorm:"not null;uniqueIndex"     json:"log_id"`
	JobID      int64     `gorm:"not null"                 json:"job_id"`
	LogContent string    `gorm:"type:longtext"            json:"log_content"`
	CreateTime time.Time `gorm:"autoCreateTime"           json:"create_time"`
}

func (JobLogDetail) TableName() string { return "job_log_detail" }

// ─── JobLock ──────────────────────────────────────────────────────────────────

// JobLock maps to job_lock (DB-level distributed lock fallback).
type JobLock struct {
	ID         int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	LockKey    string    `gorm:"size:128;not null;uniqueIndex" json:"lock_key"`
	LockUntil  time.Time `gorm:"not null"                 json:"lock_until"`
	LockNode   string    `gorm:"size:128;not null"        json:"lock_node"`
	CreateTime time.Time `gorm:"autoCreateTime"           json:"create_time"`
}

func (JobLock) TableName() string { return "job_lock" }

// ─── SysUser ──────────────────────────────────────────────────────────────────

// UserRole defines admin vs regular user access level.
type UserRole int8

const (
	RoleAdmin   UserRole = 1
	RoleNormal  UserRole = 2
)

// UserStatus reflects whether the account is active.
type UserStatus int8

const (
	UserDisabled UserStatus = 0
	UserEnabled  UserStatus = 1
)

// SysUser maps to sys_user.
type SysUser struct {
	ID         int64      `gorm:"primaryKey;autoIncrement" json:"id"`
	Username   string     `gorm:"size:32;not null;uniqueIndex" json:"username"`
	Password   string     `gorm:"size:128;not null"        json:"-"` // never serialise
	Nickname   string     `gorm:"size:64"                  json:"nickname"`
	Email      string     `gorm:"size:128"                 json:"email"`
	Role       UserRole   `gorm:"not null;default:2"       json:"role"`
	Status     UserStatus `gorm:"not null;default:1"       json:"status"`
	LastLogin  *time.Time `gorm:"column:last_login"        json:"last_login"`
	CreateTime time.Time  `gorm:"autoCreateTime"           json:"create_time"`
	UpdateTime time.Time  `gorm:"autoUpdateTime"           json:"update_time"`
}

func (SysUser) TableName() string { return "sys_user" }

// IsAdmin returns true if the user has admin privileges.
func (u *SysUser) IsAdmin() bool { return u.Role == RoleAdmin }

// IsActive returns true if the user account is enabled.
func (u *SysUser) IsActive() bool { return u.Status == UserEnabled }
