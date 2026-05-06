package model

import "time"

// JobType categorises a scheduled job.
type JobType int8

const (
	JobTypeCron     JobType = 1 // standard cron expression
	JobTypeDelay    JobType = 2 // delayed (fire once after N seconds)
	JobTypeOnce     JobType = 3 // one-shot, then disabled
	JobTypeSharding JobType = 4 // broadcast across all executors with shard index
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
	MisfireIgnore  MisfireStrategy = 1
	MisfireRunOnce MisfireStrategy = 2
)

// JobStatus represents the enabled/disabled state of a job.
type JobStatus int8

const (
	JobStop JobStatus = 0
	JobRun  JobStatus = 1
)

// JobInfo maps to job_info.
type JobInfo struct {
	ID              int64           `gorm:"primaryKey;autoIncrement"              json:"id"`
	ExecutorID      int64           `gorm:"not null"                              json:"executor_id"`
	ExecutorApp     string          `gorm:"size:64;not null"                      json:"executor_app"`
	JobName         string          `gorm:"size:128;not null"                     json:"job_name"`
	JobDesc         string          `gorm:"size:255"                              json:"job_desc"`
	JobType         JobType         `gorm:"not null;default:1"                    json:"job_type"`
	CronExpression  string          `gorm:"size:64"                               json:"cron_expression"`
	ExecuteType     ExecuteType     `gorm:"size:16;not null;default:'BEAN'"       json:"execute_type"`
	ExecuteParam    string          `gorm:"size:512"                              json:"execute_param"`
	ExecuteHandler  string          `gorm:"size:255;not null"                     json:"execute_handler"`
	RouteStrategy   RouteStrategy   `gorm:"size:32;not null;default:'ROUND_ROBIN'" json:"route_strategy"`
	BlockStrategy   BlockStrategy   `gorm:"not null;default:1"                    json:"block_strategy"`
	MisfireStrategy MisfireStrategy `gorm:"not null;default:1"                    json:"misfire_strategy"`
	Timeout         int             `gorm:"not null;default:0"                    json:"timeout"`
	RetryCount      int             `gorm:"not null;default:0"                    json:"retry_count"`
	RetryInterval   int             `gorm:"not null;default:0"                    json:"retry_interval"`
	ShardingNum     int             `gorm:"not null;default:1"                    json:"sharding_num"`
	AlarmEmail      string          `gorm:"size:255"                              json:"alarm_email"`
	AlarmWebhook    string          `gorm:"size:255"                              json:"alarm_webhook"`
	Status          JobStatus       `gorm:"not null;default:1"                    json:"status"`
	NextTriggerTime *time.Time      `gorm:"column:next_trigger_time"              json:"next_trigger_time"`
	LastTriggerTime *time.Time      `gorm:"column:last_trigger_time"              json:"last_trigger_time"`
	ChildJobIDs     string          `gorm:"size:255;default:''"`                   json:"child_job_ids"`
	GroupID         int64           `gorm:"default:0"`                            json:"group_id"`
	TriggerCount    int64           `gorm:"default:0"`                            json:"trigger_count"`
	CreateUser      string          `gorm:"size:32;default:'admin'"`                json:"create_user"`
	CreateTime      time.Time       `gorm:"autoCreateTime"                        json:"create_time"`
	UpdateTime      time.Time       `gorm:"autoUpdateTime"                        json:"update_time"`
}

func (JobInfo) TableName() string { return "job_info" }

// IsRunning returns true if the job is enabled.
func (j *JobInfo) IsRunning() bool { return j.Status == JobRun }

// JobGroup 对应 job_group 任务分组表（v3 新增）。
type JobGroup struct {
	ID          int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	Name        string    `gorm:"size:64;not null;uniqueIndex" json:"name"`
	Description string    `gorm:"size:255;default:''"          json:"description"`
	CreateTime  time.Time `gorm:"autoCreateTime"               json:"create_time"`
}

func (JobGroup) TableName() string { return "job_group" }
